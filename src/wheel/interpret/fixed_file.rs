use std::{
    io,
    path::{
        Path,
        PathBuf,
    },
    time::{
        Instant,
        Duration,
    },
};

use futures::{
    channel::{
        mpsc,
    },
    StreamExt,
};

use tokio::{
    fs,
    io::{
        AsyncSeekExt,
        AsyncReadExt,
        AsyncWriteExt,
    },
};

use alloc_pool::bytes::{
    BytesPool,
};

use crate::{
    context::Context,
    wheel::{
        block,
        storage,
        core::{
            task,
            performer,
        },
        interpret::{
            Pid,
            Synced,
            Command,
            Request,
            DoneTask,
            AppendTerminatorError,
            block_append_terminator,
        },
    },
    InterpretStats,
};

// #[cfg(test)]
// mod tests;

#[derive(Debug)]
pub enum Error {
    WheelFileInitialSeek(io::Error),
    WheelFileSeek {
        offset: u64,
        cursor: u64,
        error: io::Error,
    },
    AppendTerminator(AppendTerminatorError),
    BlockWrite(io::Error),
    TerminatorWrite(io::Error),
    BlockRead(io::Error),
    DeviceSyncFlush(io::Error),
}

#[derive(Debug)]
pub enum WheelCreateError {
    FileCreate {
        wheel_filename: PathBuf,
        error: io::Error,
    },
    InitWheelSizeIsTooSmall {
        provided: usize,
        required_min: usize,
    },
    HeaderSerialize(bincode::Error),
    TerminatorTagSerialize(bincode::Error),
    HeaderTagWrite(io::Error),
    ZeroChunkWrite(io::Error),
    Flush(io::Error),
}

#[derive(Debug)]
pub enum WheelOpenError {
    FileWrongType,
    FileMetadata {
        wheel_filename: PathBuf,
        error: io::Error,
    },
    FileOpen {
        wheel_filename: PathBuf,
        error: io::Error,
    },
    HeaderRead(io::Error),
    HeaderDeserialize(bincode::Error),
    HeaderInvalidMagic {
        provided: u64,
        expected: u64,
    },
    HeaderVersionMismatch {
        provided: usize,
        expected: usize,
    },
    WheelSizeMismatch {
        header: u64,
        actual: u64,
    },
    LocateBlock(io::Error),
    BlockSizeTooLarge {
        work_block_size_bytes: usize,
        block_size: usize,
    },
    BlockSeekCommitTag(io::Error),
    BlockRewindCommitTag(io::Error),
    BlockReadCommitTag(io::Error),
    CommitTagDeserialize(bincode::Error),
    BlockSeekContents(io::Error),
    BlockReadContents(io::Error),
    BlockCrcMismatch {
        commit_tag_crc: u64,
        block_crc: u64,
    },
    BlockSeekEnd(io::Error),
}

pub struct WheelData<C> where C: Context {
    pub gen_server: GenServer<C>,
    pub performer: performer::Performer<C>,
}

pub enum WheelOpenStatus<C> where C: Context {
    Success(WheelData<C>),
    FileNotFound {
        performer_builder: performer::PerformerBuilderInit<C>,
    },
}

#[derive(Clone, Debug)]
pub struct CreateParams<P> {
    pub wheel_filename: P,
    pub init_wheel_size_bytes: usize,
}

#[derive(Clone, Debug)]
pub struct OpenParams<P> {
    pub wheel_filename: P,
}

pub struct GenServer<C> where C: Context {
    wheel_file: fs::File,
    request_tx: mpsc::Sender<Command<C>>,
    request_rx: mpsc::Receiver<Command<C>>,
    storage_layout: storage::Layout,
}

impl<C> GenServer<C> where C: Context {
    pub async fn create<P>(
        params: CreateParams<P>,
        mut performer_builder: performer::PerformerBuilderInit<C>,
    )
        -> Result<WheelData<C>, WheelCreateError> where P: AsRef<Path>
    {
        log::debug!("creating new wheel file [ {:?} ]", params.wheel_filename.as_ref());

        let mut wheel_file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(params.wheel_filename.as_ref())
            .await
            .map_err(|error| WheelCreateError::FileCreate {
                wheel_filename: params.wheel_filename.as_ref().to_owned(),
                error,
            })?;

        let wheel_header = storage::WheelHeader {
            size_bytes: params.init_wheel_size_bytes as u64,
            ..storage::WheelHeader::default()
        };
        bincode::serialize_into(performer_builder.work_block_cleared(), &wheel_header)
            .map_err(WheelCreateError::HeaderSerialize)?;

        let terminator_tag = storage::TerminatorTag::default();
        bincode::serialize_into(performer_builder.work_block(), &terminator_tag)
            .map_err(WheelCreateError::TerminatorTagSerialize)?;

        let mut cursor = performer_builder.work_block().len();
        let min_wheel_file_size = performer_builder.storage_layout().wheel_header_size
            + performer_builder.storage_layout().terminator_tag_size;
        assert_eq!(cursor, min_wheel_file_size);
        wheel_file.write_all(performer_builder.work_block()).await
            .map_err(WheelCreateError::HeaderTagWrite)?;

        let work_block_size_bytes = performer_builder.work_block().capacity();
        let size_bytes_total = params.init_wheel_size_bytes;
        if size_bytes_total < min_wheel_file_size {
            return Err(WheelCreateError::InitWheelSizeIsTooSmall {
                provided: size_bytes_total,
                required_min: min_wheel_file_size,
            });
        }

        performer_builder
            .work_block_cleared()
            .extend((0 .. work_block_size_bytes).map(|_| 0));

        while cursor < size_bytes_total {
            let bytes_remain = size_bytes_total - cursor;
            let write_amount = if bytes_remain < work_block_size_bytes {
                bytes_remain
            } else {
                work_block_size_bytes
            };
            wheel_file.write_all(&performer_builder.work_block()[.. write_amount]).await
                .map_err(WheelCreateError::ZeroChunkWrite)?;
            cursor += write_amount;
        }
        wheel_file.flush().await
            .map_err(WheelCreateError::Flush)?;

        log::debug!("interpret::fixed_file create success");
        let storage_layout = performer_builder.storage_layout().clone();

        let (request_tx, request_rx) = mpsc::channel(0);

        let (performer_builder, _work_block) = performer_builder.start_fill();

        Ok(WheelData {
            gen_server: GenServer {
                wheel_file,
                request_tx,
                request_rx,
                storage_layout,
            },
            performer: performer_builder
                .finish(params.init_wheel_size_bytes),
        })
    }

    pub async fn open<P>(
        params: OpenParams<P>,
        mut performer_builder: performer::PerformerBuilderInit<C>,
    )
        -> Result<WheelOpenStatus<C>, WheelOpenError> where P: AsRef<Path>
    {
        log::debug!("opening existing wheel file [ {:?} ]", params.wheel_filename.as_ref());

        let file_size = match fs::metadata(&params.wheel_filename).await {
            Ok(ref metadata) if metadata.file_type().is_file() =>
                metadata.len(),
            Ok(..) =>
                return Err(WheelOpenError::FileWrongType),
            Err(ref error) if error.kind() == io::ErrorKind::NotFound =>
                return Ok(WheelOpenStatus::FileNotFound { performer_builder, }),
            Err(error) =>
                return Err(WheelOpenError::FileMetadata {
                    wheel_filename: params.wheel_filename.as_ref().to_owned(),
                    error,
                }),
        };

        let mut wheel_file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(false)
            .open(params.wheel_filename.as_ref())
            .await
            .map_err(|error| WheelOpenError::FileOpen {
                wheel_filename: params.wheel_filename.as_ref().to_owned(),
                error,
            })?;

        let wheel_header_size = performer_builder
            .storage_layout()
            .wheel_header_size;

        // read wheel header
        performer_builder
            .work_block_cleared()
            .extend((0 .. wheel_header_size).map(|_| 0));
        wheel_file.read_exact(performer_builder.work_block()).await
            .map_err(WheelOpenError::HeaderRead)?;
        let wheel_header: storage::WheelHeader = bincode::deserialize_from(&performer_builder.work_block()[..])
            .map_err(WheelOpenError::HeaderDeserialize)?;
        if wheel_header.magic != storage::WHEEL_MAGIC {
            return Err(WheelOpenError::HeaderInvalidMagic {
                provided: wheel_header.magic,
                expected: storage::WHEEL_MAGIC,
            });
        }
        if wheel_header.version != storage::WHEEL_VERSION {
            return Err(WheelOpenError::HeaderVersionMismatch {
                provided: wheel_header.version,
                expected: storage::WHEEL_VERSION,
            });
        }
        if wheel_header.size_bytes != file_size {
            return Err(WheelOpenError::WheelSizeMismatch {
                header: wheel_header.size_bytes,
                actual: file_size,
            });
        }

        // read blocks and gaps
        let (mut builder, mut work_block) = performer_builder.start_fill();

        work_block.clear();
        let mut cursor = wheel_header_size as u64;

        let work_block_size_bytes = work_block.capacity();
        work_block.resize(work_block_size_bytes, 0);
        let mut offset = 0;
        'outer: loop {
            let bytes_read = match wheel_file.read(&mut work_block[offset ..]).await {
                Ok(0) => {
                    assert!(
                        cursor + builder.storage_layout().block_header_size as u64 >= file_size,
                        "assertion failed: cursor = {} + block_header_size = {} >= file_size = {}",
                        cursor,
                        builder.storage_layout().block_header_size,
                        file_size,
                    );
                    break;
                },
                Ok(bytes_read) =>
                    bytes_read,
                Err(ref error) if error.kind() == io::ErrorKind::Interrupted =>
                    continue,
                Err(error) =>
                    return Err(WheelOpenError::LocateBlock(error)),
            };
            offset += bytes_read;
            let mut start = 0;
            while offset - start >= builder.storage_layout().block_header_size {
                let area = &work_block[start .. start + builder.storage_layout().block_header_size];
                match bincode::deserialize_from::<_, storage::BlockHeader>(area) {
                    Ok(block_header) if block_header.magic == storage::BLOCK_MAGIC => {
                        let try_read_block_status = try_read_block(
                            &mut wheel_file,
                            &mut work_block,
                            cursor,
                            &block_header,
                            builder.storage_layout(),
                        ).await?;
                        work_block.resize(work_block_size_bytes, 0);
                        offset = 0;
                        start = 0;

                        match try_read_block_status {
                            ReadBlockStatus::NotABlock { next_cursor, } =>
                                cursor = next_cursor,
                            ReadBlockStatus::BlockFound { next_cursor, } => {
                                builder.push_block(cursor, block_header);
                                cursor = next_cursor;
                            },
                        }
                        break;
                    },
                    Ok(..) | Err(..) =>
                        match bincode::deserialize_from::<_, storage::TerminatorTag>(area) {
                            Ok(terminator_tag) if terminator_tag.magic == storage::TERMINATOR_TAG_MAGIC => {
                                log::debug!("terminator found @ {:?}, loading done", cursor);
                                break 'outer;
                            },
                            Ok(..) | Err(..) =>
                                (),
                        },
                };
                start += 1;
                cursor += 1;
            }
            if start > 0 {
                work_block.copy_within(start .. offset, 0);
                offset -= start;
            }
        }

        log::debug!("loaded wheel schema");

        let (request_tx, request_rx) = mpsc::channel(0);

        Ok(WheelOpenStatus::Success(WheelData {
            gen_server: GenServer {
                wheel_file,
                request_tx,
                request_rx,
                storage_layout: builder
                    .storage_layout()
                    .clone(),
            },
            performer: builder
                .finish(wheel_header.size_bytes as usize),
        }))
    }

    pub fn pid(&self) -> Pid<C> {
        Pid {
            request_tx: self.request_tx.clone(),
        }
    }

    pub async fn run(self, blocks_pool: BytesPool) -> Result<(), Error> {
        busyloop(
            self.request_rx,
            self.wheel_file,
            self.storage_layout,
            blocks_pool,
        ).await
    }
}

enum ReadBlockStatus {
    NotABlock { next_cursor: u64, },
    BlockFound { next_cursor: u64, },
}

async fn try_read_block(
    wheel_file: &mut fs::File,
    work_block: &mut Vec<u8>,
    cursor: u64,
    block_header: &storage::BlockHeader,
    storage_layout: &storage::Layout,
)
    -> Result<ReadBlockStatus, WheelOpenError>
{
    // seek to commit tag position
    wheel_file.seek(io::SeekFrom::Start(cursor + storage_layout.block_header_size as u64 + block_header.block_size as u64)).await
        .map_err(WheelOpenError::BlockSeekCommitTag)?;
    // read commit tag
    work_block.resize(storage_layout.commit_tag_size, 0);
    wheel_file.read_exact(work_block).await
        .map_err(WheelOpenError::BlockReadCommitTag)?;
    let commit_tag: storage::CommitTag = bincode::deserialize_from(&work_block[..])
        .map_err(WheelOpenError::CommitTagDeserialize)?;
    if commit_tag.magic != storage::COMMIT_TAG_MAGIC {
        // not a block: rewind and step
        let next_cursor = cursor + 1;
        wheel_file.seek(io::SeekFrom::Start(next_cursor)).await
            .map_err(WheelOpenError::BlockRewindCommitTag)?;
        return Ok(ReadBlockStatus::NotABlock { next_cursor, });
    }
    if commit_tag.block_id != block_header.block_id {
        // some other block terminator: rewind
        let next_cursor = cursor + 1;
        wheel_file.seek(io::SeekFrom::Start(next_cursor)).await
            .map_err(WheelOpenError::BlockRewindCommitTag)?;
        return Ok(ReadBlockStatus::NotABlock { next_cursor, });
    }
    if block_header.block_size > work_block.capacity() {
        return Err(WheelOpenError::BlockSizeTooLarge {
            work_block_size_bytes: work_block.capacity(),
            block_size: block_header.block_size,
        });
    }
    // seek to block contents
    wheel_file.seek(io::SeekFrom::Start(cursor + storage_layout.block_header_size as u64)).await
        .map_err(WheelOpenError::BlockSeekContents)?;
    // read block contents
    work_block.resize(block_header.block_size, 0);
    wheel_file.read_exact(work_block).await
        .map_err(WheelOpenError::BlockReadContents)?;
    let crc = block::crc(work_block);
    if crc != commit_tag.crc {
        return Err(WheelOpenError::BlockCrcMismatch {
            commit_tag_crc: commit_tag.crc,
            block_crc: crc,
        });
    }
    // seek to the end of commit tag
    let next_cursor = wheel_file.seek(io::SeekFrom::Current(storage_layout.commit_tag_size as i64)).await
        .map_err(WheelOpenError::BlockSeekEnd)?;
    Ok(ReadBlockStatus::BlockFound { next_cursor, })
}

#[derive(Debug, Default)]
struct Timings {
    event_wait: Duration,
    seek: Duration,
    write_write: Duration,
    read: Duration,
    write_delete: Duration,
    flush: Duration,
    total: Duration,
}

async fn busyloop<C>(
    request_rx: mpsc::Receiver<Command<C>>,
    mut wheel_file: fs::File,
    storage_layout: storage::Layout,
    blocks_pool: BytesPool,
)
    -> Result<(), Error>
where C: Context,
{
    let mut stats = InterpretStats::default();

    let mut fused_request_rx = request_rx.fuse();

    let mut terminator_block_bytes = blocks_pool.lend();
    block_append_terminator(&mut terminator_block_bytes)
        .map_err(Error::AppendTerminator)?;

    let mut cursor = storage_layout.wheel_header_size as u64;
    wheel_file.seek(io::SeekFrom::Start(cursor)).await
        .map_err(Error::WheelFileInitialSeek)?;

    let mut timings = Timings::default();
    loop {
        let now_loop = Instant::now();

        enum Event<C> { Command(C), }
        let event = Event::Command(fused_request_rx.next().await);
        timings.event_wait += now_loop.elapsed();

        match event {

            Event::Command(None) =>
                break,

            Event::Command(Some(Command::Request(Request { offset, task, reply_tx, }))) => {
                stats.count_total += 1;

                if cursor != offset {
                    if cursor < offset {
                        stats.count_seek_forward += 1;
                    } else if cursor > offset {
                        stats.count_seek_backward += 1;
                    }
                    let now = Instant::now();
                    wheel_file.seek(io::SeekFrom::Start(offset)).await
                        .map_err(|error| Error::WheelFileSeek { offset, cursor, error, })?;
                    timings.seek += now.elapsed();
                    cursor = offset;
                } else {
                    stats.count_no_seek += 1;
                }

                match task.kind {
                    task::TaskKind::WriteBlock(write_block) => {
                        let now = Instant::now();
                        wheel_file.write_all(&write_block.write_block_bytes).await
                            .map_err(Error::BlockWrite)?;
                        cursor += write_block.write_block_bytes.len() as u64;

                        match write_block.commit {
                            task::Commit::None =>
                                (),
                            task::Commit::WithTerminator => {
                                wheel_file.write_all(&terminator_block_bytes).await
                                    .map_err(Error::TerminatorWrite)?;
                                // note: do not count terminator in order to overwrite it during next write
                                // cursor += terminator_block_bytes.len() as u64;
                            }
                        }

                        timings.write_write += now.elapsed();


                        let task_done = task::Done {
                            current_offset: cursor,
                            task: task::TaskDone {
                                block_id: task.block_id,
                                kind: task::TaskDoneKind::WriteBlock(task::TaskDoneWriteBlock {
                                    context: write_block.context,
                                }),
                            },
                        };
                        if let Err(_send_error) = reply_tx.send(DoneTask { task_done, stats, }) {
                            break;
                        }
                    },

                    task::TaskKind::ReadBlock(task::ReadBlock { block_header, context, }) => {
                        let total_chunk_size = storage_layout.data_size_block_min()
                            + block_header.block_size;
                        let mut block_bytes = blocks_pool.lend();
                        block_bytes.resize(total_chunk_size, 0);
                        let now = Instant::now();
                        wheel_file.read_exact(&mut block_bytes).await
                            .map_err(Error::BlockRead)?;
                        cursor += block_bytes.len() as u64;
                        timings.read += now.elapsed();

                        let task_done = task::Done {
                            current_offset: cursor,
                            task: task::TaskDone {
                                block_id: block_header.block_id,
                                kind: task::TaskDoneKind::ReadBlock(task::TaskDoneReadBlock {
                                    block_bytes,
                                    context,
                                }),
                            },
                        };
                        if let Err(_send_error) = reply_tx.send(DoneTask { task_done, stats, }) {
                            break;
                        }
                    },

                    task::TaskKind::DeleteBlock(delete_block) => {
                        let now = Instant::now();
                        wheel_file.write_all(&delete_block.delete_block_bytes).await
                            .map_err(Error::BlockWrite)?;
                        cursor += delete_block.delete_block_bytes.len() as u64;

                        match delete_block.commit {
                            task::Commit::None =>
                                (),
                            task::Commit::WithTerminator => {
                                wheel_file.write_all(&terminator_block_bytes).await
                                    .map_err(Error::TerminatorWrite)?;
                                cursor += terminator_block_bytes.len() as u64;
                            }
                        }

                        timings.write_delete += now.elapsed();

                        let task_done = task::Done {
                            current_offset: cursor,
                            task: task::TaskDone {
                                block_id: task.block_id,
                                kind: task::TaskDoneKind::DeleteBlock(task::TaskDoneDeleteBlock {
                                    context: delete_block.context,
                                }),
                            },
                        };
                        if let Err(_send_error) = reply_tx.send(DoneTask { task_done, stats, }) {
                            break;
                        }
                    },
                }
            },

            Event::Command(Some(Command::DeviceSync { reply_tx, })) => {
                let now = Instant::now();
                wheel_file.flush().await
                    .map_err(Error::DeviceSyncFlush)?;
                timings.flush += now.elapsed();
                if let Err(_send_error) = reply_tx.send(Synced) {
                    break;
                }
                log::info!("current timings: {:?}", timings);
            },

        }
        timings.total += now_loop.elapsed();
    }

    log::debug!("master channel closed in interpret_loop, shutting down");
    Ok(())
}
