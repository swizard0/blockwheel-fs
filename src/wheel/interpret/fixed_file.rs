use std::{
    io,
    path::{
        Path,
        PathBuf,
    },
};

use futures::{
    channel::mpsc,
    Future,
    StreamExt,
};

use tokio::{
    fs,
    io::{
        AsyncReadExt,
        AsyncWriteExt,
    },
};

use crate::wheel::{
    core::{
        task,
        schema,
    },
    block,
    storage,
};

use super::Request;

#[derive(Debug)]
pub enum Error {
    WheelFileInitialSeek(io::Error),
    WheelFileSeek {
        offset: u64,
        cursor: u64,
        error: io::Error,
    },
    BlockHeaderSerialize(bincode::Error),
    CommitTagSerialize(bincode::Error),
    TombstoneTagSerialize(bincode::Error),
    BlockWrite(io::Error),
    BlockFlush(io::Error),
    BlockRead(io::Error),
    BlockHeaderDeserialize(bincode::Error),
    CommitTagDeserialize(bincode::Error),
    CorruptedData(CorruptedDataError),
}

#[derive(Debug)]
pub enum CorruptedDataError {
    BlockIdMismatch {
        offset: u64,
        block_id_expected: block::Id,
        block_id_actual: block::Id,
    },
    BlockSizeMismatch {
        offset: u64,
        block_id: block::Id,
        block_size_expected: usize,
        block_size_actual: usize,
    },
    CommitTagBlockIdMismatch {
        offset: u64,
        block_id_expected: block::Id,
        block_id_actual: block::Id,
    },
    CommitTagCrcMismatch {
        offset: u64,
        crc_expected: u64,
        crc_actual: u64,
    },
}

#[derive(Debug)]
pub enum WheelCreateError {
    FileCreate {
        wheel_filename: PathBuf,
        error: io::Error,
    },
    StorageLayoutCalculate(storage::LayoutError),
    InitWheelSizeIsTooSmall {
        provided: usize,
        required_min: usize,
    },
    HeaderSerialize(bincode::Error),
    HeaderTagWrite(io::Error),
    ZeroChunkWrite(io::Error),
    Flush(io::Error),
}

#[derive(Debug)]
pub enum WheelOpenError {
    FileWrongType,
    FileNotFound,
    FileMetadata {
        wheel_filename: PathBuf,
        error: io::Error,
    },
    FileOpen {
        wheel_filename: PathBuf,
        error: io::Error,
    },
    StorageLayoutCalculate(storage::LayoutError),
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

#[derive(Clone, Debug)]
pub struct CreateParams<P> {
    pub wheel_filename: P,
    pub work_block_size_bytes: usize,
    pub init_wheel_size_bytes: usize,
}

#[derive(Clone, Debug)]
pub struct OpenParams<P> {
    pub wheel_filename: P,
    pub work_block_size_bytes: usize,
}

pub struct GenServer {
    wheel_file: fs::File,
    work_block: Vec<u8>,
    schema: schema::Schema,
    request_tx: mpsc::Sender<Request>,
    request_rx: mpsc::Receiver<Request>,
}

impl GenServer {
    pub async fn create<P>(params: CreateParams<P>) -> Result<Self, WheelCreateError> where P: AsRef<Path> {
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

        let mut work_block: Vec<u8> = Vec::with_capacity(params.work_block_size_bytes);

        let storage_layout = storage::Layout::calculate(&mut work_block)
            .map_err(WheelCreateError::StorageLayoutCalculate)?;

        let wheel_header = storage::WheelHeader {
            size_bytes: params.init_wheel_size_bytes as u64,
            ..storage::WheelHeader::default()
        };
        bincode::serialize_into(&mut work_block, &wheel_header)
            .map_err(WheelCreateError::HeaderSerialize)?;

        let mut cursor = work_block.len();
        let min_wheel_file_size = storage_layout.wheel_header_size;
        assert_eq!(cursor, min_wheel_file_size);
        wheel_file.write_all(&work_block).await
            .map_err(WheelCreateError::HeaderTagWrite)?;

        let size_bytes_total = params.init_wheel_size_bytes;
        if size_bytes_total < min_wheel_file_size {
            return Err(WheelCreateError::InitWheelSizeIsTooSmall {
                provided: size_bytes_total,
                required_min: min_wheel_file_size,
            });
        }

        work_block.clear();
        work_block.extend((0 .. params.work_block_size_bytes).map(|_| 0));

        while cursor < size_bytes_total {
            let bytes_remain = size_bytes_total - cursor;
            let write_amount = if bytes_remain < params.work_block_size_bytes {
                bytes_remain
            } else {
                params.work_block_size_bytes
            };
            wheel_file.write_all(&work_block[.. write_amount]).await
                .map_err(WheelCreateError::ZeroChunkWrite)?;
            cursor += write_amount;
        }
        wheel_file.flush().await
            .map_err(WheelCreateError::Flush)?;

        let mut schema = schema::Schema::new(storage_layout);
        schema.initialize_empty(size_bytes_total);

        log::debug!("initialized wheel schema: {:?}", schema);

        let (request_tx, request_rx) = mpsc::channel(0);

        Ok(GenServer {
            wheel_file,
            work_block,
            schema,
            request_tx,
            request_rx,
        })
    }

    pub async fn open<P>(params: OpenParams<P>) -> Result<Self, WheelOpenError> where P: AsRef<Path> {
        log::debug!("opening existing wheel file [ {:?} ]", params.wheel_filename.as_ref());

        let file_size = match fs::metadata(&params.wheel_filename).await {
            Ok(ref metadata) if metadata.file_type().is_file() =>
                metadata.len(),
            Ok(..) =>
                return Err(WheelOpenError::FileWrongType),
            Err(ref error) if error.kind() == io::ErrorKind::NotFound =>
                return Err(WheelOpenError::FileNotFound),
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

        let mut work_block: Vec<u8> = Vec::with_capacity(params.work_block_size_bytes);

        let storage_layout = storage::Layout::calculate(&mut work_block)
            .map_err(WheelOpenError::StorageLayoutCalculate)?;

        // read wheel header
        work_block.extend((0 .. storage_layout.wheel_header_size).map(|_| 0));
        wheel_file.read_exact(&mut work_block).await
            .map_err(WheelOpenError::HeaderRead)?;
        let wheel_header: storage::WheelHeader = bincode::deserialize_from(&work_block[..])
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
        let mut builder = schema::Builder::new(storage_layout);

        let mut cursor = work_block.len() as u64;
        assert_eq!(cursor, builder.storage_layout().wheel_header_size as u64);

        work_block.clear();
        work_block.extend((0 .. params.work_block_size_bytes).map(|_| 0));
        let mut offset = 0;

        loop {
            let bytes_read = match wheel_file.read(&mut work_block[offset ..]).await {
                Ok(0) =>
                    break,
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
                enum Outcome { Success, Failure, }

                let area = &work_block[start .. start + builder.storage_layout().block_header_size];
                let outcome = match bincode::deserialize_from::<_, storage::BlockHeader>(area) {
                    Ok(block_header) if block_header.magic == storage::BLOCK_MAGIC => {
                        let try_read_block_async = try_read_block(
                            &mut wheel_file,
                            &mut work_block,
                            cursor,
                            &block_header,
                            builder.storage_layout(),
                            &params,
                        );
                        match try_read_block_async.await? {
                            ReadBlockStatus::NotABlock =>
                                Outcome::Failure,
                            ReadBlockStatus::BlockFound { next_cursor, } => {
                                builder.push_block(cursor, block_header);
                                cursor = next_cursor;
                                Outcome::Success
                            },
                        }
                    },
                    Ok(..) | Err(..) =>
                        Outcome::Failure,
                };
                match outcome {
                    Outcome::Success => {
                        work_block.clear();
                        offset = 0;
                        start = 0;
                        break;
                    },
                    Outcome::Failure => {
                        start += 1;
                        cursor += 1;
                    },
                }
            }
            if start > 0 {
                work_block.copy_within(start .. offset, 0);
                offset -= start;
            }
        }
        assert_eq!(cursor, file_size);
        let schema = builder.finish(&wheel_header);

        log::debug!("loaded wheel schema");

        let (request_tx, request_rx) = mpsc::channel(0);

        Ok(GenServer {
            wheel_file,
            work_block,
            schema,
            request_tx,
            request_rx,
        })
    }

    pub fn run(self) -> (Pid, impl Future<Output = Result<(), Error>>) {
        let storage_layout = self.schema.storage_layout().clone();
        (
            Pid {
                request_tx: self.request_tx,
                schema: self.schema,
            },
            busyloop(
                self.request_rx,
                self.wheel_file,
                self.work_block,
                storage_layout,
            ),
        )
    }
}

pub struct Pid {
    pub request_tx: mpsc::Sender<Request>,
    pub schema: schema::Schema,
}

enum ReadBlockStatus {
    NotABlock,
    BlockFound { next_cursor: u64, },
}

async fn try_read_block<P>(
    wheel_file: &mut fs::File,
    work_block: &mut Vec<u8>,
    cursor: u64,
    block_header: &storage::BlockHeader,
    storage_layout: &storage::Layout,
    params: &OpenParams<P>,
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
        // not a block: rewind
        wheel_file.seek(io::SeekFrom::Start(cursor)).await
            .map_err(WheelOpenError::BlockRewindCommitTag)?;
        return Ok(ReadBlockStatus::NotABlock);
    }
    if commit_tag.block_id != block_header.block_id {
        // some other block terminator: rewind
        wheel_file.seek(io::SeekFrom::Start(cursor)).await
            .map_err(WheelOpenError::BlockRewindCommitTag)?;
        return Ok(ReadBlockStatus::NotABlock);
    }
    if block_header.block_size > params.work_block_size_bytes {
        return Err(WheelOpenError::BlockSizeTooLarge {
            work_block_size_bytes: params.work_block_size_bytes,
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

async fn busyloop(
    mut request_rx: mpsc::Receiver<Request>,
    mut wheel_file: fs::File,
    mut work_block: Vec<u8>,
    storage_layout: storage::Layout,
)
    -> Result<(), Error>
{
    let mut cursor = 0;
    wheel_file.seek(io::SeekFrom::Start(cursor)).await
        .map_err(Error::WheelFileInitialSeek)?;

    while let Some(Request { offset, task, reply_tx, }) = request_rx.next().await {
        if cursor != offset {
            wheel_file.seek(io::SeekFrom::Start(offset)).await
                .map_err(|error| Error::WheelFileSeek { offset, cursor, error, })?;
            cursor = offset;
        }

        match task.kind {
            task::TaskKind::WriteBlock(write_block) => {
                let block_header = storage::BlockHeader {
                    block_id: task.block_id.clone(),
                    block_size: write_block.block_bytes.len(),
                    ..Default::default()
                };
                bincode::serialize_into(&mut work_block, &block_header)
                    .map_err(Error::BlockHeaderSerialize)?;
                work_block.extend(write_block.block_bytes.iter());
                let commit_tag = storage::CommitTag {
                    block_id: task.block_id.clone(),
                    crc: block::crc(&write_block.block_bytes),
                    ..Default::default()
                };
                bincode::serialize_into(&mut work_block, &commit_tag)
                    .map_err(Error::CommitTagSerialize)?;

                wheel_file.write_all(&work_block).await
                    .map_err(Error::BlockWrite)?;
                wheel_file.flush().await
                    .map_err(Error::BlockFlush)?;

                cursor += work_block.len() as u64;
                work_block.clear();

                let task_done = task::Done {
                    current_offset: cursor,
                    task: task::TaskDone {
                        block_id: task.block_id,
                        kind: task::TaskDoneKind::WriteBlock(task::TaskDoneWriteBlock {
                            context: write_block.context,
                        }),
                    },
                };
                if let Err(_send_error) = reply_tx.send(task_done) {
                    break;
                }
            },

            task::TaskKind::ReadBlock(mut read_block) => {
                let total_chunk_size = storage_layout.data_size_block_min()
                    + read_block.block_header.block_size;
                work_block.extend((0 .. total_chunk_size).map(|_| 0));
                wheel_file.read_exact(&mut work_block).await
                    .map_err(Error::BlockRead)?;

                let block_buffer_start = storage_layout.block_header_size;
                let block_buffer_end = work_block.len() - storage_layout.commit_tag_size;

                let block_header: storage::BlockHeader = bincode::deserialize_from(&work_block[.. block_buffer_start])
                    .map_err(Error::BlockHeaderDeserialize)?;
                if block_header.block_id != read_block.block_header.block_id {
                    return Err(Error::CorruptedData(CorruptedDataError::BlockIdMismatch {
                        offset,
                        block_id_expected: read_block.block_header.block_id,
                        block_id_actual: block_header.block_id,
                    }));
                }
                if block_header.block_size != read_block.block_header.block_size {
                    return Err(Error::CorruptedData(CorruptedDataError::BlockSizeMismatch {
                        offset,
                        block_id: block_header.block_id,
                        block_size_expected: read_block.block_header.block_size,
                        block_size_actual: block_header.block_size,
                    }));
                }

                read_block.block_bytes.extend(work_block[block_buffer_start .. block_buffer_end].iter());

                let commit_tag: storage::CommitTag = bincode::deserialize_from(&work_block[block_buffer_end ..])
                    .map_err(Error::CommitTagDeserialize)?;
                if commit_tag.block_id != read_block.block_header.block_id {
                    return Err(Error::CorruptedData(CorruptedDataError::CommitTagBlockIdMismatch {
                        offset,
                        block_id_expected: read_block.block_header.block_id,
                        block_id_actual: commit_tag.block_id,
                    }));
                }
                if commit_tag.crc != block::crc(&read_block.block_bytes) {
                    return Err(Error::CorruptedData(CorruptedDataError::CommitTagCrcMismatch {
                        offset,
                        crc_expected: block::crc(&read_block.block_bytes),
                        crc_actual: commit_tag.crc,
                    }));
                }

                cursor += work_block.len() as u64;
                work_block.clear();

                let task_done = task::Done {
                    current_offset: cursor,
                    task: task::TaskDone {
                        block_id: read_block.block_header.block_id,
                        kind: task::TaskDoneKind::ReadBlock(task::TaskDoneReadBlock {
                            block_bytes: read_block.block_bytes,
                            context: read_block.context,
                        }),
                    },
                };
                if let Err(_send_error) = reply_tx.send(task_done) {
                    break;
                }
            },

            task::TaskKind::DeleteBlock(delete_block) => {
                let tombstone_tag = storage::TombstoneTag::default();
                bincode::serialize_into(&mut work_block, &tombstone_tag)
                    .map_err(Error::TombstoneTagSerialize)?;

                wheel_file.write_all(&work_block).await
                    .map_err(Error::BlockWrite)?;
                wheel_file.flush().await
                    .map_err(Error::BlockFlush)?;

                cursor += work_block.len() as u64;
                work_block.clear();

                let task_done = task::Done {
                    current_offset: cursor,
                    task: task::TaskDone {
                        block_id: task.block_id,
                        kind: task::TaskDoneKind::DeleteBlock(task::TaskDoneDeleteBlock {
                            context: delete_block.context,
                        }),
                    },
                };
                if let Err(_send_error) = reply_tx.send(task_done) {
                    break;
                }
            },
        }
    }

    log::debug!("master channel closed in interpret_loop, shutting down");
    Ok(())
}
