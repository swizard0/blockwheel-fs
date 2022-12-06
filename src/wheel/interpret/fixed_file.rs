use std::{
    fs,
    io::{
        self,
        Seek,
        Read,
        Write,
    },
    path::{
        PathBuf,
    },
    time::{
        Instant,
        Duration,
    },
};

use alloc_pool::{
    bytes::{
        BytesPool,
    },
};

use alloc_pool_pack::{
    SourceSlice,
    ReadFromSource,
    WriteToBytesMut,
};

use arbeitssklave::{
    ewig,
};

use crate::{
    block,
    storage,
    blockwheel_context::{
        Context,
    },
    wheel::{
        performer_sklave,
        core::{
            task,
            performer,
        },
        interpret::{
            Order,
            Request,
            Error as InterpretError,
            block_append_terminator,
        },
    },
    EchoPolicy,
    InterpretStats,
    FixedFileInterpreterParams,
};

#[cfg(test)]
mod tests;

#[derive(Debug)]
pub enum Error {
    WheelCreate(WheelCreateError),
    WheelOpen(WheelOpenError),
    WheelFileInitialSeek(io::Error),
    WheelFileSeek {
        offset: u64,
        cursor: u64,
        error: io::Error,
    },
    BlockWrite(io::Error),
    TerminatorWrite(io::Error),
    BlockRead(io::Error),
    DeviceSyncFlush(io::Error),
    ThreadSpawn(io::Error),
    Arbeitssklave(arbeitssklave::Error),
    UnexpectedOrderBeforeCommitStart,
    UnexpectedCommitStartOrder,
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
    HeaderDeserialize(storage::ReadWheelHeaderError),
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
    CommitTagDeserialize(storage::ReadCommitTagError),
    BlockSeekContents(io::Error),
    BlockReadContents(io::Error),
    BlockCrcMismatch {
        commit_tag_crc: u64,
        block_crc: u64,
    },
    BlockSeekEnd(io::Error),
}

pub(super) fn bootstrap<E, W, J>(
    sklave: &mut ewig::Sklave<Order<E>, InterpretError>,
    params: FixedFileInterpreterParams,
    performer_builder: performer::PerformerBuilderInit<Context<E>>,
    performer_sklave_meister: arbeitssklave::Meister<W, performer_sklave::Order<E>>,
    blocks_pool: BytesPool,
    thread_pool: edeltraud::Handle<J>,
)
    -> Result<(), InterpretError>
where E: EchoPolicy,
      J: From<arbeitssklave::SklaveJob<W, performer_sklave::Order<E>>>,
{
    let WheelData { wheel_file, storage_layout, performer, } =
        match open(&params, performer_builder, &blocks_pool) {
            Ok(WheelOpenStatus::Success(wheel_data)) =>
                wheel_data,
            Ok(WheelOpenStatus::FileNotFound { performer_builder, }) => {
                create(&params, performer_builder)
                    .map_err(Error::WheelCreate)?
            },
            Err(wheel_open_error) =>
                return Err(Error::WheelOpen(wheel_open_error).into()),
        };

    performer_sklave_meister
        .befehl(
            performer_sklave::Order::Bootstrap(
                performer_sklave::OrderBootstrap {
                    performer,
                },
            ),
            &thread_pool,
        )
        .map_err(Error::Arbeitssklave)?;

    run(sklave, wheel_file, storage_layout, performer_sklave_meister, blocks_pool, thread_pool)
}

enum WheelOpenStatus<E> where E: EchoPolicy {
    Success(WheelData<E>),
    FileNotFound {
        performer_builder: performer::PerformerBuilderInit<Context<E>>,
    },
}

fn open<E>(
    params: &FixedFileInterpreterParams,
    performer_builder: performer::PerformerBuilderInit<Context<E>>,
    blocks_pool: &BytesPool,
)
    -> Result<WheelOpenStatus<E>, WheelOpenError>
where E: EchoPolicy,
{
    log::debug!("opening existing wheel file [ {:?} ]", params.wheel_filename);

    let file_size = match fs::metadata(&params.wheel_filename) {
        Ok(ref metadata) if metadata.file_type().is_file() =>
            metadata.len(),
        Ok(..) =>
            return Err(WheelOpenError::FileWrongType),
        Err(ref error) if error.kind() == io::ErrorKind::NotFound =>
            return Ok(WheelOpenStatus::FileNotFound { performer_builder, }),
        Err(error) =>
            return Err(WheelOpenError::FileMetadata {
                wheel_filename: params.wheel_filename.clone(),
                error,
            }),
    };

    let mut wheel_file = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(false)
        .open(&params.wheel_filename)
        .map_err(|error| WheelOpenError::FileOpen {
            wheel_filename: params.wheel_filename.clone(),
            error,
        })?;

    let wheel_header_size = performer_builder
        .storage_layout()
        .wheel_header_size;

    // read wheel header
    let mut wheel_header_bytes_mut = blocks_pool.lend();
    wheel_header_bytes_mut
        .extend((0 .. wheel_header_size).map(|_| 0));
    wheel_file.read_exact(&mut wheel_header_bytes_mut)
        .map_err(WheelOpenError::HeaderRead)?;
    let mut source = SourceSlice::from(&***wheel_header_bytes_mut);
    let wheel_header = storage::WheelHeader::read_from_source(&mut source)
        .map_err(WheelOpenError::HeaderDeserialize)?;
    if wheel_header.size_bytes != file_size {
        return Err(WheelOpenError::WheelSizeMismatch {
            header: wheel_header.size_bytes,
            actual: file_size,
        });
    }

    log::debug!("wheel_header read: {:?}", wheel_header);

    // read blocks and gaps
    let (mut builder, mut work_block) = performer_builder.start_fill();
    work_block.clear();

    let mut cursor = wheel_header_size as u64;
    let work_block_size_bytes = work_block.capacity();
    work_block.resize(work_block_size_bytes, 0);

    let mut offset = 0;
    'outer: loop {
        let bytes_read = match wheel_file.read(&mut work_block[offset ..]) {
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
            let mut source = SourceSlice::from(area);
            match storage::BlockHeader::read_from_source(&mut source) {
                Ok(block_header) => {
                    let try_read_block_status = try_read_block(
                        &mut wheel_file,
                        &mut work_block,
                        cursor,
                        &block_header,
                        builder.storage_layout(),
                    )?;
                    work_block.resize(work_block_size_bytes, 0);
                    offset = 0;
                    start = 0;

                    match try_read_block_status {
                        ReadBlockStatus::NotABlock { next_cursor, } =>
                            cursor = next_cursor,
                        ReadBlockStatus::BlockFound { next_cursor, } => {
                            log::debug!("restored block @ {}: {:?}, next_cursor = {}", cursor, block_header, next_cursor);
                            builder.push_block(cursor, block_header);
                            cursor = next_cursor;
                        },
                    }
                    break;
                },
                Err(..) =>
                    if storage::TerminatorTag::read_from_source(&mut SourceSlice::from(area)).is_ok() {
                        log::debug!("terminator found @ {:?}, loading done", cursor);
                        break 'outer;
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

    Ok(WheelOpenStatus::Success(WheelData {
        wheel_file,
        storage_layout: builder
            .storage_layout()
            .clone(),
        performer: builder
            .finish(wheel_header.size_bytes as usize),
    }))
}

fn create<E>(
    params: &FixedFileInterpreterParams,
    mut performer_builder: performer::PerformerBuilderInit<Context<E>>,
)
    -> Result<WheelData<E>, WheelCreateError>
where E: EchoPolicy,
{
    log::debug!("creating new wheel file [ {:?} ]", params.wheel_filename);

    let mut wheel_file = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(&params.wheel_filename)
        .map_err(|error| WheelCreateError::FileCreate {
            wheel_filename: params.wheel_filename.clone(),
            error,
        })?;

    let wheel_header = storage::WheelHeader {
        size_bytes: params.init_wheel_size_bytes as u64,
        ..storage::WheelHeader::default()
    };
    wheel_header.write_to_bytes_mut(performer_builder.work_block_cleared());
    let terminator_tag = storage::TerminatorTag::default();
    terminator_tag.write_to_bytes_mut(performer_builder.work_block());

    let mut cursor = performer_builder.work_block().len();
    let min_wheel_file_size = performer_builder.storage_layout().wheel_header_size
        + performer_builder.storage_layout().terminator_tag_size;
    assert_eq!(cursor, min_wheel_file_size);
    wheel_file.write_all(performer_builder.work_block())
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
        wheel_file.write_all(&performer_builder.work_block()[.. write_amount])
            .map_err(WheelCreateError::ZeroChunkWrite)?;
        cursor += write_amount;
    }
    wheel_file.flush()
        .map_err(WheelCreateError::Flush)?;

    log::debug!("interpret::fixed_file create success");
    let storage_layout = performer_builder.storage_layout().clone();

    let (performer_builder, _work_block) = performer_builder.start_fill();

    Ok(WheelData {
        wheel_file,
        storage_layout,
        performer: performer_builder
            .finish(params.init_wheel_size_bytes),
    })
}

struct WheelData<E> where E: EchoPolicy {
    wheel_file: fs::File,
    storage_layout: storage::Layout,
    performer: performer::Performer<Context<E>>,
}

enum ReadBlockStatus {
    NotABlock { next_cursor: u64, },
    BlockFound { next_cursor: u64, },
}

fn try_read_block(
    wheel_file: &mut fs::File,
    work_block: &mut Vec<u8>,
    cursor: u64,
    block_header: &storage::BlockHeader,
    storage_layout: &storage::Layout,
)
    -> Result<ReadBlockStatus, WheelOpenError>
{
    // seek to commit tag position
    let commit_offset = wheel_file
        .seek(io::SeekFrom::Start(cursor + storage_layout.block_header_size as u64 + block_header.block_size as u64))
        .map_err(WheelOpenError::BlockSeekCommitTag)?;
    // read commit tag
    work_block.resize(storage_layout.commit_tag_size, 0);
    wheel_file.read_exact(work_block)
        .map_err(WheelOpenError::BlockReadCommitTag)?;
    let mut source = SourceSlice::from(&work_block[..]);
    let commit_tag = match storage::CommitTag::read_from_source(&mut source) {
        Ok(commit_tag) if commit_tag.block_id != block_header.block_id => {
            // some other block terminator: rewind
            let next_cursor = cursor + 1;
            wheel_file.seek(io::SeekFrom::Start(next_cursor))
                .map_err(WheelOpenError::BlockRewindCommitTag)?;

            log::debug!("NotABlock because of commit block_id {:?} != header.block_id {:?}", commit_tag.block_id, block_header.block_id);

            return Ok(ReadBlockStatus::NotABlock { next_cursor, });
        },
        Ok(commit_tag) =>
            commit_tag,
        Err(storage::ReadCommitTagError::InvalidMagic { expected, provided, }) => {
            // not a block: rewind and step
            let next_cursor = cursor + 1;
            wheel_file.seek(io::SeekFrom::Start(next_cursor))
                .map_err(WheelOpenError::BlockRewindCommitTag)?;

            log::debug!("NotABlock because of commit magic {:?} != {:?}", provided, expected);

            return Ok(ReadBlockStatus::NotABlock { next_cursor, });
        },
        Err(error) =>
            return Err(WheelOpenError::CommitTagDeserialize(error)),
    };

    if block_header.block_size > work_block.capacity() as u64 {
        return Err(WheelOpenError::BlockSizeTooLarge {
            work_block_size_bytes: work_block.capacity(),
            block_size: block_header.block_size as usize,
        });
    }
    // seek to block contents
    wheel_file.seek(io::SeekFrom::Start(cursor + storage_layout.block_header_size as u64))
        .map_err(WheelOpenError::BlockSeekContents)?;
    // read block contents
    work_block.resize(block_header.block_size as usize, 0);
    wheel_file.read_exact(work_block)
        .map_err(WheelOpenError::BlockReadContents)?;
    let crc = block::crc(work_block);
    if crc != commit_tag.crc {
        return Err(WheelOpenError::BlockCrcMismatch {
            commit_tag_crc: commit_tag.crc,
            block_crc: crc,
        });
    }
    // seek to the end of commit tag
    let next_cursor = wheel_file.seek(io::SeekFrom::Current(storage_layout.commit_tag_size as i64))
        .map_err(WheelOpenError::BlockSeekEnd)?;

    assert_eq!(next_cursor, commit_offset + storage_layout.commit_tag_size as u64);

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

fn run<E, W, J>(
    sklave: &mut ewig::Sklave<Order<E>, InterpretError>,
    mut wheel_file: fs::File,
    storage_layout: storage::Layout,
    performer_sklave_meister: arbeitssklave::Meister<W, performer_sklave::Order<E>>,
    blocks_pool: BytesPool,
    thread_pool: edeltraud::Handle<J>,
)
    -> Result<(), InterpretError>
where E: EchoPolicy,
      J: From<arbeitssklave::SklaveJob<W, performer_sklave::Order<E>>>,
{
    log::debug!("running background interpreter job");

    let mut stats = InterpretStats::default();

    let mut terminator_block_bytes = blocks_pool.lend();
    block_append_terminator(&mut terminator_block_bytes);

    let mut cursor = storage_layout.wheel_header_size as u64;
    wheel_file.seek(io::SeekFrom::Start(cursor))
        .map_err(Error::WheelFileInitialSeek)?;
    let mut pending_terminator = false;
    let mut timings = Timings::default();
    'outer: loop {
        let now_loop = Instant::now();
        let orders = sklave.zu_ihren_diensten()?;
        timings.event_wait += now_loop.elapsed();

        for order in orders {
            match order {

                Order::Request(Request { offset, task, }) => {
                    stats.count_total += 1;

                    if cursor != offset {
                        #[allow(clippy::comparison_chain)]
                        if cursor < offset {
                            stats.count_seek_forward += 1;
                        } else if cursor > offset {
                            stats.count_seek_backward += 1;
                            if pending_terminator {
                                log::debug!("writing pending_terminator during seek @ {}", cursor);
                                wheel_file.write_all(&terminator_block_bytes)
                                    .map_err(Error::TerminatorWrite)?;
                                pending_terminator = false;
                            }
                        }
                        let now = Instant::now();
                        wheel_file.seek(io::SeekFrom::Start(offset))
                            .map_err(|error| Error::WheelFileSeek { offset, cursor, error, })?;
                        timings.seek += now.elapsed();
                        cursor = offset;
                    } else {
                        stats.count_no_seek += 1;
                    }

                    match task.kind {
                        task::TaskKind::WriteBlock(write_block) => {

                            log::debug!(
                                "write block {:?} @ {} of {} bytes, context: {:?}",
                                task.block_id,
                                cursor,
                                write_block.write_block_bytes.len(),
                                write_block.context,
                            );

                            let now = Instant::now();
                            match &write_block.write_block_bytes {
                                task::WriteBlockBytes::Chunk(write_block_bytes) => {
                                    wheel_file.write_all(write_block_bytes)
                                        .map_err(Error::BlockWrite)?;
                                },
                                task::WriteBlockBytes::Composite(task::WriteBlockBytesComposite {
                                    block_header,
                                    block_bytes,
                                    commit_tag,
                                }) => {
                                    wheel_file.write_all(block_header)
                                        .map_err(Error::BlockWrite)?;
                                    wheel_file.write_all(block_bytes)
                                        .map_err(Error::BlockWrite)?;
                                    wheel_file.write_all(commit_tag)
                                        .map_err(Error::BlockWrite)?;
                                },
                            }
                            cursor += write_block.write_block_bytes.len() as u64;
                            timings.write_write += now.elapsed();

                            pending_terminator = match write_block.commit {
                                task::Commit::None =>
                                    false,
                                task::Commit::WithTerminator =>
                                    true,
                            };

                            let order = performer_sklave::Order::TaskDoneStats(
                                performer_sklave::OrderTaskDoneStats {
                                    task_done: task::Done {
                                        current_offset: cursor,
                                        task: task::TaskDone {
                                            block_id: task.block_id,
                                            kind: task::TaskDoneKind::WriteBlock(task::TaskDoneWriteBlock {
                                                context: write_block.context,
                                            }),
                                        },
                                    },
                                    stats,
                                },
                            );
                            match performer_sklave_meister.befehl(order, &thread_pool) {
                                Ok(()) =>
                                    (),
                                Err(arbeitssklave::Error::Terminated) =>
                                    break 'outer,
                                Err(error) =>
                                    return Err(Error::Arbeitssklave(error).into()),
                            }
                        },

                        task::TaskKind::ReadBlock(task::ReadBlock { block_header, context, }) => {
                            let total_chunk_size = storage_layout.data_size_block_min()
                                + block_header.block_size as usize;

                            log::debug!(
                                "read block {:?} @ {} of {} bytes, context = {:?}",
                                task.block_id,
                                cursor,
                                total_chunk_size,
                                context,
                            );

                            let mut block_bytes = blocks_pool.lend();
                            block_bytes.reserve(total_chunk_size);
                            let now = Instant::now();
                            let wheel_file_ref = Read::by_ref(&mut wheel_file);
                            wheel_file_ref
                                .take(total_chunk_size as u64)
                                .read_to_end(&mut block_bytes)
                                .map_err(Error::BlockRead)?;
                            cursor += block_bytes.len() as u64;
                            timings.read += now.elapsed();

                            let order = performer_sklave::Order::TaskDoneStats(
                                performer_sklave::OrderTaskDoneStats {
                                    task_done: task::Done {
                                        current_offset: cursor,
                                        task: task::TaskDone {
                                            block_id: block_header.block_id,
                                            kind: task::TaskDoneKind::ReadBlock(task::TaskDoneReadBlock {
                                                block_bytes,
                                                context,
                                            }),
                                        },
                                    },
                                    stats,
                                },
                            );
                            match performer_sklave_meister.befehl(order, &thread_pool) {
                                Ok(()) =>
                                    (),
                                Err(arbeitssklave::Error::Terminated) =>
                                    break 'outer,
                                Err(error) =>
                                    return Err(Error::Arbeitssklave(error).into()),
                            }
                        },

                        task::TaskKind::DeleteBlock(delete_block) => {

                            log::debug!("delete block {:?} @ {}, context: {:?}", task.block_id, cursor, delete_block.context);

                            let now = Instant::now();
                            wheel_file.write_all(&delete_block.delete_block_bytes)
                                .map_err(Error::BlockWrite)?;
                            cursor += delete_block.delete_block_bytes.len() as u64;
                            timings.write_delete += now.elapsed();

                            pending_terminator = match delete_block.commit {
                                task::Commit::None =>
                                    false,
                                task::Commit::WithTerminator =>
                                    true,
                            };

                            let order = performer_sklave::Order::TaskDoneStats(
                                performer_sklave::OrderTaskDoneStats {
                                    task_done: task::Done {
                                        current_offset: cursor,
                                        task: task::TaskDone {
                                            block_id: task.block_id,
                                            kind: task::TaskDoneKind::DeleteBlock(task::TaskDoneDeleteBlock {
                                                context: delete_block.context,
                                            }),
                                        },
                                    },
                                    stats,
                                },
                            );
                            match performer_sklave_meister.befehl(order, &thread_pool) {
                                Ok(()) =>
                                    (),
                                Err(arbeitssklave::Error::Terminated) =>
                                    break 'outer,
                                Err(error) =>
                                    return Err(Error::Arbeitssklave(error).into()),
                            }
                        },
                    }
                },

                Order::DeviceSync { flush_context, } => {
                    let now = Instant::now();
                    if pending_terminator {
                        log::debug!("writing pending_terminator during flush @ {}", cursor);
                        wheel_file.write_all(&terminator_block_bytes)
                            .map_err(Error::TerminatorWrite)?;
                        pending_terminator = false;
                        cursor += terminator_block_bytes.len() as u64;
                    } else {
                        log::debug!("flushed with no pending_terminator (cursor @ {})", cursor);
                    }
                    wheel_file.flush()
                        .map_err(Error::DeviceSyncFlush)?;
                    timings.flush += now.elapsed();

                    let order = performer_sklave::Order::DeviceSyncDone(
                        performer_sklave::OrderDeviceSyncDone {
                            flush_context,
                        },
                    );
                    match performer_sklave_meister.befehl(order, &thread_pool) {
                        Ok(()) =>
                            (),
                        Err(arbeitssklave::Error::Terminated) =>
                            break 'outer,
                        Err(error) =>
                            return Err(Error::Arbeitssklave(error).into()),
                    }

                    log::info!("current timings: {:?}", timings);
                },

            }
        }
        timings.total += now_loop.elapsed();
    }

    log::debug!("performer meister dropped in interpret_loop, shutting down");
    Ok(())
}
