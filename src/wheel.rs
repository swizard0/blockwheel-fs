use std::{
    io,
    path::PathBuf,
};

use futures::{
    future,
    select,
    stream,
    channel::{
        mpsc,
        oneshot,
    },
    SinkExt,
    StreamExt,
    FutureExt,
};

use tokio::{
    fs,
    io::{
        AsyncReadExt,
        AsyncWriteExt,
    },
};

use ero::{
    ErrorSeverity,
    supervisor::SupervisorPid,
};

use super::{
    block,
    proto,
    storage,
    Params,
};

mod gaps;
mod task;
mod pool;
mod index;
mod defrag;
mod schema;

#[derive(Debug)]
pub enum Error {
    InitWheelSizeIsTooSmall {
        provided: usize,
        required_min: usize,
    },
    WheelFileMetadata {
        wheel_filename: PathBuf,
        error: io::Error,
    },
    WheelFileOpen {
        wheel_filename: PathBuf,
        error: io::Error,
    },
    StorageLayoutCalculate(storage::LayoutError),
    WheelHeaderSerialize(bincode::Error),
    EofTagSerialize(bincode::Error),
    WheelHeaderAndEofTagWrite(io::Error),
    ZeroChunkWrite(io::Error),
    WheelCreateFlush(io::Error),
    WheelFileInitialSeek(io::Error),
    WheelFileSeek {
        offset: u64,
        cursor: u64,
        error: io::Error,
    },
    BlockHeaderSerialize(bincode::Error),
    CommitTagSerialize(bincode::Error),
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
}

pub struct State {
    pub parent_supervisor: SupervisorPid,
    pub fused_request_rx: stream::Fuse<mpsc::Receiver<proto::Request>>,
    pub params: Params,
}

pub async fn busyloop_init(mut supervisor_pid: SupervisorPid, state: State) -> Result<(), ErrorSeverity<State, Error>> {
    let WheelState { wheel_file, work_block, schema, state, } = match fs::metadata(&state.params.wheel_filename).await {
        Ok(ref metadata) if metadata.file_type().is_file() =>
            wheel_open(state).await?,
        Ok(_metadata) => {
            log::error!("[ {:?} ] is not a file", state.params.wheel_filename);
            return Err(ErrorSeverity::Recoverable { state, });
        },
        Err(ref error) if error.kind() == io::ErrorKind::NotFound =>
            wheel_create(state).await?,
        Err(error) =>
            return Err(ErrorSeverity::Fatal(Error::WheelFileMetadata {
                wheel_filename: state.params.wheel_filename,
                error,
            })),
    };

    let (interpret_tx, interpret_rx) = mpsc::channel(0);
    let (interpret_error_tx, interpret_error_rx) = oneshot::channel();
    let storage_layout = schema.storage_layout().clone();
    supervisor_pid.spawn_link_permanent(
        async move {
            let interpret_task = interpret_loop(
                interpret_rx,
                wheel_file,
                work_block,
                storage_layout,
            );
            if let Err(interpret_error) = interpret_task.await {
                interpret_error_tx.send(interpret_error).ok();
            }
        },
    );

    busyloop(supervisor_pid, interpret_tx, interpret_error_rx.fuse(), state, schema).await
}

async fn busyloop(
    _supervisor_pid: SupervisorPid,
    mut interpret_tx: mpsc::Sender<InterpretRequest>,
    mut fused_interpret_error_rx: future::Fuse<oneshot::Receiver<ErrorSeverity<(), Error>>>,
    mut state: State,
    mut schema: schema::Schema,
)
    -> Result<(), ErrorSeverity<State, Error>>
{
    let mut blocks_pool = pool::Blocks::new();
    let mut tasks_queue = task::Queue::new();
    let mut bg_task: Option<future::Fuse<oneshot::Receiver<task::TaskDone>>> = None;

    loop {
        enum Source<A, B, C> {
            Pid(A),
            InterpreterDone(B),
            InterpreterError(C),
        }

        let req = if let Some(mut fused_interpret_result_rx) = bg_task.as_mut() {
            select! {
                result = state.fused_request_rx.next() =>
                    Source::Pid(result),
                result = fused_interpret_result_rx =>
                    Source::InterpreterDone(result),
                result = fused_interpret_error_rx =>
                    Source::InterpreterError(result),
            }
        } else if let Some((offset, task_kind)) = tasks_queue.pop() {
            let (reply_tx, reply_rx) = oneshot::channel();
            if let Err(_send_error) = interpret_tx.send(InterpretRequest { offset, task_kind, reply_tx, }).await {
                log::warn!("interpreter request channel closed");
            }

            bg_task = Some(reply_rx.fuse());
            continue;
        } else {
            select! {
                result = state.fused_request_rx.next() =>
                    Source::Pid(result),
                result = fused_interpret_error_rx =>
                    Source::InterpreterError(result),
            }
        };

        match req {
            Source::Pid(None) => {
                log::debug!("all Pid frontends have been terminated");
                return Ok(());
            },

            Source::Pid(Some(proto::Request::LendBlock(proto::RequestLendBlock { reply_tx, }))) => {
                let block = blocks_pool.lend();
                if let Err(_send_error) = reply_tx.send(block) {
                    log::warn!("Pid is gone during query result send");
                }
            },

            Source::Pid(Some(proto::Request::RepayBlock(proto::RequestRepayBlock { block_bytes, }))) => {
                blocks_pool.repay(block_bytes);
            },

            Source::Pid(Some(proto::Request::WriteBlock(request_write_block))) =>
                schema.process_write_block_request(request_write_block, &mut tasks_queue),

            Source::Pid(Some(proto::Request::ReadBlock(request_read_block))) => {
                let block = blocks_pool.lend();
                schema.process_read_block_request(request_read_block, block, &mut tasks_queue);
            },

            Source::Pid(Some(proto::Request::DeleteBlock(proto::RequestDeleteBlock { block_id, reply_tx, }))) => {

                unimplemented!()
            },

            Source::InterpreterDone(Ok(task::TaskDone::WriteBlock(write_block))) => {
                bg_task = None;
                if let Err(_send_error) = write_block.reply_tx.send(Ok(write_block.block_id)) {
                    log::warn!("client channel was closed before a block is actually written");
                }
            },

            Source::InterpreterDone(Ok(task::TaskDone::ReadBlock(read_block))) => {
                // TODO: update LRU

                bg_task = None;
                if let Err(_send_error) = read_block.reply_tx.send(Ok(read_block.block_bytes.freeze())) {
                    log::warn!("client channel was closed before a block is actually read");
                }
            },

            Source::InterpreterDone(Err(oneshot::Canceled)) => {
                log::debug!("interpreter reply channel closed: shutting down");
                return Ok(());
            },

            Source::InterpreterError(Ok(ErrorSeverity::Recoverable { state: (), })) =>
                return Err(ErrorSeverity::Recoverable { state, }),

            Source::InterpreterError(Ok(ErrorSeverity::Fatal(error))) =>
                return Err(ErrorSeverity::Fatal(error)),

            Source::InterpreterError(Err(oneshot::Canceled)) => {
                log::debug!("interpreter error channel closed: shutting down");
                return Ok(());
            },
        }
    }
}

struct InterpretRequest {
    offset: u64,
    task_kind: task::TaskKind,
    reply_tx: oneshot::Sender<task::TaskDone>,
}

async fn interpret_loop(
    mut interpret_rx: mpsc::Receiver<InterpretRequest>,
    mut wheel_file: fs::File,
    mut work_block: Vec<u8>,
    storage_layout: storage::Layout,
)
    -> Result<(), ErrorSeverity<(), Error>>
{
    let mut cursor = 0;
    wheel_file.seek(io::SeekFrom::Start(cursor)).await
        .map_err(Error::WheelFileInitialSeek)
        .map_err(ErrorSeverity::Fatal)?;

    while let Some(InterpretRequest { offset, task_kind, reply_tx, }) = interpret_rx.next().await {
        if cursor != offset {
            wheel_file.seek(io::SeekFrom::Start(offset)).await
                .map_err(|error| ErrorSeverity::Fatal(Error::WheelFileSeek { offset, cursor, error, }))?;
            cursor = offset;
        }

        match task_kind {
            task::TaskKind::WriteBlock(write_block) => {
                let block_header = storage::BlockHeader {
                    block_id: write_block.block_id.clone(),
                    block_size: write_block.block_bytes.len(),
                };
                bincode::serialize_into(&mut work_block, &block_header)
                    .map_err(Error::BlockHeaderSerialize)
                    .map_err(ErrorSeverity::Fatal)?;
                work_block.extend(write_block.block_bytes.iter());
                let commit_tag = storage::CommitTag {
                    block_id: write_block.block_id.clone(),
                    ..Default::default()
                };
                bincode::serialize_into(&mut work_block, &block_header)
                    .map_err(Error::CommitTagSerialize)
                    .map_err(ErrorSeverity::Fatal)?;

                match write_block.commit_type {
                    task::CommitType::CommitOnly =>
                        (),
                    task::CommitType::CommitAndEof => {
                        bincode::serialize_into(&mut work_block, &storage::EofTag::default())
                            .map_err(Error::EofTagSerialize)
                            .map_err(ErrorSeverity::Fatal)?;
                    },
                }

                wheel_file.write_all(&work_block).await
                    .map_err(Error::BlockWrite)
                    .map_err(ErrorSeverity::Fatal)?;
                wheel_file.flush().await
                    .map_err(Error::BlockFlush)
                    .map_err(ErrorSeverity::Fatal)?;

                let task_done = task::TaskDone::WriteBlock(task::TaskDoneWriteBlock {
                    block_id: write_block.block_id,
                    reply_tx: write_block.reply_tx,
                });
                if let Err(_send_error) = reply_tx.send(task_done) {
                    break;
                }

                cursor += work_block.len() as u64;
                work_block.clear();
            },

            task::TaskKind::ReadBlock(mut read_block) => {
                let total_chunk_size = storage_layout.data_size_block_min()
                    + read_block.block_header.block_size;
                work_block.extend((0 .. total_chunk_size).map(|_| 0));
                wheel_file.read_exact(&mut work_block).await
                    .map_err(Error::BlockRead)
                    .map_err(ErrorSeverity::Fatal)?;

                let block_buffer_start = storage_layout.block_header_size;
                let block_buffer_end = work_block.len() - storage_layout.commit_tag_size;

                let block_header: storage::BlockHeader = bincode::deserialize_from(&work_block[.. block_buffer_start])
                    .map_err(Error::BlockHeaderDeserialize)
                    .map_err(ErrorSeverity::Fatal)?;
                if block_header.block_id != read_block.block_header.block_id {
                    return Err(ErrorSeverity::Fatal(Error::CorruptedData(CorruptedDataError::BlockIdMismatch {
                        offset,
                        block_id_expected: read_block.block_header.block_id,
                        block_id_actual: block_header.block_id,
                    })));
                }
                if block_header.block_size != read_block.block_header.block_size {
                    return Err(ErrorSeverity::Fatal(Error::CorruptedData(CorruptedDataError::BlockSizeMismatch {
                        offset,
                        block_id: block_header.block_id,
                        block_size_expected: read_block.block_header.block_size,
                        block_size_actual: block_header.block_size,
                    })));
                }

                read_block.block_bytes.extend(work_block[block_buffer_start .. block_buffer_end].iter());

                let commit_tag: storage::CommitTag = bincode::deserialize_from(&work_block[block_buffer_end ..])
                    .map_err(Error::CommitTagDeserialize)
                    .map_err(ErrorSeverity::Fatal)?;
                if commit_tag.block_id != read_block.block_header.block_id {
                    return Err(ErrorSeverity::Fatal(Error::CorruptedData(CorruptedDataError::CommitTagBlockIdMismatch {
                        offset,
                        block_id_expected: read_block.block_header.block_id,
                        block_id_actual: commit_tag.block_id,
                    })));
                }

                let task_done = task::TaskDone::ReadBlock(task::TaskDoneReadBlock {
                    block_id: read_block.block_header.block_id,
                    block_bytes: read_block.block_bytes,
                    reply_tx: read_block.reply_tx,
                });
                if let Err(_send_error) = reply_tx.send(task_done) {
                    break;
                }

                cursor += work_block.len() as u64;
                work_block.clear();
            },
        }
    }

    log::debug!("master channel closed in interpret_loop, shutting down");
    Ok(())
}

struct WheelState {
    wheel_file: fs::File,
    work_block: Vec<u8>,
    schema: schema::Schema,
    state: State,
}

async fn wheel_create(state: State) -> Result<WheelState, ErrorSeverity<State, Error>> {
    log::debug!("creating new wheel file [ {:?} ]", state.params.wheel_filename);

    let maybe_file = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(&state.params.wheel_filename)
        .await;
    let mut wheel_file = match maybe_file {
        Ok(file) =>
            file,
        Err(error) =>
            return Err(ErrorSeverity::Fatal(Error::WheelFileOpen {
                wheel_filename: state.params.wheel_filename,
                error,
            })),
    };
    let mut work_block: Vec<u8> = Vec::with_capacity(state.params.work_block_size);

    let storage_layout = storage::Layout::calculate(&mut work_block)
        .map_err(Error::StorageLayoutCalculate)
        .map_err(ErrorSeverity::Fatal)?;

    let wheel_header = storage::WheelHeader {
        size_bytes: state.params.init_wheel_size_bytes,
        ..storage::WheelHeader::default()
    };
    bincode::serialize_into(&mut work_block, &wheel_header)
        .map_err(Error::WheelHeaderSerialize)
        .map_err(ErrorSeverity::Fatal)?;
    bincode::serialize_into(&mut work_block, &storage::EofTag::default())
        .map_err(Error::EofTagSerialize)
        .map_err(ErrorSeverity::Fatal)?;

    let mut cursor = work_block.len();
    let min_wheel_file_size = storage_layout.wheel_header_size + storage_layout.eof_tag_size;
    assert_eq!(cursor, min_wheel_file_size);
    wheel_file.write_all(&work_block).await
        .map_err(Error::WheelHeaderAndEofTagWrite)
        .map_err(ErrorSeverity::Fatal)?;

    let size_bytes_total = state.params.init_wheel_size_bytes;
    if size_bytes_total < min_wheel_file_size {
        return Err(ErrorSeverity::Fatal(Error::InitWheelSizeIsTooSmall {
            provided: size_bytes_total,
            required_min: min_wheel_file_size,
        }));
    }

    work_block.clear();
    work_block.extend((0 .. state.params.work_block_size).map(|_| 0));

    while cursor < size_bytes_total {
        let bytes_remain = size_bytes_total - cursor;
        let write_amount = if bytes_remain < state.params.work_block_size {
            bytes_remain
        } else {
            state.params.work_block_size
        };
        wheel_file.write_all(&work_block[.. write_amount]).await
            .map_err(Error::ZeroChunkWrite)
            .map_err(ErrorSeverity::Fatal)?;
        cursor += write_amount;
    }
    wheel_file.flush().await
        .map_err(Error::WheelCreateFlush)
        .map_err(ErrorSeverity::Fatal)?;

    let mut schema = schema::Schema::new(storage_layout);
    schema.initialize_empty(size_bytes_total);

    log::debug!("initialized wheel schema: {:?}", schema);

    Ok(WheelState { wheel_file, work_block, schema, state, })
}

async fn wheel_open(state: State) -> Result<WheelState, ErrorSeverity<State, Error>> {
    log::debug!("opening existing wheel file [ {:?} ]", state.params.wheel_filename);

    unimplemented!()
}
