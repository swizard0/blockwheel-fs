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
        // AsyncReadExt,
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
    Deleted,
};

pub mod context;

mod lru;
mod gaps;
mod task;
mod pool;
mod index;
mod defrag;
mod schema;
mod performer;
mod interpret;

#[derive(Debug)]
pub enum Error {
    Interpret(interpret::Error),
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
}

type Request = proto::Request<context::blockwheel::Context>;

pub struct State {
    pub parent_supervisor: SupervisorPid,
    pub fused_request_rx: stream::Fuse<mpsc::Receiver<Request>>,
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
            let interpret_task = interpret::busyloop(
                interpret_rx,
                wheel_file,
                work_block,
                storage_layout,
            );
            if let Err(interpret_error) = interpret_task.await {
                interpret_error_tx.send(ErrorSeverity::Fatal(Error::Interpret(interpret_error))).ok();
            }
        },
    );

    busyloop(supervisor_pid, interpret_tx, interpret_error_rx.fuse(), state, schema).await
}

async fn busyloop(
    _supervisor_pid: SupervisorPid,
    mut interpret_tx: mpsc::Sender<interpret::Request>,
    mut fused_interpret_error_rx: future::Fuse<oneshot::Receiver<ErrorSeverity<(), Error>>>,
    mut state: State,
    schema: schema::Schema,
)
    -> Result<(), ErrorSeverity<State, Error>>
{
    let mut performer = performer::Performer::new(
        schema,
        lru::Cache::new(state.params.lru_cache_size_bytes),
        if state.params.disable_defragmentation {
            None
        } else {
            Some(defrag::Queues {
                pending: defrag::PendingQueue::new(),
                tasks: defrag::TaskQueue::new(),
            })
        },
    );

    loop {
        let perform_op = match performer.next() {

            performer::Op::PollRequestAndInterpreter(poll) => {
                enum Source<A, B, C> {
                    Pid(A),
                    InterpreterDone(B),
                    InterpreterError(C),
                }

                let mut fused_interpret_result_rx = poll.interpreter_context;
                let source = select! {
                    result = state.fused_request_rx.next() =>
                        Source::Pid(result),
                    result = fused_interpret_result_rx =>
                        Source::InterpreterDone(result),
                    result = fused_interpret_error_rx =>
                        Source::InterpreterError(result),
                };
                match source {
                    Source::Pid(Some(request)) =>
                        poll.next.incoming_request(request, fused_interpret_result_rx),
                    Source::InterpreterDone(Ok(task_done)) =>
                        poll.next.incoming_task_done(task_done),
                    Source::Pid(None) => {
                        log::debug!("all Pid frontends have been terminated");
                        return Ok(());
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
            },

            performer::Op::PollRequest(poll) => {
                enum Source<A, B> {
                    Pid(A),
                    InterpreterError(B),
                }

                let source = select! {
                    result = state.fused_request_rx.next() =>
                        Source::Pid(result),
                    result = fused_interpret_error_rx =>
                        Source::InterpreterError(result),
                };
                match source {
                    Source::Pid(Some(request)) =>
                        poll.next.incoming_request(request),
                    Source::Pid(None) => {
                        log::debug!("all Pid frontends have been terminated");
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
            },

            performer::Op::InterpretTask(performer::InterpretTask { offset, task, next, }) => {
                let (reply_tx, reply_rx) = oneshot::channel();
                if let Err(_send_error) = interpret_tx.send(interpret::Request { offset, task, reply_tx, }).await {
                    log::warn!("interpreter request channel closed");
                }
                performer = next.task_accepted(reply_rx.fuse());
                continue;
            },

        };

        performer = match perform_op {

            performer::PerformOp::Idle(performer) =>
                performer,

            performer::PerformOp::LendBlock(performer::LendBlockOp::Success { block_bytes, context: reply_tx, performer, }) => {
                if let Err(_send_error) = reply_tx.send(block_bytes) {
                    log::warn!("Pid is gone during LendBlock query result send");
                }
                performer
            },

            performer::PerformOp::WriteBlock(performer::WriteBlockOp::NoSpaceLeft { context: reply_tx, performer, }) => {
                if let Err(_send_error) = reply_tx.send(Err(context::blockwheel::RequestWriteBlockError::NoSpaceLeft)) {
                    log::warn!("reply channel has been closed during WriteBlock result send");
                }
                performer
            },

            performer::PerformOp::ReadBlock(performer::ReadBlockOp::CacheHit { context: reply_tx, block_bytes, performer, }) => {
                if let Err(_send_error) = reply_tx.send(Ok(block_bytes)) {
                    log::warn!("pid is gone during ReadBlock query result send");
                }
                performer
            },

            performer::PerformOp::ReadBlock(performer::ReadBlockOp::NotFound { context: reply_tx, performer, }) => {
                if let Err(_send_error) = reply_tx.send(Err(context::blockwheel::RequestReadBlockError::NotFound)) {
                    log::warn!("reply channel has been closed during ReadBlock result send");
                }
                performer
            },

            performer::PerformOp::DeleteBlock(performer::DeleteBlockOp::NotFound { context: reply_tx, performer, }) => {
                if let Err(_send_error) = reply_tx.send(Err(context::blockwheel::RequestDeleteBlockError::NotFound)) {
                    log::warn!("reply channel has been closed during DeleteBlock result send");
                }
                performer
            },

            performer::PerformOp::TaskDone(performer::TaskDone::WriteBlockDone(
                performer::WriteBlockDoneOp { block_id, context: reply_tx, performer, },
            )) => {
                if let Err(_send_error) = reply_tx.send(Ok(block_id)) {
                    log::warn!("client channel was closed before a block is actually written");
                }
                performer
            },

            performer::PerformOp::TaskDone(performer::TaskDone::ReadBlockDone(
                mut done_op @ performer::ReadBlockDoneOp { .. },
            )) =>
                loop {
                    let performer::ReadBlockDoneOp { block_bytes, context: reply_tx, next, } = done_op;
                    if let Err(_send_error) = reply_tx.send(Ok(block_bytes)) {
                        log::warn!("client channel was closed before a block is actually read");
                    }
                    match next.step() {
                        performer::TaskReadBlockDoneLoop::Done(performer) =>
                            break performer,
                        performer::TaskReadBlockDoneLoop::More(done_op_more) =>
                            done_op = done_op_more,
                    }
                },

            performer::PerformOp::TaskDone(performer::TaskDone::DeleteBlockDone(
                mut done_op @ performer::DeleteBlockDoneOp { .. },
            )) =>
                loop {
                    let performer::DeleteBlockDoneOp { context: reply_tx, next, } = done_op;
                    if let Err(_send_error) = reply_tx.send(Ok(Deleted)) {
                        log::warn!("client channel was closed before a block is actually deleted");
                    }
                    match next.step() {
                        performer::TaskDeleteBlockDoneLoop::Done(performer) =>
                            break performer,
                        performer::TaskDeleteBlockDoneLoop::More(done_op_more) =>
                            done_op = done_op_more,
                    }
                },
        };
    }
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
    let mut work_block: Vec<u8> = Vec::with_capacity(state.params.work_block_size_bytes);

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
    work_block.extend((0 .. state.params.work_block_size_bytes).map(|_| 0));

    while cursor < size_bytes_total {
        let bytes_remain = size_bytes_total - cursor;
        let write_amount = if bytes_remain < state.params.work_block_size_bytes {
            bytes_remain
        } else {
            state.params.work_block_size_bytes
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
