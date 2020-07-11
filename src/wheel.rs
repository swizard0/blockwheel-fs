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
};

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
    let mut performer = performer::Performer::new(schema, state.params.lru_cache_size_bytes);

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
                        poll.next.next(performer::RequestOrInterpreterIncoming::Request(request)),
                    Source::InterpreterDone(Ok(task_done)) =>
                        poll.next.next(performer::RequestOrInterpreterIncoming::Interpreter(task_done)),
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
                        poll.next.next(request),
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

            performer::Op::InterpretTask(performer::InterpretTask { offset, task_kind, next, }) => {
                let (reply_tx, reply_rx) = oneshot::channel();
                if let Err(_send_error) = interpret_tx.send(interpret::Request { offset, task_kind, reply_tx, }).await {
                    log::warn!("interpreter request channel closed");
                }
                performer = next.task_accepted(reply_rx.fuse());
                continue;
            },

        };

        unimplemented!()

        // let perform_op = match source {
        //     Source::Pid(None) => {
        //         log::debug!("all Pid frontends have been terminated");
        //         return Ok(());
        //     },

        //     Source::Pid(Some(request)) =>
        //         match target {
        //             target::PollRequestAndInterpreter(poll) =>
        //                 poll.next(perform::PollRequestAndInterpreter::Request(request)),
        //             target::PollRequest(poll) =>
        //                 poll.next(request),
        //         },

        //     //     let block = blocks_pool.lend();
        //     //     if let Err(_send_error) = reply_tx.send(block) {
        //     //         log::warn!("Pid is gone during LendBlock query result send");
        //     //     }
        //     // },

        //     // Source::Pid(Some(proto::Request::RepayBlock(proto::RequestRepayBlock { block_bytes, }))) => {
        //     //     blocks_pool.repay(block_bytes);
        //     // },

        //     // Source::Pid(Some(proto::Request::WriteBlock(request_write_block))) =>
        //     //     match schema.process_write_block_request(&request_write_block.block_bytes) {

        //     //         schema::WriteBlockOp::Perform(schema::WriteBlockPerform { defrag_op, task_op, }) => {
        //     //             match defrag_op {
        //     //                 schema::DefragOp::None =>
        //     //                     (),
        //     //                 schema::DefragOp::Queue { free_space_offset, space_key, } =>
        //     //                     defrag_task_queue.push(free_space_offset, space_key),
        //     //             }

        //     //             tasks_queue.push(
        //     //                 bg_task.current_offset,
        //     //                 task_op.block_offset,
        //     //                 task::TaskKind::WriteBlock(
        //     //                     task::WriteBlock {
        //     //                         block_id: task_op.block_id,
        //     //                         block_bytes: request_write_block.block_bytes,
        //     //                         commit_type: match task_op.commit_type {
        //     //                             schema::WriteBlockTaskCommitType::CommitOnly =>
        //     //                                 task::CommitType::CommitOnly,
        //     //                             schema::WriteBlockTaskCommitType::CommitAndEof =>
        //     //                                 task::CommitType::CommitAndEof,
        //     //                         },
        //     //                         context: task::WriteBlockContext::External(
        //     //                             task::WriteBlockContextExternal {
        //     //                                 reply_tx: request_write_block.reply_tx,
        //     //                             },
        //     //                         ),
        //     //                     },
        //     //                 ),
        //     //             );
        //     //         },

        //     //         schema::WriteBlockOp::QueuePendingDefrag => {
        //     //             log::debug!(
        //     //                 "cannot directly allocate {} bytes in process_write_block_request: moving to pending defrag queue",
        //     //                 request_write_block.block_bytes.len(),
        //     //             );
        //     //             defrag_pending_queue.push(request_write_block);
        //     //         },

        //     //         schema::WriteBlockOp::ReplyNoSpaceLeft => {
        //     //             if let Err(_send_error) = request_write_block.reply_tx.send(Err(proto::RequestWriteBlockError::NoSpaceLeft)) {
        //     //                 log::warn!("reply channel has been closed during WriteBlock result send");
        //     //             }
        //     //         },
        //     //     }

        //     // Source::Pid(Some(proto::Request::ReadBlock(request_read_block))) =>
        //     //     if let Some(block_bytes) = lru_cache.get(&request_read_block.block_id) {
        //     //         if let Err(_send_error) = request_read_block.reply_tx.send(Ok(block_bytes.clone())) {
        //     //             log::warn!("pid is gone during ReadBlock query result send");
        //     //         }
        //     //     } else {
        //     //         match schema.process_read_block_request(&request_read_block.block_id) {

        //     //             schema::ReadBlockOp::Perform(schema::ReadBlockPerform { block_offset, block_header, }) => {
        //     //                 let block_bytes = blocks_pool.lend();
        //     //                 tasks_queue.push(
        //     //                     bg_task.current_offset,
        //     //                     block_offset,
        //     //                     task::TaskKind::ReadBlock(task::ReadBlock {
        //     //                         block_header: block_header,
        //     //                         block_bytes,
        //     //                         context: task::ReadBlockContext::External(
        //     //                             task::ReadBlockContextExternal {
        //     //                                 reply_tx: request_read_block.reply_tx,
        //     //                             },
        //     //                         ),
        //     //                     }),
        //     //                 );
        //     //             },

        //     //             schema::ReadBlockOp::NotFound =>
        //     //                 if let Err(_send_error) = request_read_block.reply_tx.send(Err(proto::RequestReadBlockError::NotFound)) {
        //     //                     log::warn!("reply channel has been closed during ReadBlock result send");
        //     //                 },

        //     //         }
        //     //     },

        //     // Source::Pid(Some(proto::Request::DeleteBlock(request_delete_block))) => {
        //     //     lru_cache.invalidate(&request_delete_block.block_id);
        //     //     match schema.process_delete_block_request(&request_delete_block.block_id) {

        //     //         schema::DeleteBlockOp::Perform(schema::DeleteBlockPerform { block_offset, }) => {
        //     //             tasks_queue.push(
        //     //                 bg_task.current_offset,
        //     //                 block_offset,
        //     //                 task::TaskKind::MarkTombstone(task::MarkTombstone {
        //     //                     block_id: request_delete_block.block_id,
        //     //                     context: task::MarkTombstoneContext::External(
        //     //                         task::MarkTombstoneContextExternal {
        //     //                             reply_tx: request_delete_block.reply_tx,
        //     //                         },
        //     //                     ),
        //     //                 }),
        //     //             );
        //     //         },

        //     //         schema::DeleteBlockOp::NotFound => {
        //     //             if let Err(_send_error) = request_delete_block.reply_tx.send(Err(proto::RequestDeleteBlockError::NotFound)) {
        //     //                 log::warn!("reply channel has been closed during DeleteBlock result send");
        //     //             }
        //     //         },

        //     //     }
        //     // },

        //     Source::InterpreterDone(Ok(task::Done { current_offset, task: task::TaskDone::WriteBlock(write_block), })) => {
        //         bg_task = BackgroundTask { current_offset, state: BackgroundTaskState::Idle, };
        //         match write_block.context {
        //             task::WriteBlockContext::External(context) =>
        //                 if let Err(_send_error) = context.reply_tx.send(Ok(write_block.block_id)) {
        //                     log::warn!("client channel was closed before a block is actually written");
        //                 },
        //         }
        //     },

        //     Source::InterpreterDone(Ok(task::Done { current_offset, task: task::TaskDone::ReadBlock(read_block), })) => {
        //         let block_bytes = read_block.block_bytes.freeze();
        //         lru_cache.insert(read_block.block_id.clone(), block_bytes.clone());
        //         bg_task = BackgroundTask { current_offset, state: BackgroundTaskState::Idle, };
        //         match read_block.context {
        //             task::ReadBlockContext::External(context) =>
        //                 if let Err(_send_error) = context.reply_tx.send(Ok(block_bytes)) {
        //                     log::warn!("client channel was closed before a block is actually read");
        //                 },
        //         }
        //     },

        //     Source::InterpreterDone(Ok(task::Done { current_offset, task: task::TaskDone::MarkTombstone(mark_tombstone), })) => {
        //         match schema.process_tombstone_written(mark_tombstone.block_id) {
        //             schema::TombstoneWrittenOp::Perform(schema::TombstoneWrittenPerform { defrag_op, }) => {
        //                 match defrag_op {
        //                     schema::DefragOp::None =>
        //                         (),
        //                     schema::DefragOp::Queue { free_space_offset, space_key, } =>
        //                         defrag_task_queue.push(free_space_offset, space_key),
        //                 }
        //             },
        //         }
        //         bg_task = BackgroundTask { current_offset, state: BackgroundTaskState::Idle, };
        //         match mark_tombstone.context {
        //             task::MarkTombstoneContext::External(context) =>
        //                 if let Err(_send_error) = context.reply_tx.send(Ok(proto::Deleted)) {
        //                     log::warn!("client channel was closed before a block is actually deleted");
        //                 },
        //         }
        //     },

        //     Source::InterpreterDone(Err(oneshot::Canceled)) => {
        //         log::debug!("interpreter reply channel closed: shutting down");
        //         return Ok(());
        //     },

        //     Source::InterpreterError(Ok(ErrorSeverity::Recoverable { state: (), })) =>
        //         return Err(ErrorSeverity::Recoverable { state, }),

        //     Source::InterpreterError(Ok(ErrorSeverity::Fatal(error))) =>
        //         return Err(ErrorSeverity::Fatal(error)),

        //     Source::InterpreterError(Err(oneshot::Canceled)) => {
        //         log::debug!("interpreter error channel closed: shutting down");
        //         return Ok(());
        //     },
        // }
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
