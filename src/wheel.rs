use futures::{
    future,
    select,
    stream::{
        self,
        FuturesUnordered,
    },
    channel::{
        mpsc,
        oneshot,
    },
    SinkExt,
    StreamExt,
    FutureExt,
};

use ero::{
    ErrorSeverity,
    supervisor::SupervisorPid,
};

use alloc_pool::bytes::{
    Bytes,
    BytesPool,
};

use super::{
    block,
    proto,
    storage,
    context,
    Params,
    Flushed,
    Deleted,
    IterBlocks,
    IterBlocksItem,
    blockwheel_context::Context,
};

pub mod core;
use self::core::{
    performer,
};

pub mod interpret;

mod lru;

#[derive(Debug)]
pub enum Error {
    InterpreterInit(performer::BuilderError),
    InterpreterOpen(interpret::fixed_file::WheelOpenError),
    InterpreterCreate(interpret::fixed_file::WheelCreateError),
    InterpreterRun(interpret::fixed_file::Error),
    InterpreterCrash,
}

type Request = proto::Request<Context>;

pub struct State {
    pub parent_supervisor: SupervisorPid,
    pub blocks_pool: BytesPool,
    pub fused_request_rx: stream::Fuse<mpsc::Receiver<Request>>,
    pub params: Params,
}

pub async fn busyloop_init(mut supervisor_pid: SupervisorPid, state: State) -> Result<(), ErrorSeverity<State, Error>> {
    let performer_builder = performer::PerformerBuilderInit::new(
        lru::Cache::new(state.params.lru_cache_size_bytes),
        state.blocks_pool.clone(),
        if state.params.defrag_parallel_tasks_limit == 0 {
            None
        } else {
            Some(performer::DefragConfig::new(state.params.defrag_parallel_tasks_limit))
        },
        state.params.work_block_size_bytes,
    )
        .map_err(Error::InterpreterInit)
        .map_err(ErrorSeverity::Fatal)?;

    let open_async = interpret::fixed_file::GenServer::open(
        interpret::fixed_file::OpenParams {
            wheel_filename: &state.params.wheel_filename,
        },
        performer_builder,
    );
    let interpret::fixed_file::WheelData { gen_server: interpreter_gen_server, performer, } = match open_async.await {
        Ok(interpret::fixed_file::WheelOpenStatus::Success(wheel_data)) =>
            wheel_data,
        Ok(interpret::fixed_file::WheelOpenStatus::FileNotFound { performer_builder, }) => {
            let create_async = interpret::fixed_file::GenServer::create(
                interpret::fixed_file::CreateParams {
                    wheel_filename: &state.params.wheel_filename,
                    init_wheel_size_bytes: state.params.init_wheel_size_bytes,
                },
                performer_builder,
            );
            create_async.await
                .map_err(Error::InterpreterCreate)
                .map_err(ErrorSeverity::Fatal)?
        },
        Err(interpret::fixed_file::WheelOpenError::FileWrongType) => {
            log::error!("[ {:?} ] is not a file", state.params.wheel_filename);
            return Err(ErrorSeverity::Recoverable { state, });
        },
        Err(error) =>
            return Err(ErrorSeverity::Fatal(Error::InterpreterOpen(error))),
    };

    let interpreter_pid = interpreter_gen_server.pid();
    let interpreter_task = interpreter_gen_server.run();
    let (interpret_error_tx, interpret_error_rx) = oneshot::channel();
    supervisor_pid.spawn_link_permanent(
        async move {
            if let Err(interpret_error) = interpreter_task.await {
                interpret_error_tx.send(ErrorSeverity::Fatal(Error::InterpreterRun(interpret_error))).ok();
            }
        },
    );

    busyloop(supervisor_pid, interpreter_pid, interpret_error_rx.fuse(), state, performer).await
}

async fn busyloop(
    _supervisor_pid: SupervisorPid,
    mut interpreter_pid: interpret::fixed_file::Pid<Context>,
    mut fused_interpret_error_rx: future::Fuse<oneshot::Receiver<ErrorSeverity<(), Error>>>,
    mut state: State,
    performer: performer::Performer<Context>,
)
    -> Result<(), ErrorSeverity<State, Error>>
{
    let mut iter_tasks = FuturesUnordered::new();

    let mut op = performer.next();
    loop {
        op = match op {

            performer::Op::Idle(performer) =>
                performer.next(),

            performer::Op::Query(performer::QueryOp::PollRequestAndInterpreter(poll)) => {
                enum Source<A, B, C, D> {
                    Pid(A),
                    InterpreterDone(B),
                    InterpreterError(C),
                    IterTask(D),
                }

                let mut fused_interpret_result_rx = poll.interpreter_context;
                loop {
                    let source = if iter_tasks.is_empty() {
                        select! {
                            result = state.fused_request_rx.next() =>
                                Source::Pid(result),
                            result = fused_interpret_result_rx =>
                                Source::InterpreterDone(result),
                            result = fused_interpret_error_rx =>
                                Source::InterpreterError(result),
                        }
                    } else {
                        select! {
                            result = state.fused_request_rx.next() =>
                                Source::Pid(result),
                            result = fused_interpret_result_rx =>
                                Source::InterpreterDone(result),
                            result = fused_interpret_error_rx =>
                                Source::InterpreterError(result),
                            result = iter_tasks.next() => match result {
                                None =>
                                    unreachable!(),
                                Some(iter_task_done) =>
                                    Source::IterTask(iter_task_done),
                            },
                        }
                    };
                    break match source {
                        Source::Pid(Some(request)) =>
                            poll.next.incoming_request(request, fused_interpret_result_rx),
                        Source::InterpreterDone(Ok(interpret::DoneTask { task_done, stats, })) =>
                            poll.next.incoming_task_done_stats(task_done, stats),
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
                        Source::IterTask(IterTaskDone::PeerLost) => {
                            log::debug!("client closed iteration channel");
                            continue;
                        },
                        Source::IterTask(IterTaskDone::ItemSent(iter_block_state)) =>
                            poll.next.incoming_iter_blocks(iter_block_state, fused_interpret_result_rx),
                        Source::IterTask(IterTaskDone::Finished) => {
                            log::debug!("iteration finished");
                            continue;
                        },
                    }
                }
            },

            performer::Op::Query(performer::QueryOp::PollRequest(poll)) => {
                enum Source<A, B, C> {
                    Pid(A),
                    InterpreterError(B),
                    IterTask(C),
                }

                loop {
                    let source = if iter_tasks.is_empty() {
                        select! {
                            result = state.fused_request_rx.next() =>
                                Source::Pid(result),
                            result = fused_interpret_error_rx =>
                                Source::InterpreterError(result),
                        }
                    } else {
                        select! {
                            result = state.fused_request_rx.next() =>
                                Source::Pid(result),
                            result = fused_interpret_error_rx =>
                                Source::InterpreterError(result),
                            result = iter_tasks.next() => match result {
                                None =>
                                    unreachable!(),
                                Some(iter_task_done) =>
                                    Source::IterTask(iter_task_done),
                            },
                        }
                    };
                    break match source {
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
                        Source::IterTask(IterTaskDone::PeerLost) => {
                            log::debug!("client closed iteration channel");
                            continue;
                        },
                        Source::IterTask(IterTaskDone::ItemSent(iter_block_state)) =>
                            poll.next.incoming_iter_blocks(iter_block_state),
                        Source::IterTask(IterTaskDone::Finished) => {
                            log::debug!("iteration finished");
                            continue;
                        },
                    }
                }
            },

            performer::Op::Query(performer::QueryOp::InterpretTask(performer::InterpretTask { offset, task, next, })) => {
                let reply_rx = interpreter_pid.push_request(offset, task).await
                    .map_err(|ero::NoProcError| ErrorSeverity::Fatal(Error::InterpreterCrash))?;
                let performer = next.task_accepted(reply_rx.fuse());
                performer.next()
            },

            performer::Op::Query(performer::QueryOp::MakeIterBlocksStream(performer::MakeIterBlocksStream {
                blocks_total_count,
                blocks_total_size,
                iter_blocks_context: reply_tx,
                next,
            })) => {
                let (iter_blocks_tx, iter_blocks_rx) = mpsc::channel(0);
                let iter_blocks = IterBlocks {
                    blocks_total_count,
                    blocks_total_size,
                    blocks_rx: iter_blocks_rx,
                };
                if let Err(_send_error) = reply_tx.send(iter_blocks) {
                    log::warn!("Pid is gone during IterBlocks query result send");
                }
                next.stream_ready(iter_blocks_tx)
            },

            performer::Op::Event(performer::Event {
                op: performer::EventOp::Info(
                    performer::TaskDoneOp { context: reply_tx, op: performer::InfoOp::Success { info, }, },
                ),
                performer,
            }) => {
                if let Err(_send_error) = reply_tx.send(info) {
                    log::warn!("Pid is gone during Info query result send");
                }
                performer.next()
            },

            performer::Op::Event(performer::Event {
                op: performer::EventOp::Flush(
                    performer::TaskDoneOp { context: reply_tx, op: performer::FlushOp::Flushed, },
                ),
                performer,
            }) => {
                if let Err(_send_error) = reply_tx.send(Flushed) {
                    log::warn!("Pid is gone during Flush query result send");
                }
                performer.next()
            },

            performer::Op::Event(performer::Event {
                op: performer::EventOp::WriteBlock(
                    performer::TaskDoneOp { context: reply_tx, op: performer::WriteBlockOp::NoSpaceLeft, },
                ),
                performer,
            }) => {
                if let Err(_send_error) = reply_tx.send(Err(super::blockwheel_context::RequestWriteBlockError::NoSpaceLeft)) {
                    log::warn!("reply channel has been closed during WriteBlock result send");
                }
                performer.next()
            },

            performer::Op::Event(performer::Event {
                op: performer::EventOp::WriteBlock(
                    performer::TaskDoneOp { context: reply_tx, op: performer::WriteBlockOp::Done { block_id, }, },
                ),
                performer,
            }) => {
                if let Err(_send_error) = reply_tx.send(Ok(block_id)) {
                    log::warn!("client channel was closed before a block is actually written");
                }
                performer.next()
            },

            performer::Op::Event(performer::Event {
                op: performer::EventOp::ReadBlock(
                    performer::TaskDoneOp { context: reply_tx, op: performer::ReadBlockOp::NotFound, },
                ),
                performer,
            }) => {
                if let Err(_send_error) = reply_tx.send(Err(super::blockwheel_context::RequestReadBlockError::NotFound)) {
                    log::warn!("reply channel has been closed during ReadBlock result send");
                }
                performer.next()
            },

            performer::Op::Event(performer::Event {
                op: performer::EventOp::ReadBlock(
                    performer::TaskDoneOp { context: reply_tx, op: performer::ReadBlockOp::Done { block_bytes, }, },
                ),
                performer,
            }) => {
                if let Err(_send_error) = reply_tx.send(Ok(block_bytes)) {
                    log::warn!("client channel was closed before a block is actually read");
                }
                performer.next()
            },

            performer::Op::Event(performer::Event {
                op: performer::EventOp::DeleteBlock(
                    performer::TaskDoneOp { context: reply_tx, op: performer::DeleteBlockOp::NotFound, },
                ),
                performer,
            }) => {
                if let Err(_send_error) = reply_tx.send(Err(super::blockwheel_context::RequestDeleteBlockError::NotFound)) {
                    log::warn!("reply channel has been closed during DeleteBlock result send");
                }
                performer.next()
            },

            performer::Op::Event(performer::Event {
                op: performer::EventOp::DeleteBlock(
                    performer::TaskDoneOp { context: reply_tx, op: performer::DeleteBlockOp::Done { .. }, },
                ),
                performer,
            }) => {
                if let Err(_send_error) = reply_tx.send(Ok(Deleted)) {
                    log::warn!("client channel was closed before a block is actually deleted");
                }
                performer.next()
            },

            performer::Op::Event(performer::Event {
                op: performer::EventOp::IterBlocksItem(
                    performer::IterBlocksItemOp {
                        block_id,
                        block_bytes,
                        iter_blocks_state: performer::IterBlocksState {
                            iter_blocks_stream_context: blocks_tx,
                            iter_blocks_cursor,
                        },
                    },
                ),
                performer,
            }) => {
                iter_tasks.push(push_iter_blocks_item(
                    blocks_tx,
                    IterTask::Item {
                        block_id,
                        block_bytes,
                        iter_blocks_cursor,
                    },
                ));
                performer.next()
            },

            performer::Op::Event(performer::Event {
                op: performer::EventOp::IterBlocksFinish(
                    performer::IterBlocksFinishOp {
                        iter_blocks_stream_context: blocks_tx,
                    },
                ),
                performer,
            }) => {
                iter_tasks.push(push_iter_blocks_item(blocks_tx, IterTask::Finish));
                performer.next()
            },

        };
    }
}

enum IterTask {
    Item {
        block_id: block::Id,
        block_bytes: Bytes,
        iter_blocks_cursor: performer::IterBlocksCursor,
    },
    Finish,
}

enum IterTaskDone {
    PeerLost,
    ItemSent(performer::IterBlocksState<<Context as context::Context>::IterBlocksStream>),
    Finished,
}

async fn push_iter_blocks_item(mut blocks_tx: mpsc::Sender<IterBlocksItem>, task: IterTask) -> IterTaskDone {
    match task {
        IterTask::Item { block_id, block_bytes, iter_blocks_cursor, } => {
            let item = IterBlocksItem::Block { block_id, block_bytes, };
            match blocks_tx.send(item).await {
                Ok(()) =>
                    IterTaskDone::ItemSent(performer::IterBlocksState {
                        iter_blocks_stream_context: blocks_tx,
                        iter_blocks_cursor,
                    }),
                Err(_send_error) =>
                    IterTaskDone::PeerLost,
            }
        },
        IterTask::Finish =>
            match blocks_tx.send(IterBlocksItem::NoMoreBlocks).await {
                Ok(()) =>
                    IterTaskDone::Finished,
                Err(_send_error) =>
                    IterTaskDone::PeerLost,
            }
    }
}
