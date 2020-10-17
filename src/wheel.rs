use futures::{
    future,
    select,
    stream,
    channel::{
        mpsc,
        oneshot,
    },
    StreamExt,
    FutureExt,
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
    Flushed,
    Deleted,
    blockwheel_context::Context,
};

pub mod core;
use self::core::{
    schema,
    performer,
};

mod lru;
mod pool;
mod interpret;

#[derive(Debug)]
pub enum Error {
    InterpreterOpen(interpret::fixed_file::WheelOpenError),
    InterpreterCreate(interpret::fixed_file::WheelCreateError),
    InterpreterRun(interpret::fixed_file::Error),
    InterpreterCrash,
}

type Request = proto::Request<Context>;

pub struct State {
    pub parent_supervisor: SupervisorPid,
    pub fused_request_rx: stream::Fuse<mpsc::Receiver<Request>>,
    pub params: Params,
}

pub async fn busyloop_init(mut supervisor_pid: SupervisorPid, state: State) -> Result<(), ErrorSeverity<State, Error>> {
    let open_async = interpret::fixed_file::GenServer::open(interpret::fixed_file::OpenParams {
        wheel_filename: &state.params.wheel_filename,
        work_block_size_bytes: state.params.work_block_size_bytes,
    });
    let interpreter_gen_server = match open_async.await {
        Ok(gen_server) =>
            gen_server,
        Err(interpret::fixed_file::WheelOpenError::FileWrongType) => {
            log::error!("[ {:?} ] is not a file", state.params.wheel_filename);
            return Err(ErrorSeverity::Recoverable { state, });
        },
        Err(interpret::fixed_file::WheelOpenError::FileNotFound) => {
            let create_async = interpret::fixed_file::GenServer::create(interpret::fixed_file::CreateParams {
                wheel_filename: &state.params.wheel_filename,
                work_block_size_bytes: state.params.work_block_size_bytes,
                init_wheel_size_bytes: state.params.init_wheel_size_bytes,
            });
            create_async.await
                .map_err(Error::InterpreterCreate)
                .map_err(ErrorSeverity::Fatal)?
        },
        Err(error) =>
            return Err(ErrorSeverity::Fatal(Error::InterpreterOpen(error))),
    };

    let interpreter_pid = interpreter_gen_server.pid();
    let (schema, interpreter_task) = interpreter_gen_server.run();
    let (interpret_error_tx, interpret_error_rx) = oneshot::channel();
    supervisor_pid.spawn_link_permanent(
        async move {
            if let Err(interpret_error) = interpreter_task.await {
                interpret_error_tx.send(ErrorSeverity::Fatal(Error::InterpreterRun(interpret_error))).ok();
            }
        },
    );

    busyloop(supervisor_pid, interpreter_pid, interpret_error_rx.fuse(), state, schema).await
}

async fn busyloop(
    _supervisor_pid: SupervisorPid,
    mut interpreter_pid: interpret::fixed_file::Pid<Context>,
    mut fused_interpret_error_rx: future::Fuse<oneshot::Receiver<ErrorSeverity<(), Error>>>,
    mut state: State,
    schema: schema::Schema,
)
    -> Result<(), ErrorSeverity<State, Error>>
{
    let performer = performer::Performer::new(
        schema,
        lru::Cache::new(state.params.lru_cache_size_bytes),
        if state.params.defrag_parallel_tasks_limit == 0 {
            None
        } else {
            Some(performer::DefragConfig::new(state.params.defrag_parallel_tasks_limit))
        },
    );

    let mut op = performer.next();
    loop {
        op = match op {

            performer::Op::Idle(performer) =>
                performer.next(),

            performer::Op::Query(performer::QueryOp::PollRequestAndInterpreter(poll)) => {
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

            performer::Op::Query(performer::QueryOp::PollRequest(poll)) => {
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

            performer::Op::Query(performer::QueryOp::InterpretTask(performer::InterpretTask { offset, task, next, })) => {
                let reply_rx = interpreter_pid.push_request(offset, task).await
                    .map_err(|ero::NoProcError| ErrorSeverity::Fatal(Error::InterpreterCrash))?;
                let performer = next.task_accepted(reply_rx.fuse());
                performer.next()
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
                op: performer::EventOp::LendBlock(
                    performer::TaskDoneOp { context: reply_tx, op: performer::LendBlockOp::Success { block_bytes, }, },
                ),
                performer,
            }) => {
                if let Err(_send_error) = reply_tx.send(block_bytes) {
                    log::warn!("Pid is gone during LendBlock query result send");
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

        };
    }
}
