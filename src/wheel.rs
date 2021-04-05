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

use edeltraud::{
    Edeltraud,
};

use alloc_pool::bytes::{
    Bytes,
    BytesMut,
    BytesPool,
};

use super::{
    job,
    block,
    proto,
    storage,
    context,
    Params,
    Flushed,
    Deleted,
    IterBlocks,
    IterBlocksItem,
    InterpretStats,
    InterpreterParams,
    blockwheel_context::Context,
};

pub mod core;
use self::core::{
    task,
    performer,
};

pub mod interpret;

mod lru;

#[derive(Debug)]
pub enum Error {
    InterpreterInit(performer::BuilderError),
    InterpreterOpen(interpret::OpenError),
    InterpreterCreate(interpret::CreateError),
    InterpreterRun(interpret::RunError),
    InterpreterAppendTerminator(interpret::AppendTerminatorError),
    InterpreterCrash,
    ThreadPoolGone,
    BlockPrepareWrite(interpret::BlockPrepareWriteJobError),
    BlockProcessRead(interpret::BlockProcessReadJobError),
    BlockPrepareDelete(interpret::BlockPrepareDeleteJobError),
}

type Request = proto::Request<Context>;

pub struct State<J> where J: edeltraud::Job {
    pub parent_supervisor: SupervisorPid,
    pub thread_pool: Edeltraud<J>,
    pub blocks_pool: BytesPool,
    pub fused_request_rx: stream::Fuse<mpsc::Receiver<Request>>,
    pub params: Params,
}

pub async fn busyloop_init<J>(mut supervisor_pid: SupervisorPid, state: State<J>) -> Result<(), ErrorSeverity<State<J>, Error>>
where J: edeltraud::Job + From<job::Job>,
      J::Output: From<job::JobOutput>,
      job::JobOutput: From<J::Output>,
{
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

    let (interpreter_pid, performer, interpret_error_rx) = match state.params.interpreter {

        InterpreterParams::FixedFile(ref interpreter_params) => {
            let open_async = interpret::fixed_file::GenServer::open(
                interpret::fixed_file::OpenParams {
                    wheel_filename: &interpreter_params.wheel_filename,
                },
                performer_builder,
            );
            let interpret::fixed_file::WheelData { gen_server: interpreter_gen_server, performer, } = match open_async.await {
                Ok(interpret::fixed_file::WheelOpenStatus::Success(wheel_data)) =>
                    wheel_data,
                Ok(interpret::fixed_file::WheelOpenStatus::FileNotFound { performer_builder, }) => {
                    let create_async = interpret::fixed_file::GenServer::create(
                        interpret::fixed_file::CreateParams {
                            wheel_filename: &interpreter_params.wheel_filename,
                            init_wheel_size_bytes: interpreter_params.init_wheel_size_bytes,
                        },
                        performer_builder,
                    );
                    create_async.await
                        .map_err(interpret::CreateError::FixedFile)
                        .map_err(Error::InterpreterCreate)
                        .map_err(ErrorSeverity::Fatal)?
                },
                Err(interpret::fixed_file::WheelOpenError::FileWrongType) => {
                    log::error!("[ {:?} ] is not a file", interpreter_params.wheel_filename);
                    return Err(ErrorSeverity::Recoverable { state, });
                },
                Err(error) =>
                    return Err(ErrorSeverity::Fatal(Error::InterpreterOpen(interpret::OpenError::FixedFile(error)))),
            };

            let interpreter_pid = interpreter_gen_server.pid();
            let interpreter_task = interpreter_gen_server.run();
            let (interpret_error_tx, interpret_error_rx) = oneshot::channel();
            supervisor_pid.spawn_link_permanent(
                async move {
                    if let Err(interpret_error) = interpreter_task.await {
                        interpret_error_tx.send(ErrorSeverity::Fatal(Error::InterpreterRun(interpret::RunError::FixedFile(interpret_error)))).ok();
                    }
                },
            );

            (interpreter_pid, performer, interpret_error_rx)
        },

        InterpreterParams::Ram(ref interpreter_params) => {
            let create_async = interpret::ram::GenServer::create(
                interpret::ram::CreateParams {
                    init_wheel_size_bytes: interpreter_params.init_wheel_size_bytes,
                },
                performer_builder,
            );
            let interpret::ram::WheelData { gen_server: interpreter_gen_server, performer, } = create_async.await
                .map_err(interpret::CreateError::Ram)
                .map_err(Error::InterpreterCreate)
                .map_err(ErrorSeverity::Fatal)?;

            let interpreter_pid = interpreter_gen_server.pid();
            let interpreter_task = interpreter_gen_server.run();
            let (interpret_error_tx, interpret_error_rx) = oneshot::channel();
            supervisor_pid.spawn_link_permanent(
                async move {
                    if let Err(interpret_error) = interpreter_task.await {
                        interpret_error_tx.send(ErrorSeverity::Fatal(Error::InterpreterRun(interpret::RunError::Ram(interpret_error)))).ok();
                    }
                },
            );

            (interpreter_pid, performer, interpret_error_rx)
        },

    };

    busyloop(supervisor_pid, interpreter_pid, interpret_error_rx.fuse(), state, performer).await
}

async fn busyloop<J>(
    _supervisor_pid: SupervisorPid,
    mut interpreter_pid: interpret::Pid<Context>,
    mut fused_interpret_error_rx: future::Fuse<oneshot::Receiver<ErrorSeverity<(), Error>>>,
    mut state: State<J>,
    performer: performer::Performer<Context>,
)
    -> Result<(), ErrorSeverity<State<J>, Error>>
where J: edeltraud::Job + From<job::Job>,
      J::Output: From<job::JobOutput>,
      job::JobOutput: From<J::Output>,
{
    let mut job_tasks = FuturesUnordered::new();
    let mut iter_tasks = FuturesUnordered::new();

    let mut op = performer.next();
    loop {
        op = match op {

            performer::Op::Idle(performer) =>
                performer.next(),

            performer::Op::Query(performer::QueryOp::PollRequestAndInterpreter(poll)) => {
                enum Source<A, B, C, D, E> {
                    Pid(A),
                    InterpreterDone(B),
                    InterpreterError(C),
                    IterTask(D),
                    JobTask(E),
                }

                let mut fused_interpret_result_rx = poll.interpreter_context;
                loop {
                    let source = match (iter_tasks.is_empty(), job_tasks.is_empty()) {
                        (true, true) =>
                            select! {
                                result = state.fused_request_rx.next() =>
                                    Source::Pid(result),
                                result = fused_interpret_result_rx =>
                                    Source::InterpreterDone(result),
                                result = fused_interpret_error_rx =>
                                    Source::InterpreterError(result),
                            },
                        (false, true) =>
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
                            },
                        (true, false) =>
                            select! {
                                result = state.fused_request_rx.next() =>
                                    Source::Pid(result),
                                result = fused_interpret_result_rx =>
                                    Source::InterpreterDone(result),
                                result = fused_interpret_error_rx =>
                                    Source::InterpreterError(result),
                                result = job_tasks.next() => match result {
                                    None =>
                                        unreachable!(),
                                    Some(job_task_done) =>
                                        Source::JobTask(job_task_done),
                                },
                            },
                        (false, false) =>
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
                                result = job_tasks.next() => match result {
                                    None =>
                                        unreachable!(),
                                    Some(job_task_done) =>
                                        Source::JobTask(job_task_done),
                                },
                            },
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
                        Source::JobTask(Ok(JobDone::BlockPrepareWrite { block_id, context, done, })) =>
                            poll.next.prepared_write_block_done(
                                block_id,
                                task::WriteBlock {
                                    write_block_bytes: done.write_block_bytes,
                                    context,
                                },
                            ),
                        Source::JobTask(Ok(JobDone::BlockProcessRead { context, done, stats, })) =>
                            poll.next.incoming_task_done_stats( // ??? wtf to do
                                task::Done {
                                    current_offset: done.current_offset,
                                    task: task::TaskDone {
                                        block_id: done.block_id,
                                        kind: task::TaskDoneKind::ReadBlock(task::TaskDoneReadBlock {
                                            block_bytes: done.block_bytes,
                                            context,
                                        }),
                                    },
                                },
                                stats,
                            ),
                        Source::JobTask(Ok(JobDone::BlockPrepareDelete { block_id, context, done, })) =>
                            poll.next.prepared_delete_block_done(
                                block_id,
                                task::DeleteBlock {
                                    delete_block_bytes: done.delete_block_bytes,
                                    context,
                                },
                            ),
                        Source::JobTask(Err(error)) =>
                            return Err(ErrorSeverity::Fatal(error)),
                    }
                }
            },

            performer::Op::Query(performer::QueryOp::PollRequest(poll)) => {
                enum Source<A, B, C, D> {
                    Pid(A),
                    InterpreterError(B),
                    IterTask(C),
                    JobTask(D),
                }

                loop {
                    let source = match (iter_tasks.is_empty(), job_tasks.is_empty()) {
                        (true, true) =>
                            select! {
                                result = state.fused_request_rx.next() =>
                                    Source::Pid(result),
                                result = fused_interpret_error_rx =>
                                    Source::InterpreterError(result),
                            },
                        (false, true) =>
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
                            },
                        (true, false) =>
                            select! {
                                result = state.fused_request_rx.next() =>
                                    Source::Pid(result),
                                result = fused_interpret_error_rx =>
                                    Source::InterpreterError(result),
                                result = job_tasks.next() => match result {
                                    None =>
                                        unreachable!(),
                                    Some(job_task_done) =>
                                        Source::JobTask(job_task_done),
                                },
                            },
                        (false, false) =>
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
                                result = job_tasks.next() => match result {
                                    None =>
                                        unreachable!(),
                                    Some(job_task_done) =>
                                        Source::JobTask(job_task_done),
                                },
                            },
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
                        Source::JobTask(Ok(JobDone::BlockPrepareWrite { block_id, context, done, })) =>
                            poll.next.prepared_write_block_done(
                                block_id,
                                task::WriteBlock {
                                    write_block_bytes: done.write_block_bytes,
                                    context,
                                },
                            ),
                        Source::JobTask(Ok(JobDone::BlockProcessRead { context: reply_tx, done, })) => {
                            if let Err(_send_error) = reply_tx.send(Ok(done.block_bytes)) {
                                log::warn!("client channel was closed before a block is actually read");
                            }
                            continue;
                        },
                        Source::JobTask(Ok(JobDone::BlockPrepareDelete { block_id, context, done, })) =>
                            poll.next.prepared_delete_block_done(
                                block_id,
                                task::DeleteBlock {
                                    delete_block_bytes: done.delete_block_bytes,
                                    context,
                                },
                            ),
                        Source::JobTask(Err(error)) =>
                            return Err(ErrorSeverity::Fatal(error)),
                    }
                }
            },

            performer::Op::Query(performer::QueryOp::InterpretTask(performer::InterpretTask { offset, mut task, force_terminator, next, })) => {
                match &mut task {
                    task::Task { kind: task::TaskKind::WriteBlock(task::WriteBlock { write_block_bytes: block_bytes, .. }), .. } |
                    task::Task { kind: task::TaskKind::DeleteBlock(task::DeleteBlock { delete_block_bytes: block_bytes, .. }), .. }
                    if force_terminator => {
                        interpret::block_append_terminator(block_bytes)
                            .map_err(|error| ErrorSeverity::Fatal(Error::InterpreterAppendTerminator(error)))?
                    },
                    _ =>
                        (),
                }
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
                let interpret::Synced = interpreter_pid.device_sync().await
                    .map_err(|ero::NoProcError| ErrorSeverity::Fatal(Error::InterpreterCrash))?;
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

            performer::Op::Event(performer::Event {
                op: performer::EventOp::PrepareInterpretTask(
                    performer::PrepareInterpretTaskOp {
                        block_id,
                        task: performer::PrepareInterpretTaskKind::WriteBlock(performer::PrepareInterpretTaskWriteBlock {
                            block_bytes,
                            context,
                        }),
                    },
                ),
                performer,
            }) => {
                job_tasks.push(make_job_task(
                    JobTask::BlockPrepareWrite {
                        block_id,
                        block_bytes,
                        blocks_pool: state.blocks_pool.clone(),
                        context,
                    },
                    state.thread_pool.clone(),
                ));
                performer.next()
            },

            performer::Op::Event(performer::Event {
                op: performer::EventOp::PrepareInterpretTask(
                    performer::PrepareInterpretTaskOp {
                        block_id,
                        task: performer::PrepareInterpretTaskKind::DeleteBlock(performer::PrepareInterpretTaskDeleteBlock {
                            context,
                        }),
                    },
                ),
                performer,
            }) => {
                job_tasks.push(make_job_task(
                    JobTask::BlockPrepareDelete {
                        block_id,
                        blocks_pool: state.blocks_pool.clone(),
                        context,
                    },
                    state.thread_pool.clone(),
                ));
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

enum JobTask<C> where C: context::Context {
    BlockPrepareWrite {
        block_id: block::Id,
        block_bytes: Bytes,
        blocks_pool: BytesPool,
        context: task::WriteBlockContext<C::WriteBlock>,
    },
    BlockProcessRead {
        offset: u64,
        storage_layout: storage::Layout,
        block_header: storage::BlockHeader,
        block_bytes: BytesMut,
        context: task::ReadBlockContext<C>,
        stats: InterpretStats,
    },
    BlockPrepareDelete {
        block_id: block::Id,
        blocks_pool: BytesPool,
        context: task::DeleteBlockContext<C::DeleteBlock>,
    },
}

enum JobDone<C> where C: context::Context {
    BlockPrepareWrite {
        block_id: block::Id,
        context: task::WriteBlockContext<C::WriteBlock>,
        done: interpret::BlockPrepareWriteJobDone,
    },
    BlockProcessRead {
        context: task::ReadBlockContext<C>,
        done: interpret::BlockProcessReadJobDone,
        stats: InterpretStats,
    },
    BlockPrepareDelete {
        block_id: block::Id,
        context: task::DeleteBlockContext<C::DeleteBlock>,
        done: interpret::BlockPrepareDeleteJobDone,
    },
}

async fn make_job_task<C, J>(
    job_task: JobTask<C>,
    thread_pool: Edeltraud<J>,
)
    -> Result<JobDone<C>, Error>
where C: context::Context + Send,
      J: edeltraud::Job + From<job::Job>,
      J::Output: From<job::JobOutput>,
      job::JobOutput: From<J::Output>,
{
    match job_task {

        JobTask::BlockPrepareWrite { block_id, block_bytes, blocks_pool, context, } => {
            let job = job::Job::BlockPrepareWrite(interpret::BlockPrepareWriteJobArgs { block_id: block_id.clone(), block_bytes, blocks_pool, });
            let job_output = thread_pool.spawn(job).await
                .map_err(|edeltraud::SpawnError::ThreadPoolGone| Error::ThreadPoolGone)?;
            let job_output: job::JobOutput = job_output.into();
            let job::BlockPrepareWriteDone(block_prepare_write_result) = job_output.into();
            let done = block_prepare_write_result
                .map_err(Error::BlockPrepareWrite)?;
            Ok(JobDone::BlockPrepareWrite { block_id, context, done, })
        },

        JobTask::BlockProcessRead {
            offset,
            storage_layout,
            block_header,
            block_bytes,
            context,
            stats,
        } => {
            let job = job::Job::BlockProcessRead(interpret::BlockProcessReadJobArgs { offset, storage_layout, block_header, block_bytes, });
            let job_output = thread_pool.spawn(job).await
                .map_err(|edeltraud::SpawnError::ThreadPoolGone| Error::ThreadPoolGone)?;
            let job_output: job::JobOutput = job_output.into();
            let job::BlockProcessReadDone(block_process_read_result) = job_output.into();
            let done = block_process_read_result
                .map_err(Error::BlockProcessRead)?;
            Ok(JobDone::BlockProcessRead { context, done, stats, })
        },

        JobTask::BlockPrepareDelete { block_id, blocks_pool, context, } => {
            let job = job::Job::BlockPrepareDelete(interpret::BlockPrepareDeleteJobArgs { block_id: block_id.clone(), blocks_pool, });
            let job_output = thread_pool.spawn(job).await
                .map_err(|edeltraud::SpawnError::ThreadPoolGone| Error::ThreadPoolGone)?;
            let job_output: job::JobOutput = job_output.into();
            let job::BlockPrepareDeleteDone(block_prepare_delete_result) = job_output.into();
            let done = block_prepare_delete_result
                .map_err(Error::BlockPrepareDelete)?;
            Ok(JobDone::BlockPrepareDelete { block_id, context, done, })
        },

    }
}
