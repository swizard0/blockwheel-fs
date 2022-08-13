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
    BytesPool,
};

use crate::{
    job,
    block,
    proto,
    storage,
    context,
    Params,
    Flushed,
    IterBlocksItem,
    InterpreterParams,
    blockwheel_context::Context,
};

pub mod core;
use self::core::{
    task,
    performer,
};

pub mod interpret;
pub mod performer_job;

mod lru;

#[derive(Debug)]
pub enum Error {
    InterpreterInit(performer::BuilderError),
    InterpreterOpen(interpret::OpenError),
    InterpreterCreate(interpret::CreateError),
    InterpreterTaskJoin(interpret::TaskJoinError),
    InterpreterRun(interpret::RunError),
    InterpreterCrash,
    ThreadPoolGone,
    BlockPrepareWrite(interpret::BlockPrepareWriteJobError),
    BlockProcessRead(interpret::BlockProcessReadJobError),
    BlockPrepareDelete(interpret::BlockPrepareDeleteJobError),
    PerformerJobRun(performer_job::Error),
}

type Request = proto::Request<Context>;

pub struct State<J> where J: edeltraud::Job {
    pub parent_supervisor: SupervisorPid,
    pub thread_pool: Edeltraud<J>,
    pub blocks_pool: BytesPool,
    pub fused_request_rx: stream::Fuse<mpsc::Receiver<Request>>,
    pub params: Params,
}

pub async fn busyloop_init<J>(supervisor_pid: SupervisorPid, state: State<J>) -> Result<(), ErrorSeverity<State<J>, Error>>
where J: edeltraud::Job + From<job::Job>,
      J::Output: From<job::JobOutput>,
      job::JobOutput: From<J::Output>,
{
    let performer_builder = performer::PerformerBuilderInit::new(
        lru::Cache::new(state.params.lru_cache_size_bytes),
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
            let cloned_interpreter_params = interpreter_params.clone();
            let open_async = tokio::task::spawn_blocking(move || {
                interpret::fixed_file::SyncGenServer::open(
                    interpret::fixed_file::OpenParams {
                        wheel_filename: &cloned_interpreter_params.wheel_filename,
                    },
                    performer_builder,
                )
            });
            let interpret::fixed_file::WheelData { sync_gen_server: interpreter_gen_server, performer, } = match open_async.await {
                Ok(Ok(interpret::fixed_file::WheelOpenStatus::Success(wheel_data))) =>
                    wheel_data,
                Ok(Ok(interpret::fixed_file::WheelOpenStatus::FileNotFound { performer_builder, })) => {
                    let cloned_interpreter_params = interpreter_params.clone();
                    let create_async = tokio::task::spawn_blocking(move || {
                        interpret::fixed_file::SyncGenServer::create(
                            interpret::fixed_file::CreateParams {
                                wheel_filename: &cloned_interpreter_params.wheel_filename,
                                init_wheel_size_bytes: cloned_interpreter_params.init_wheel_size_bytes,
                            },
                            performer_builder,
                        )
                    });
                    match create_async.await {
                        Ok(Ok(data)) =>
                            data,
                        Ok(Err(error)) =>
                            return Err(ErrorSeverity::Fatal(Error::InterpreterCreate(interpret::CreateError::FixedFile(error)))),
                        Err(error) =>
                            return Err(ErrorSeverity::Fatal(
                                Error::InterpreterTaskJoin(
                                    interpret::TaskJoinError::FixedFile(
                                        interpret::fixed_file::TaskJoinError::Create(error),
                                    ),
                                ),
                            )),
                    }
                },
                Ok(Err(interpret::fixed_file::WheelOpenError::FileWrongType)) => {
                    log::error!("[ {:?} ] is not a file", interpreter_params.wheel_filename);
                    return Err(ErrorSeverity::Recoverable { state, });
                },
                Ok(Err(error)) =>
                    return Err(ErrorSeverity::Fatal(Error::InterpreterOpen(interpret::OpenError::FixedFile(error)))),
                Err(error) =>
                    return Err(ErrorSeverity::Fatal(
                        Error::InterpreterTaskJoin(
                            interpret::TaskJoinError::FixedFile(
                                interpret::fixed_file::TaskJoinError::Open(error),
                            ),
                        ),
                    )),
            };

            let interpreter_pid = interpreter_gen_server.pid();
            let (interpret_error_tx, interpret_error_rx) = oneshot::channel();
            interpreter_gen_server
                .run(
                    state.blocks_pool.clone(),
                    interpret_error_tx,
                    |error| ErrorSeverity::Fatal(Error::InterpreterRun(interpret::RunError::FixedFile(error))),
                )
                .map_err(interpret::RunError::FixedFile)
                .map_err(Error::InterpreterRun)
                .map_err(ErrorSeverity::Fatal)?;

            (interpreter_pid, performer, interpret_error_rx)
        },

        InterpreterParams::Ram(ref interpreter_params) => {
            let cloned_interpreter_params = interpreter_params.clone();
            let create_async = tokio::task::spawn_blocking(move || {
                interpret::ram::SyncGenServer::create(
                    interpret::ram::CreateParams {
                        init_wheel_size_bytes: cloned_interpreter_params.init_wheel_size_bytes,
                    },
                    performer_builder,
                )
            });
            let interpret::ram::WheelData { sync_gen_server: interpreter_gen_server, performer, } = match create_async.await {
                Ok(Ok(data)) =>
                    data,
                Ok(Err(error)) =>
                    return Err(ErrorSeverity::Fatal(Error::InterpreterCreate(interpret::CreateError::Ram(error)))),
                Err(error) =>
                    return Err(ErrorSeverity::Fatal(
                        Error::InterpreterTaskJoin(
                            interpret::TaskJoinError::Ram(
                                interpret::ram::TaskJoinError::Create(error),
                            ),
                        ),
                    )),
            };

            let interpreter_pid = interpreter_gen_server.pid();
            let (interpret_error_tx, interpret_error_rx) = oneshot::channel();
            interpreter_gen_server
                .run(
                    state.blocks_pool.clone(),
                    interpret_error_tx,
                    |error| ErrorSeverity::Fatal(Error::InterpreterRun(interpret::RunError::Ram(error))),
                )
                .map_err(interpret::RunError::Ram)
                .map_err(Error::InterpreterRun)
                .map_err(ErrorSeverity::Fatal)?;

            (interpreter_pid, performer, interpret_error_rx)
        },

    };

    busyloop(supervisor_pid, interpreter_pid, interpret_error_rx.fuse(), state, performer).await
}

async fn busyloop<J>(
    _supervisor_pid: SupervisorPid,
    interpreter_pid: interpret::Pid<Context>,
    mut fused_interpret_error_rx: future::Fuse<oneshot::Receiver<ErrorSeverity<(), Error>>>,
    mut state: State<J>,
    performer: performer::Performer<Context>,
)
    -> Result<(), ErrorSeverity<State<J>, Error>>
where J: edeltraud::Job + From<job::Job>,
      J::Output: From<job::JobOutput>,
      job::JobOutput: From<J::Output>,
{
    enum PerformerState {
        Ready {
            job_args: performer_job::JobArgs,
        },
        InProgress,
    }

    let mut performer_state = PerformerState::Ready {
        job_args: performer_job::JobArgs {
            env: performer_job::Env {
                interpreter_pid,
                incoming: Default::default(),
                outgoing: Default::default(),
            },
            kont: performer_job::Kont::Start { performer, },
        },
    };

    let mut incoming = performer_job::Incoming::default();

    enum Mode {
        Regular,
        Flushing,
    }

    let mut mode = Mode::Regular;

    let mut tasks = FuturesUnordered::new();
    let mut tasks_count = 0;

    log::debug!("starting wheel busyloop");
    loop {
        enum PerformerAction<A, S> {
            Run(A),
            KeepState(S),
        }

        let performer_action = match performer_state {
            PerformerState::Ready {
                job_args: job_args @ performer_job::JobArgs { kont: performer_job::Kont::Start { .. }, .. },
            } =>
                PerformerAction::Run(job_args),
            PerformerState::Ready {
                job_args: job_args @ performer_job::JobArgs { kont: performer_job::Kont::PollRequestAndInterpreter { .. }, .. },
            } if !incoming.is_empty() =>
                PerformerAction::Run(job_args),
            PerformerState::Ready {
                job_args: job_args @ performer_job::JobArgs { kont: performer_job::Kont::PollRequest { .. }, .. },
            } if !incoming.is_empty() =>
                PerformerAction::Run(job_args),
            other =>
                PerformerAction::KeepState(other),
        };

        match performer_action {
            PerformerAction::Run(mut job_args) => {
                job_args.env.incoming.transfill_from(&mut incoming);
                if job_args.env.incoming.is_empty() {
                    performer_state = PerformerState::Ready { job_args, };
                } else {
                    job_args.env.incoming.transfill_from(&mut incoming);
                    let job = job::Job::PerformerJobRun(job_args);
                    let job_handle = state.thread_pool.spawn_handle(job)
                        .map_err(|edeltraud::SpawnError::ThreadPoolGone| Error::ThreadPoolGone)
                        .map_err(ErrorSeverity::Fatal)?;
                    tasks.push(Task::<Context, J>::Job(job_handle).run());
                    tasks_count += 1;
                    performer_state = PerformerState::InProgress;
                }
            },
            PerformerAction::KeepState(state) =>
                performer_state = state,
        }

        enum Event<R, T, E> {
            Request(Option<R>),
            Task(T),
            InterpreterError(E),
        }

        let event = match mode {
            Mode::Regular if tasks_count == 0 =>
                select! {
                    result = state.fused_request_rx.next() =>
                        Event::Request(result),
                    result = fused_interpret_error_rx =>
                        Event::InterpreterError(result),
                },
            Mode::Regular =>
                select! {
                    result = state.fused_request_rx.next() =>
                        Event::Request(result),
                    result = fused_interpret_error_rx =>
                        Event::InterpreterError(result),
                    result = tasks.next() =>
                        match result {
                            None =>
                                unreachable!(),
                            Some(task) => {
                                tasks_count -= 1;
                                Event::Task(task)
                            },
                        },
                },
            Mode::Flushing =>
                select! {
                    result = fused_interpret_error_rx =>
                        Event::InterpreterError(result),
                    result = tasks.next() =>
                        match result {
                            None =>
                                unreachable!(),
                            Some(task) => {
                                tasks_count -= 1;
                                Event::Task(task)
                            },
                        },
                },
        };

        match event {

            Event::Request(None) => {
                log::info!("requests sink channel depleted: terminating");
                return Ok(());
            },

            Event::Request(Some(request)) => {
                if let proto::Request::Flush(..) = request {
                    mode = Mode::Flushing;
                }
                incoming.incoming_request.push(request);
            },

            Event::Task(Ok(TaskOutput::Job(performer_job::Done::Poll { mut env, kont, }))) => {
                if let Some(interpret_task) = env.outgoing.interpret_task.take() {
                    let performer_job::QueryInterpretTask { fused_interpret_result_rx, } =
                        interpret_task;
                    tasks.push(Task::InterpreterAwait { fused_interpret_result_rx, }.run());
                    tasks_count += 1;
                }
                if let Some(performer_job::TaskDoneFlush { reply_tx, }) = env.outgoing.task_done_flush.take() {
                    let interpret::Synced = env.interpreter_pid.device_sync().await
                        .map_err(|ero::NoProcError| ErrorSeverity::Fatal(Error::InterpreterCrash))?;
                    if let Err(_send_error) = reply_tx.send(Flushed) {
                        log::warn!("Pid is gone during Flush query result send");
                    }
                    assert!(matches!(mode, Mode::Flushing));
                    mode = Mode::Regular;
                }
                for prepare_write_block in env.outgoing.prepare_write_blocks.drain(..) {
                    let performer_job::PrepareWriteBlock { block_id, block_bytes, context, } =
                        prepare_write_block;
                    let thread_pool = state.thread_pool.clone();
                    let blocks_pool = state.blocks_pool.clone();
                    tasks.push(Task::BlockPrepareWrite { thread_pool, block_id, block_bytes, blocks_pool, context, }.run());
                    tasks_count += 1;
                }
                for prepare_delete_block in env.outgoing.prepare_delete_blocks.drain(..) {
                    let performer_job::PrepareDeleteBlock { block_id, context, } =
                        prepare_delete_block;
                    let thread_pool = state.thread_pool.clone();
                    let blocks_pool = state.blocks_pool.clone();
                    tasks.push(Task::BlockPrepareDelete { thread_pool, block_id, blocks_pool, context, }.run());
                    tasks_count += 1;
                }
                for process_read_block in env.outgoing.process_read_blocks.drain(..) {
                    let performer_job::ProcessReadBlock { storage_layout, block_header, block_bytes, pending_contexts, } =
                        process_read_block;
                    let thread_pool = state.thread_pool.clone();
                    tasks.push(Task::BlockProcessRead { thread_pool, storage_layout, block_header, block_bytes, pending_contexts, }.run());
                    tasks_count += 1;
                }
                for iter_task in env.outgoing.iter_tasks.drain(..) {
                    tasks.push(Task::Iter(iter_task).run());
                    tasks_count += 1;
                }

                performer_state = match performer_state {
                    PerformerState::InProgress =>
                        PerformerState::Ready {
                            job_args: performer_job::JobArgs { env, kont, },
                        },
                    PerformerState::Ready { .. } =>
                        unreachable!(),
                };
            },

            Event::Task(Ok(TaskOutput::BlockPrepareWrite { block_id, context, done, })) =>
                incoming.prepared_write_block_done.push(performer_job::PreparedWriteBlockDone {
                    block_id,
                    write_block_bytes: done.write_block_bytes,
                    context,
                }),

            Event::Task(Ok(TaskOutput::BlockProcessRead { pending_contexts, done, })) =>
                incoming.process_read_block_done.push(performer_job::ProcessReadBlockDone {
                    block_id: done.block_id,
                    block_bytes: done.block_bytes,
                    pending_contexts,
                }),

            Event::Task(Ok(TaskOutput::BlockPrepareDelete { block_id, context, done, })) =>
                incoming.prepared_delete_block_done.push(performer_job::PreparedDeleteBlockDone {
                    block_id,
                    delete_block_bytes: done.delete_block_bytes,
                    context,
                }),

            Event::Task(Ok(TaskOutput::Iter(IterTaskDone::PeerLost))) =>
                log::debug!("client closed iteration channel"),

            Event::Task(Ok(TaskOutput::Iter(IterTaskDone::ItemSent(iter_block_state)))) =>
                incoming.incoming_iter_blocks.push(performer_job::IncomingIterBlocks {
                    iter_block_state,
                }),

            Event::Task(Ok(TaskOutput::Iter(IterTaskDone::Finished))) =>
                log::debug!("iteration finished"),

            Event::Task(Ok(TaskOutput::InterpreterAwait { maybe_done: Ok(interpret::DoneTask { task_done, stats, }), })) =>
                incoming.incoming_task_done_stats.push(performer_job::IncomingTaskDoneStats {
                    task_done,
                    stats,
                }),

            Event::Task(Ok(TaskOutput::InterpreterAwait { maybe_done: Err(oneshot::Canceled), })) => {
                log::debug!("interpreter reply channel closed: shutting down");
                return Ok(());
            },

            Event::Task(Err(error)) =>
                return Err(ErrorSeverity::Fatal(error)),

            Event::InterpreterError(Ok(ErrorSeverity::Recoverable { state: (), })) =>
                return Err(ErrorSeverity::Recoverable { state, }),

            Event::InterpreterError(Ok(ErrorSeverity::Fatal(error))) =>
                return Err(ErrorSeverity::Fatal(error)),

            Event::InterpreterError(Err(oneshot::Canceled)) => {
                log::debug!("interpreter error channel closed: shutting down");
                return Ok(());
            },

        }
    }
}

pub enum IterTask {
    Taken,
    Item {
        blocks_tx: mpsc::Sender<IterBlocksItem>,
        item: IterBlocksItem,
        iter_blocks_cursor: performer::IterBlocksCursor,
    },
    Finish {
        blocks_tx: mpsc::Sender<IterBlocksItem>,
    },
}

pub enum IterTaskDone {
    PeerLost,
    ItemSent(performer::IterBlocksState<<Context as context::Context>::IterBlocksStream>),
    Finished,
}

use std::{
    mem,
    pin::Pin,
    task::Poll,
};

impl future::Future for IterTask {
    type Output = IterTaskDone;

    fn poll(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        match mem::replace(this, IterTask::Taken) {
            IterTask::Taken =>
                panic!("polled IterTask after completion"),
            IterTask::Item { mut blocks_tx, item, iter_blocks_cursor, } => {
                let mut sink = Pin::new(&mut blocks_tx);
                match sink.as_mut().poll_ready(cx) {
                    Poll::Ready(Ok(())) =>
                        (),
                    Poll::Ready(Err(_send_error)) =>
                        return Poll::Ready(IterTaskDone::PeerLost),
                    Poll::Pending => {
                        *this = IterTask::Item { blocks_tx, item, iter_blocks_cursor, };
                        return Poll::Pending;
                    },
                }
                match sink.as_mut().start_send(item) {
                    Ok(()) =>
                        Poll::Ready(IterTaskDone::ItemSent(performer::IterBlocksState {
                            iter_blocks_stream_context: blocks_tx,
                            iter_blocks_cursor,
                        })),
                    Err(_send_error) =>
                        Poll::Ready(IterTaskDone::PeerLost),
                }
            },
            IterTask::Finish { mut blocks_tx, } => {
                let mut sink = Pin::new(&mut blocks_tx);
                match sink.as_mut().poll_ready(cx) {
                    Poll::Ready(Ok(())) =>
                        (),
                    Poll::Ready(Err(_send_error)) =>
                        return Poll::Ready(IterTaskDone::PeerLost),
                    Poll::Pending => {
                        *this = IterTask::Finish { blocks_tx, };
                        return Poll::Pending;
                    },
                }
                match sink.as_mut().start_send(IterBlocksItem::NoMoreBlocks) {
                    Ok(()) =>
                        Poll::Ready(IterTaskDone::Finished),
                    Err(_send_error) =>
                        Poll::Ready(IterTaskDone::PeerLost),
                }
            },
        }
    }
}

enum Task<C, J> where C: context::Context, J: edeltraud::Job {
    Job(edeltraud::Handle<J::Output>),
    BlockPrepareWrite {
        thread_pool: Edeltraud<J>,
        block_id: block::Id,
        block_bytes: Bytes,
        blocks_pool: BytesPool,
        context: task::WriteBlockContext<C::WriteBlock>,
    },
    BlockProcessRead {
        thread_pool: Edeltraud<J>,
        storage_layout: storage::Layout,
        block_header: storage::BlockHeader,
        block_bytes: Bytes,
        pending_contexts: task::queue::PendingReadContextBag,
    },
    BlockPrepareDelete {
        thread_pool: Edeltraud<J>,
        block_id: block::Id,
        blocks_pool: BytesPool,
        context: task::DeleteBlockContext<C::DeleteBlock>,
    },
    Iter(IterTask),
    InterpreterAwait {
        fused_interpret_result_rx: <Context as context::Context>::Interpreter,
    },
}

enum TaskOutput<C> where C: context::Context {
    Job(performer_job::Done),
    BlockPrepareWrite {
        block_id: block::Id,
        context: task::WriteBlockContext<C::WriteBlock>,
        done: interpret::BlockPrepareWriteJobDone,
    },
    BlockProcessRead {
        pending_contexts: task::queue::PendingReadContextBag,
        done: interpret::BlockProcessReadJobDone,
    },
    BlockPrepareDelete {
        block_id: block::Id,
        context: task::DeleteBlockContext<C::DeleteBlock>,
        done: interpret::BlockPrepareDeleteJobDone,
    },
    Iter(IterTaskDone),
    InterpreterAwait {
        maybe_done: Result<interpret::DoneTask<Context>, oneshot::Canceled>,
    },
}

impl<C, J> Task<C, J>
where C: context::Context + Send,
      J: edeltraud::Job + From<job::Job>,
      J::Output: From<job::JobOutput>,
      job::JobOutput: From<J::Output>,
{
    async fn run(self) -> Result<TaskOutput<C>, Error> {
        match self {
            Task::Job(job_handle) => {
                let job_output = job_handle.await
                    .map_err(|edeltraud::SpawnError::ThreadPoolGone| Error::ThreadPoolGone)?;
                let job_output: job::JobOutput = job_output.into();
                let job::PerformerJobRunDone(performer_job_result) = job_output.into();
                let job_done = performer_job_result
                    .map_err(Error::PerformerJobRun)?;
                Ok(TaskOutput::Job(job_done))
            },

            Task::BlockPrepareWrite {
                thread_pool,
                block_id,
                block_bytes,
                blocks_pool,
                context,
            } => {
                let job = job::Job::BlockPrepareWrite(interpret::BlockPrepareWriteJobArgs { block_id: block_id.clone(), block_bytes, blocks_pool, });
                let job_output = thread_pool.spawn(job).await
                    .map_err(|edeltraud::SpawnError::ThreadPoolGone| Error::ThreadPoolGone)?;
                let job_output: job::JobOutput = job_output.into();
                let job::BlockPrepareWriteDone(block_prepare_write_result) = job_output.into();
                let done = block_prepare_write_result
                    .map_err(Error::BlockPrepareWrite)?;
                Ok(TaskOutput::BlockPrepareWrite { block_id, context, done, })
            },

            Task::BlockProcessRead {
                thread_pool,
                storage_layout,
                block_header,
                block_bytes,
                pending_contexts,
            } => {
                let job = job::Job::BlockProcessRead(interpret::BlockProcessReadJobArgs { storage_layout, block_header, block_bytes, });
                let job_output = thread_pool.spawn(job).await
                    .map_err(|edeltraud::SpawnError::ThreadPoolGone| Error::ThreadPoolGone)?;
                let job_output: job::JobOutput = job_output.into();
                let job::BlockProcessReadDone(block_process_read_result) = job_output.into();
                let done = block_process_read_result
                    .map_err(Error::BlockProcessRead)?;
                Ok(TaskOutput::BlockProcessRead { pending_contexts, done, })
            },

            Task::BlockPrepareDelete {
                thread_pool,
                block_id,
                blocks_pool,
                context,
            } => {
                let job = job::Job::BlockPrepareDelete(interpret::BlockPrepareDeleteJobArgs { blocks_pool, });
                let job_output = thread_pool.spawn(job).await
                    .map_err(|edeltraud::SpawnError::ThreadPoolGone| Error::ThreadPoolGone)?;
                let job_output: job::JobOutput = job_output.into();
                let job::BlockPrepareDeleteDone(block_prepare_delete_result) = job_output.into();
                let done = block_prepare_delete_result
                    .map_err(Error::BlockPrepareDelete)?;
                Ok(TaskOutput::BlockPrepareDelete { block_id, context, done, })
            },

            Task::Iter(iter_task) =>
                Ok(TaskOutput::Iter(iter_task.await)),

            Task::InterpreterAwait { fused_interpret_result_rx, } =>
                Ok(TaskOutput::InterpreterAwait { maybe_done: fused_interpret_result_rx.await, }),

        }
    }
}
