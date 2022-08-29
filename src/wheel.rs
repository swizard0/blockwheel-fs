use futures::{
    future,
    select,
    stream,
    channel::{
        mpsc,
        oneshot,
    },
    StreamExt,
};

use ero::{
    ErrorSeverity,
    supervisor::SupervisorPid,
};

use alloc_pool::bytes::{
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
    blockwheel_context::{
        Context,
        IterBlocksContext,
        IterBlocksStreamContext,
    },
};

pub mod core;
use self::core::{
    performer,
};

pub mod interpret;
pub mod performer_sklave;

mod lru;

#[derive(Debug)]
pub enum Error {
    InterpreterInit(performer::BuilderError),
    InterpreterOpen(interpret::OpenError),
    InterpreterCreate(interpret::CreateError),
    InterpreterTaskJoin(interpret::TaskJoinError),
    InterpreterRun(interpret::RunError),
    Arbeitssklave(arbeitssklave::Error),
    PerformerSklave(performer_sklave::Error),
}

type Request = proto::Request<Context>;

pub struct State<J> where J: edeltraud::Job {
    pub parent_supervisor: SupervisorPid,
    pub thread_pool: edeltraud::Edeltraud<J>,
    pub blocks_pool: BytesPool,
    pub fused_request_rx: stream::Fuse<mpsc::Receiver<Request>>,
    pub params: Params,
}

pub async fn busyloop_init<J>(
    supervisor_pid: SupervisorPid,
    state: State<J>,
)
    -> Result<(), ErrorSeverity<State<J>, Error>>
where J: edeltraud::Job<Output = ()> + From<job::Job>,
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

    let (meister, performer_sklave_error_rx) = match state.params.interpreter {

        InterpreterParams::FixedFile(ref interpreter_params) => {
            let cloned_interpreter_params = interpreter_params.clone();
            let open_async = tokio::task::spawn_blocking(move || {
                interpret::fixed_file::SyncGenServerInit::open(
                    interpret::fixed_file::OpenParams {
                        wheel_filename: &cloned_interpreter_params.wheel_filename,
                    },
                    performer_builder,
                )
            });
            let interpret::fixed_file::WheelData { sync_gen_server_init: interpreter_gen_server_init, performer, } = match open_async.await {
                Ok(Ok(interpret::fixed_file::WheelOpenStatus::Success(wheel_data))) =>
                    wheel_data,
                Ok(Ok(interpret::fixed_file::WheelOpenStatus::FileNotFound { performer_builder, })) => {
                    let cloned_interpreter_params = interpreter_params.clone();
                    let create_async = tokio::task::spawn_blocking(move || {
                        interpret::fixed_file::SyncGenServerInit::create(
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

            let interpreter_gen_server = interpreter_gen_server_init.finish();
            let interpreter_pid = interpreter_gen_server.pid();
            let (performer_sklave_error_tx, performer_sklave_error_rx) = mpsc::unbounded();

            let meister =
                arbeitssklave::Meister::start(
                    performer_sklave::Welt {
                        env: performer_sklave::Env {
                            interpreter_pid,
                            blocks_pool: state.blocks_pool.clone(),
                            error_tx: performer_sklave_error_tx,
                        },
                        kont: performer_sklave::Kont::Start { performer, },
                    },
                    &edeltraud::EdeltraudJobMap::new(&state.thread_pool),
                )
                .map_err(Error::Arbeitssklave)
                .map_err(ErrorSeverity::Fatal)?;

            interpreter_gen_server
                .run(
                    meister.clone(),
                    edeltraud::EdeltraudJobMap::new(state.thread_pool.clone()),
                    state.blocks_pool.clone(),
                )
                .map_err(interpret::RunError::FixedFile)
                .map_err(Error::InterpreterRun)
                .map_err(ErrorSeverity::Fatal)?;

            (meister, performer_sklave_error_rx)
        },

        InterpreterParams::Ram(ref interpreter_params) => {
            let cloned_interpreter_params = interpreter_params.clone();
            let create_async = tokio::task::spawn_blocking(move || {
                interpret::ram::SyncGenServerInit::create(
                    interpret::ram::CreateParams {
                        init_wheel_size_bytes: cloned_interpreter_params.init_wheel_size_bytes,
                    },
                    performer_builder,
                )
            });
            let interpret::ram::WheelData { sync_gen_server_init: interpreter_gen_server_init, performer, } = match create_async.await {
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

            let interpreter_gen_server = interpreter_gen_server_init.finish();
            let interpreter_pid = interpreter_gen_server.pid();
            let (performer_sklave_error_tx, performer_sklave_error_rx) = mpsc::unbounded();

            let meister =
                arbeitssklave::Meister::start(
                    performer_sklave::Welt {
                        env: performer_sklave::Env {
                            interpreter_pid,
                            blocks_pool: state.blocks_pool.clone(),
                            error_tx: performer_sklave_error_tx,
                        },
                        kont: performer_sklave::Kont::Start { performer, },
                    },
                    &edeltraud::EdeltraudJobMap::new(&state.thread_pool),
                )
                .map_err(Error::Arbeitssklave)
                .map_err(ErrorSeverity::Fatal)?;

            interpreter_gen_server
                .run(
                    meister.clone(),
                    edeltraud::EdeltraudJobMap::new(state.thread_pool.clone()),
                    state.blocks_pool.clone(),
                )
                .map_err(interpret::RunError::Ram)
                .map_err(Error::InterpreterRun)
                .map_err(ErrorSeverity::Fatal)?;

            (meister, performer_sklave_error_rx)
        },

    };

    busyloop(supervisor_pid, meister, performer_sklave_error_rx.fuse(), state).await
}

async fn busyloop<J>(
    mut supervisor_pid: SupervisorPid,
    meister: performer_sklave::Meister,
    mut fused_performer_sklave_error_rx: stream::Fuse<mpsc::UnboundedReceiver<performer_sklave::Error>>,
    mut state: State<J>,
)
    -> Result<(), ErrorSeverity<State<J>, Error>>
where J: edeltraud::Job<Output = ()> + From<job::Job>,
{
    enum Mode {
        Regular,
        AwaitFlush {
            done_rx: oneshot::Receiver<Flushed>,
            reply_tx: oneshot::Sender<Flushed>,
        },
        Flushed {
            reply_tx: oneshot::Sender<Flushed>,
        },
    }

    let mut mode = Mode::Regular;

    log::debug!("starting wheel busyloop");
    loop {
        enum Event<R, E, F> {
            Request(Option<R>),
            PerformerError(E),
            FlushDone(F),
        }

        let event = match mode {
            Mode::Regular => {
                mode = Mode::Regular;
                select! {
                    result = state.fused_request_rx.next() =>
                        Event::Request(result),
                    result = fused_performer_sklave_error_rx.next() =>
                        Event::PerformerError(result),
                }
            },
            Mode::AwaitFlush { mut done_rx, reply_tx, } =>
                select! {
                    result = fused_performer_sklave_error_rx.next() => {
                        mode = Mode::Regular;
                        Event::PerformerError(result)
                    },
                    result = &mut done_rx => {
                        mode = Mode::Flushed { reply_tx, };
                        Event::FlushDone(result)
                    },
                },
            Mode::Flushed { reply_tx, } => {
                if let Err(_send_error) = reply_tx.send(Flushed) {
                    log::warn!("Pid is gone during Flush query result send");
                }
                mode = Mode::Regular;
                continue;
            },
        };

        match event {

            Event::Request(None) => {
                log::info!("requests sink channel depleted: terminating");
                return Ok(());
            },

            Event::Request(Some(proto::Request::Flush(proto::RequestFlush { context: reply_tx, }))) => {
                let (done_tx, done_rx) = oneshot::channel();
                mode = Mode::AwaitFlush { done_rx, reply_tx, };
                let order = performer_sklave::Order::Request(
                    proto::Request::Flush(proto::RequestFlush { context: done_tx, }),
                );
                match meister.order(order, &edeltraud::EdeltraudJobMap::new(&state.thread_pool)) {
                    Ok(()) =>
                        (),
                    Err(arbeitssklave::Error::Terminated) => {
                        log::debug!("performer sklave is gone, terminating");
                        return Ok(());
                    },
                    Err(error) =>
                        return Err(ErrorSeverity::Fatal(Error::Arbeitssklave(error))),
                }
            },

            Event::Request(Some(proto::Request::IterBlocks(proto::RequestIterBlocks {
                context: IterBlocksContext {
                    reply_tx,
                    maybe_iter_task_tx: None,
                },
            }))) => {
                let (iter_task_tx, iter_task_rx) = oneshot::channel();
                supervisor_pid.spawn_link_temporary(
                    forward_blocks_iter(
                        iter_task_rx,
                        meister.clone(),
                        state.thread_pool.clone(),
                    ),
                );
                let order = performer_sklave::Order::Request(
                    proto::Request::IterBlocks(proto::RequestIterBlocks {
                        context: IterBlocksContext {
                            reply_tx,
                            maybe_iter_task_tx: Some(iter_task_tx),
                        },
                    }),
                );
                match meister.order(order, &edeltraud::EdeltraudJobMap::new(&state.thread_pool)) {
                    Ok(()) =>
                        (),
                    Err(arbeitssklave::Error::Terminated) => {
                        log::debug!("performer sklave is gone, terminating");
                        return Ok(());
                    },
                    Err(error) =>
                        return Err(ErrorSeverity::Fatal(Error::Arbeitssklave(error))),
                }
            },

            Event::Request(Some(proto::Request::IterBlocks(proto::RequestIterBlocks {
                context: IterBlocksContext {
                    maybe_iter_task_tx: Some(..),
                    ..
                },
            }))) =>
                unreachable!(),

            Event::Request(Some(request)) => {
                let order = performer_sklave::Order::Request(request);
                match meister.order(order, &edeltraud::EdeltraudJobMap::new(&state.thread_pool)) {
                    Ok(()) =>
                        (),
                    Err(arbeitssklave::Error::Terminated) => {
                        log::debug!("performer sklave is gone, terminating");
                        return Ok(());
                    },
                    Err(error) =>
                        return Err(ErrorSeverity::Fatal(Error::Arbeitssklave(error))),
                }
            },

            Event::PerformerError(Some(error)) =>
                return Err(ErrorSeverity::Fatal(Error::PerformerSklave(error))),

            Event::PerformerError(None) => {
                log::debug!("interpreter error channel closed: shutting down");
                return Ok(());
            },

            Event::FlushDone(Ok(Flushed)) =>
                todo!(),

            Event::FlushDone(Err(oneshot::Canceled)) => {
                log::debug!("flushed notify channel closed: shutting down");
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
    ItemSent {
        iter_blocks_state: performer::IterBlocksState<<Context as context::Context>::IterBlocksStream>,
        iter_task_rx: oneshot::Receiver<IterTask>,
    },
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
                    Ok(()) => {
                        let (iter_task_tx, iter_task_rx) = oneshot::channel();
                        Poll::Ready(IterTaskDone::ItemSent {
                            iter_blocks_state: performer::IterBlocksState {
                                iter_blocks_stream_context: IterBlocksStreamContext {
                                    blocks_tx,
                                    iter_task_tx,
                                },
                                iter_blocks_cursor,
                            },
                            iter_task_rx,
                        })
                    },
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

async fn forward_blocks_iter<J>(
    iter_task_rx: oneshot::Receiver<IterTask>,
    meister: performer_sklave::Meister,
    thread_pool: edeltraud::Edeltraud<J>,
)
where J: edeltraud::Job<Output = ()> + From<job::Job>,
{
    if let Err(error) = run_forward_blocks_iter(iter_task_rx, meister, thread_pool).await {
        log::debug!("canceling blocks iterator forwarding because of: {error:?}");
    }
}

#[derive(Debug)]
enum ForwardBlocksIterError {
    PerformerSklaveIsLost,
    StreamPeerLost,
    Arbeitssklave(arbeitssklave::Error),
}

async fn run_forward_blocks_iter<J>(
    mut iter_task_rx: oneshot::Receiver<IterTask>,
    meister: performer_sklave::Meister,
    thread_pool: edeltraud::Edeltraud<J>,
)
    -> Result<(), ForwardBlocksIterError>
where J: edeltraud::Job<Output = ()> + From<job::Job>,
{
    loop {
        let iter_task = iter_task_rx.await
            .map_err(|oneshot::Canceled| ForwardBlocksIterError::PerformerSklaveIsLost)?;

        match iter_task.await {
            IterTaskDone::PeerLost =>
                return Err(ForwardBlocksIterError::StreamPeerLost),
            IterTaskDone::ItemSent { iter_blocks_state, iter_task_rx: next_iter_task_rx, } => {
                let order = performer_sklave::Order::IterBlocks(
                    performer_sklave::OrderIterBlocks {
                        iter_blocks_state,
                    },
                );
                meister.order(order, &edeltraud::EdeltraudJobMap::new(&thread_pool))
                    .map_err(ForwardBlocksIterError::Arbeitssklave)?;
                iter_task_rx = next_iter_task_rx;
            },
            IterTaskDone::Finished =>
                return Ok(()),
        }
    }
}
