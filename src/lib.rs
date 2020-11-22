#![forbid(unsafe_code)]

use std::{
    path::PathBuf,
    time::Duration,
};

use futures::{
    stream,
    channel::{
        mpsc,
        oneshot,
    },
    StreamExt,
    SinkExt,
};

use alloc_pool::bytes::{
    Bytes,
    BytesPool,
};

use ero::{
    restart,
    RestartStrategy,
    supervisor::SupervisorPid,
};

pub mod block;

mod wheel;
mod proto;
mod storage;
mod context;

#[cfg(test)]
mod tests;

#[derive(Clone, Debug)]
pub struct Params {
    pub wheel_filename: PathBuf,
    pub init_wheel_size_bytes: usize,
    pub wheel_task_restart_sec: usize,
    pub wheel_task_tasks_limit: usize,
    pub work_block_size_bytes: usize,
    pub lru_cache_size_bytes: usize,
    pub defrag_parallel_tasks_limit: usize,
}

impl Default for Params {
    fn default() -> Params {
        Params {
            wheel_filename: "wheel".to_string().into(),
            init_wheel_size_bytes: 64 * 1024 * 1024,
            wheel_task_restart_sec: 4,
            wheel_task_tasks_limit: 64,
            work_block_size_bytes: 8 * 1024 * 1024,
            lru_cache_size_bytes: 16 * 1024 * 1024,
            defrag_parallel_tasks_limit: 1,
        }
    }
}

type Request = proto::Request<blockwheel_context::Context>;

pub struct GenServer {
    request_tx: mpsc::Sender<Request>,
    fused_request_rx: stream::Fuse<mpsc::Receiver<Request>>,
}

#[derive(Clone)]
pub struct Pid {
    request_tx: mpsc::Sender<Request>,
}

impl GenServer {
    pub fn new() -> GenServer {
        let (request_tx, request_rx) = mpsc::channel(0);
        GenServer {
            request_tx,
            fused_request_rx: request_rx.fuse(),
        }
    }

    pub fn pid(&self) -> Pid {
        Pid {
            request_tx: self.request_tx.clone(),
        }
    }

    pub async fn run(self, parent_supervisor: SupervisorPid, blocks_pool: BytesPool, params: Params) {
        let terminate_result = restart::restartable(
            ero::Params {
                name: format!("ero-blockwheel on {:?}", params.wheel_filename),
                restart_strategy: RestartStrategy::Delay {
                    restart_after: Duration::from_secs(params.wheel_task_restart_sec as u64),
                },
            },
            wheel::State {
                parent_supervisor,
                blocks_pool,
                fused_request_rx: self.fused_request_rx,
                params,
            },
            |mut state| async move {
                let child_supervisor_gen_server = state.parent_supervisor.child_supervisor();
                let child_supervisor_pid = child_supervisor_gen_server.pid();
                state.parent_supervisor.spawn_link_temporary(
                    child_supervisor_gen_server.run(),
                );
                wheel::busyloop_init(child_supervisor_pid, state).await
            },
        ).await;
        if let Err(error) = terminate_result {
            log::error!("fatal error: {:?}", error);
        }
    }
}

#[derive(Debug)]
pub enum WriteBlockError {
    GenServer(ero::NoProcError),
    NoSpaceLeft,
}

#[derive(Debug)]
pub enum ReadBlockError {
    GenServer(ero::NoProcError),
    NotFound,
}

#[derive(Debug)]
pub enum DeleteBlockError {
    GenServer(ero::NoProcError),
    NotFound,
}

#[derive(Debug)]
pub enum IterBlocksError {
    GenServer(ero::NoProcError),
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct Deleted;

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct Flushed;

#[derive(Clone, Copy, PartialEq, Eq, Hash, Default, Debug)]
pub struct Info {
    pub blocks_count: usize,
    pub wheel_size_bytes: usize,
    pub service_bytes_used: usize,
    pub data_bytes_used: usize,
    pub defrag_write_pending_bytes: usize,
    pub bytes_free: usize,
    pub interpret_stats: InterpretStats,
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Default, Debug)]
pub struct InterpretStats {
    pub count_total: usize,
    pub count_no_seek: usize,
    pub count_seek_forward: usize,
    pub count_seek_backward: usize,
}

pub struct IterBlocks {
    pub blocks_total_count: usize,
    pub blocks_total_size: usize,
    pub blocks_rx: mpsc::Receiver<IterBlocksItem>,
}

pub enum IterBlocksItem {
    Block { block_id: block::Id, block_bytes: Bytes, },
    NoMoreBlocks,
}

impl Pid {
    pub async fn info(&mut self) -> Result<Info, ero::NoProcError> {
        loop {
            let (reply_tx, reply_rx) = oneshot::channel();
            self.request_tx.send(proto::Request::Info(proto::RequestInfo { context: reply_tx, })).await
                .map_err(|_send_error| ero::NoProcError)?;
            match reply_rx.await {
                Ok(info) =>
                    return Ok(info),
                Err(oneshot::Canceled) =>
                    (),
            }
        }
    }

    pub async fn flush(&mut self) -> Result<Flushed, ero::NoProcError> {
        loop {
            let (reply_tx, reply_rx) = oneshot::channel();
            self.request_tx.send(proto::Request::Flush(proto::RequestFlush { context: reply_tx, })).await
                .map_err(|_send_error| ero::NoProcError)?;
            match reply_rx.await {
                Ok(Flushed) =>
                    return Ok(Flushed),
                Err(oneshot::Canceled) =>
                    (),
            }
        }
    }

    pub async fn write_block(&mut self, block_bytes: Bytes) -> Result<block::Id, WriteBlockError> {
        loop {
            let (reply_tx, reply_rx) = oneshot::channel();
            self.request_tx
                .send(proto::Request::WriteBlock(proto::RequestWriteBlock {
                    block_bytes: block_bytes.clone(),
                    context: reply_tx,
                }))
                .await
                .map_err(|_send_error| WriteBlockError::GenServer(ero::NoProcError))?;

            match reply_rx.await {
                Ok(Ok(block_id)) =>
                    return Ok(block_id),
                Ok(Err(blockwheel_context::RequestWriteBlockError::NoSpaceLeft)) =>
                    return Err(WriteBlockError::NoSpaceLeft),
                Err(oneshot::Canceled) =>
                    (),
            }
        }
    }

    pub async fn read_block(&mut self, block_id: block::Id) -> Result<Bytes, ReadBlockError> {
        loop {
            let (reply_tx, reply_rx) = oneshot::channel();
            self.request_tx
                .send(proto::Request::ReadBlock(proto::RequestReadBlock {
                    block_id: block_id.clone(),
                    context: reply_tx,
                }))
                .await
                .map_err(|_send_error| ReadBlockError::GenServer(ero::NoProcError))?;

            match reply_rx.await {
                Ok(Ok(block_bytes)) =>
                    return Ok(block_bytes),
                Ok(Err(blockwheel_context::RequestReadBlockError::NotFound)) =>
                    return Err(ReadBlockError::NotFound),
                Err(oneshot::Canceled) =>
                    (),
            }
        }
    }

    pub async fn delete_block(&mut self, block_id: block::Id) -> Result<Deleted, DeleteBlockError> {
        loop {
            let (reply_tx, reply_rx) = oneshot::channel();
            self.request_tx
                .send(proto::Request::DeleteBlock(proto::RequestDeleteBlock {
                    block_id: block_id.clone(),
                    context: reply_tx,
                }))
                .await
                .map_err(|_send_error| DeleteBlockError::GenServer(ero::NoProcError))?;

            match reply_rx.await {
                Ok(Ok(Deleted)) =>
                    return Ok(Deleted),
                Ok(Err(blockwheel_context::RequestDeleteBlockError::NotFound)) =>
                    return Err(DeleteBlockError::NotFound),
                Err(oneshot::Canceled) =>
                    (),
            }
        }
    }

    pub async fn iter_blocks(&mut self) -> Result<IterBlocks, IterBlocksError> {
        loop {
            let (reply_tx, reply_rx) = oneshot::channel();
            self.request_tx
                .send(proto::Request::IterBlocks(proto::RequestIterBlocks { context: reply_tx, })).await
                .map_err(|_send_error| IterBlocksError::GenServer(ero::NoProcError))?;

            match reply_rx.await {
                Ok(iter_blocks) =>
                    return Ok(iter_blocks),
                Err(oneshot::Canceled) =>
                    (),
            }
        }
    }
}

mod blockwheel_context {
    use futures::{
        channel::{
            mpsc,
            oneshot,
        },
        future,
    };

    use alloc_pool::bytes::Bytes;

    use super::{
        block,
        context,
        wheel::{
            interpret,
        },
        Info,
        Deleted,
        Flushed,
        IterBlocks,
        IterBlocksItem,
    };

    pub struct Context;

    impl context::Context for Context {
        type Info = oneshot::Sender<Info>;
        type Flush = oneshot::Sender<Flushed>;
        type WriteBlock = oneshot::Sender<Result<block::Id, RequestWriteBlockError>>;
        type ReadBlock = oneshot::Sender<Result<Bytes, RequestReadBlockError>>;
        type DeleteBlock = oneshot::Sender<Result<Deleted, RequestDeleteBlockError>>;
        type IterBlocks = oneshot::Sender<IterBlocks>;
        type IterBlocksStream = mpsc::Sender<IterBlocksItem>;
        type Interpreter = future::Fuse<interpret::RequestReplyRx<Self>>;
    }

    #[derive(Clone, PartialEq, Eq, Debug)]
    pub enum RequestWriteBlockError {
        NoSpaceLeft,
    }

    #[derive(Clone, PartialEq, Eq, Debug)]
    pub enum RequestReadBlockError {
        NotFound,
    }

    #[derive(Clone, PartialEq, Eq, Debug)]
    pub enum RequestDeleteBlockError {
        NotFound,
    }
}
