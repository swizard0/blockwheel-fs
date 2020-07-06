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

use ero::{
    restart,
    RestartStrategy,
    supervisor::SupervisorPid,
};

pub mod block;

mod wheel;
mod proto;
mod storage;

#[derive(Clone, Debug)]
pub struct Params {
    pub wheel_filename: PathBuf,
    pub init_wheel_size_bytes: usize,
    pub wheel_task_restart_sec: usize,
    pub work_block_size_bytes: usize,
    pub lru_cache_size_bytes: usize,
}

impl Default for Params {
    fn default() -> Params {
        Params {
            wheel_filename: "wheel".to_string().into(),
            init_wheel_size_bytes: 64 * 1024 * 1024,
            wheel_task_restart_sec: 4,
            work_block_size_bytes: 8 * 1024 * 1024,
            lru_cache_size_bytes: 16 * 1024 * 1024,
        }
    }
}

pub struct GenServer {
    request_tx: mpsc::Sender<proto::Request>,
    fused_request_rx: stream::Fuse<mpsc::Receiver<proto::Request>>,
}

#[derive(Clone)]
pub struct Pid {
    request_tx: mpsc::Sender<proto::Request>,
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

    pub async fn run(self, parent_supervisor: SupervisorPid, params: Params) {
        let terminate_result = restart::restartable(
            ero::Params {
                name: format!("ero-blockwheel on {:?}", params.wheel_filename),
                restart_strategy: RestartStrategy::Delay {
                    restart_after: Duration::from_secs(params.wheel_task_restart_sec as u64),
                },
            },
            wheel::State {
                parent_supervisor,
                fused_request_rx: self.fused_request_rx,
                params,
            },
            |mut state| async move {
                let child_supervisor_gen_server = state.parent_supervisor.child_supevisor();
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

impl Pid {
    pub async fn lend_block(&mut self) -> Result<block::BytesMut, ero::NoProcError> {
        loop {
            let (reply_tx, reply_rx) = oneshot::channel();
            self.request_tx.send(proto::Request::LendBlock(proto::RequestLendBlock { reply_tx, })).await
                .map_err(|_send_error| ero::NoProcError)?;
            match reply_rx.await {
                Ok(block) =>
                    return Ok(block),
                Err(oneshot::Canceled) =>
                    (),
            }
        }
    }

    pub async fn repay_block(&mut self, block_bytes: block::Bytes) -> Result<(), ero::NoProcError> {
        self.request_tx.send(proto::Request::RepayBlock(proto::RequestRepayBlock { block_bytes, })).await
            .map_err(|_send_error| ero::NoProcError)
    }


    pub async fn write_block(&mut self, block_bytes: block::Bytes) -> Result<block::Id, WriteBlockError> {
        loop {
            let (reply_tx, reply_rx) = oneshot::channel();
            self.request_tx
                .send(proto::Request::WriteBlock(proto::RequestWriteBlock {
                    block_bytes: block_bytes.clone(),
                    reply_tx,
                }))
                .await
                .map_err(|_send_error| WriteBlockError::GenServer(ero::NoProcError))?;

            match reply_rx.await {
                Ok(Ok(block_id)) =>
                    return Ok(block_id),
                Ok(Err(proto::RequestWriteBlockError::NoSpaceLeft)) =>
                    return Err(WriteBlockError::NoSpaceLeft),
                Err(oneshot::Canceled) =>
                    (),
            }
        }
    }

    pub async fn read_block(&mut self, block_id: block::Id) -> Result<block::Bytes, ReadBlockError> {
        loop {
            let (reply_tx, reply_rx) = oneshot::channel();
            self.request_tx
                .send(proto::Request::ReadBlock(proto::RequestReadBlock {
                    block_id: block_id.clone(),
                    reply_tx,
                }))
                .await
                .map_err(|_send_error| ReadBlockError::GenServer(ero::NoProcError))?;

            match reply_rx.await {
                Ok(Ok(block_bytes)) =>
                    return Ok(block_bytes),
                Ok(Err(proto::RequestReadBlockError::NotFound)) =>
                    return Err(ReadBlockError::NotFound),
                Err(oneshot::Canceled) =>
                    (),
            }
        }
    }

    pub async fn delete_block(&mut self, block_id: block::Id) -> Result<proto::Deleted, ero::NoProcError> {
        loop {
            let (reply_tx, reply_rx) = oneshot::channel();
            self.request_tx
                .send(proto::Request::DeleteBlock(proto::RequestDeleteBlock {
                    block_id: block_id.clone(),
                    reply_tx,
                }))
                .await
                .map_err(|_send_error| ero::NoProcError)?;

            match reply_rx.await {
                Ok(proto::Deleted) =>
                    return Ok(proto::Deleted),
                Err(oneshot::Canceled) =>
                    (),
            }
        }
    }
}
