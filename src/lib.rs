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
    pub work_block_size: usize,
}

impl Default for Params {
    fn default() -> Params {
        Params {
            wheel_filename: "wheel".to_string().into(),
            init_wheel_size_bytes: 64 * 1024 * 1024,
            wheel_task_restart_sec: 4,
            work_block_size: 8 * 1024 * 1024,
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

    pub async fn run(self, params: Params) {
        let terminate_result = restart::restartable(
            ero::Params {
                name: format!("ero-blockwheel on {:?}", params.wheel_filename),
                restart_strategy: RestartStrategy::Delay {
                    restart_after: Duration::from_secs(params.wheel_task_restart_sec as u64),
                },
            },
            wheel::State {
                fused_request_rx: self.fused_request_rx,
                params,
            },
            wheel::busyloop_init,
        ).await;
        if let Err(error) = terminate_result {
            log::error!("fatal error: {:?}", error);
        }
    }
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


    pub async fn write_block(&mut self, block_bytes: block::Bytes) -> Result<block::Id, ero::NoProcError> {
        loop {
            let (reply_tx, reply_rx) = oneshot::channel();
            self.request_tx
                .send(proto::Request::WriteBlock(proto::RequestWriteBlock {
                    block_bytes: block_bytes.clone(),
                    reply_tx,
                }))
                .await
                .map_err(|_send_error| ero::NoProcError)?;

            match reply_rx.await {
                Ok(block_id) =>
                    return Ok(block_id),
                Err(oneshot::Canceled) =>
                    (),
            }
        }
    }

    pub async fn read_block(&mut self, block_id: block::Id) -> Result<block::BytesMut, ero::NoProcError> {
        loop {
            let (reply_tx, reply_rx) = oneshot::channel();
            self.request_tx
                .send(proto::Request::ReadBlock(proto::RequestReadBlock {
                    block_id: block_id.clone(),
                    reply_tx,
                }))
                .await
                .map_err(|_send_error| ero::NoProcError)?;

            match reply_rx.await {
                Ok(block_bytes) =>
                    return Ok(block_bytes),
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
                Ok(Deleted) =>
                    return Ok(Deleted),
                Err(oneshot::Canceled) =>
                    (),
            }
        }
    }
}
