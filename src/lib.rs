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

#[derive(Clone, Debug)]
pub struct Params {
    pub wheel_filename: PathBuf,
    pub init_wheel_size_bytes: usize,
    pub wheel_task_restart_holdon: usize,
}

pub struct GenServer {
    request_tx: mpsc::Sender<wheel::Request>,
    fused_request_rx: stream::Fuse<mpsc::Receiver<wheel::Request>>,
}

#[derive(Clone)]
pub struct Pid {
    request_tx: mpsc::Sender<wheel::Request>,
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
                    restart_after: Duration::from_secs(params.wheel_task_restart_holdon as u64),
                },
            },
            wheel::State {
                fused_request_rx: self.fused_request_rx,
                params,
            },
            wheel::busyloop,
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
            self.request_tx.send(wheel::Request::LendBlock { reply_tx, }).await
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
        self.request_tx.send(wheel::Request::RepayBlock { block_bytes, }).await
            .map_err(|_send_error| ero::NoProcError)
    }


    pub async fn write_block(&mut self, block_bytes: block::Bytes) -> Result<block::Id, ero::NoProcError> {
        loop {
            let (reply_tx, reply_rx) = oneshot::channel();
            self.request_tx
                .send(wheel::Request::WriteBlock {
                    block_bytes: block_bytes.clone(),
                    reply_tx,
                })
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
                .send(wheel::Request::ReadBlock {
                    block_id: block_id.clone(),
                    reply_tx,
                })
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

    pub async fn delete_block(&mut self, block_id: block::Id) -> Result<Deleted, ero::NoProcError> {
        loop {
            let (reply_tx, reply_rx) = oneshot::channel();
            self.request_tx
                .send(wheel::Request::DeleteBlock {
                    block_id: block_id.clone(),
                    reply_tx,
                })
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

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct Deleted;
