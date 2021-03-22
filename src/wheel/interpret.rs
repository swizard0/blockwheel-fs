use futures::{
    channel::{
        mpsc,
        oneshot,
    },
    SinkExt,
};

use crate::{
    InterpretStats,
    context::Context,
    wheel::core::task,
};

pub mod fixed_file;

struct Request<C> where C: Context {
    offset: u64,
    task: task::Task<C>,
    reply_tx: oneshot::Sender<DoneTask<C>>,
}

pub struct DoneTask<C> where C: Context {
    pub task_done: task::Done<C>,
    pub stats: InterpretStats,
}

pub type RequestTask<C> = task::Task<C>;
pub type RequestReplyRx<C> = oneshot::Receiver<DoneTask<C>>;

pub struct Synced;

enum Command<C> where C: Context {
    Request(Request<C>),
    DeviceSync { reply_tx: oneshot::Sender<Synced>, },
}

#[derive(Debug)]
pub enum CreateError {
    FixedFile(fixed_file::WheelCreateError),
}

#[derive(Debug)]
pub enum OpenError {
    FixedFile(fixed_file::WheelOpenError),
}

#[derive(Debug)]
pub enum RunError {
    FixedFile(fixed_file::Error),
}

#[derive(Clone)]
pub struct Pid<C> where C: Context {
    request_tx: mpsc::Sender<Command<C>>,
}

impl<C> Pid<C> where C: Context {
    pub async fn push_request(&mut self, offset: u64, task: RequestTask<C>) -> Result<RequestReplyRx<C>, ero::NoProcError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.request_tx
            .send(Command::Request(Request { offset, task, reply_tx, }))
            .await
            .map_err(|_send_error| ero::NoProcError)?;
        Ok(reply_rx)
    }

    pub async fn device_sync(&mut self) -> Result<Synced, ero::NoProcError> {
        loop {
            let (reply_tx, reply_rx) = oneshot::channel();
            self.request_tx.send(Command::DeviceSync { reply_tx, }).await
                .map_err(|_send_error| ero::NoProcError)?;
            match reply_rx.await {
                Ok(Synced) =>
                    return Ok(Synced),
                Err(oneshot::Canceled) =>
                    (),
            }
        }
    }
}
