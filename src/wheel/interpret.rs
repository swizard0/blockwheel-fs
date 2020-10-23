use futures::{
    channel::{
        oneshot,
    },
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
