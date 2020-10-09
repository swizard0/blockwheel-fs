use futures::{
    channel::{
        oneshot,
    },
};

use crate::{
    context::Context,
    wheel::core::task,
};

pub mod fixed_file;

struct Request<C> where C: Context {
    offset: u64,
    task: task::Task<C>,
    reply_tx: oneshot::Sender<task::Done<C>>,
}

pub type RequestTask<C> = task::Task<C>;
pub type RequestReplyRx<C> = oneshot::Receiver<task::Done<C>>;
