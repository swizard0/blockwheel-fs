use futures::{
    channel::{
        oneshot,
    },
};

use crate::{
    blockwheel_context::Context,
    wheel::core::task,
};

pub mod fixed_file;

struct Request {
    offset: u64,
    task: task::Task<Context>,
    reply_tx: oneshot::Sender<task::Done<Context>>,
}

pub type RequestTask = task::Task<Context>;
pub type RequestReplyRx = oneshot::Receiver<task::Done<Context>>;
