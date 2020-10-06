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

pub struct Request {
    pub offset: u64,
    pub task: task::Task<Context>,
    pub reply_tx: oneshot::Sender<task::Done<Context>>,
}
