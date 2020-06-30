use std::{
    cmp,
    collections::{
        BinaryHeap,
    },
};

use futures::{
    channel::{
        oneshot,
    },
};

use super::{
    block,
    proto,
};

pub struct Queue {
    queue: BinaryHeap<Task>,
}

impl Queue {
    pub fn new() -> Queue {
        Queue {
            queue: BinaryHeap::new(),
        }
    }

    pub fn push(&mut self, offset: u64, task: TaskKind) {
        self.queue.push(Task { offset, task, });
    }
}

#[derive(Debug)]
struct Task {
    offset: u64,
    task: TaskKind,
}

impl PartialEq for Task {
    fn eq(&self, other: &Task) -> bool {
        self.offset == other.offset
    }
}

impl Eq for Task { }

impl PartialOrd for Task {
    fn partial_cmp(&self, other: &Task) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Task {
    fn cmp(&self, other: &Task) -> cmp::Ordering {
        other.offset.cmp(&self.offset)
    }
}

#[derive(Debug)]
pub enum TaskKind {
    WriteBlock(WriteBlock),
}

#[derive(Debug)]
pub struct WriteBlock {
    pub block_id: block::Id,
    pub block_bytes: block::Bytes,
    pub reply_tx: oneshot::Sender<Result<block::Id, proto::RequestWriteBlockError>>,
}
