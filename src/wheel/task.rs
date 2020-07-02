use std::{
    cmp, mem,
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
    storage,
};

pub struct Queue {
    serial: usize,
    queue_left: BinaryHeap<Task>,
    queue_right: BinaryHeap<Task>,
}

impl Queue {
    pub fn new() -> Queue {
        Queue {
            serial: 0,
            queue_left: BinaryHeap::new(),
            queue_right: BinaryHeap::new(),
        }
    }

    pub fn push(&mut self, offset: u64, task: TaskKind) {
        let queue = match (self.queue_left.peek(), self.queue_right.peek()) {
            (None, None) =>
                &mut self.queue_right,
            (_, Some(right_task)) if offset >= right_task.offset =>
                &mut self.queue_right,
            (_, Some(..)) =>
                &mut self.queue_left,
            (Some(..), None) => {
                // rotate
                mem::swap(&mut self.queue_left, &mut self.queue_right);
                &mut self.queue_right
            },
        };
        let serial = self.serial;
        self.serial += 1;
        queue.push(Task { offset, serial, task, });
    }

    pub fn pop(&mut self) -> Option<(u64, TaskKind)> {
        if let Some(Task { offset, task, .. }) = self.queue_right.pop() {
            Some((offset, task))
        } else if let Some(Task { offset, task, .. }) = self.queue_left.pop() {
            // rotate
            mem::swap(&mut self.queue_left, &mut self.queue_right);
            Some((offset, task))
        } else {
            None
        }
    }
}

#[derive(Debug)]
struct Task {
    offset: u64,
    serial: usize,
    task: TaskKind,
}

impl PartialEq for Task {
    fn eq(&self, other: &Task) -> bool {
        self.offset == other.offset && self.serial == other.serial
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
            .then_with(|| self.serial.cmp(&other.serial))
    }
}

#[derive(Debug)]
pub enum TaskKind {
    WriteBlock(WriteBlock),
    ReadBlock(ReadBlock),
}

#[derive(Debug)]
pub struct WriteBlock {
    pub block_id: block::Id,
    pub block_bytes: block::Bytes,
    pub reply_tx: oneshot::Sender<Result<block::Id, proto::RequestWriteBlockError>>,
    pub commit_type: CommitType,
}

#[derive(Debug)]
pub enum CommitType {
    CommitOnly,
    CommitAndEof,
}

#[derive(Debug)]
pub struct ReadBlock {
    pub block_header: storage::BlockHeader,
    pub block_bytes: block::BytesMut,
    pub reply_tx: oneshot::Sender<Result<block::Bytes, proto::RequestReadBlockError>>,
}

#[derive(Debug)]
pub enum TaskDone {
    WriteBlock(TaskDoneWriteBlock),
    ReadBlock(TaskDoneReadBlock),
}

#[derive(Debug)]
pub struct TaskDoneWriteBlock {
    pub block_id: block::Id,
    pub reply_tx: oneshot::Sender<Result<block::Id, proto::RequestWriteBlockError>>,
}

#[derive(Debug)]
pub struct TaskDoneReadBlock {
    pub block_id: block::Id,
    pub block_bytes: block::BytesMut,
    pub reply_tx: oneshot::Sender<Result<block::Bytes, proto::RequestReadBlockError>>,
}
