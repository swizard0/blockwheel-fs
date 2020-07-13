use std::{
    cmp, mem,
    collections::{
        BinaryHeap,
    },
};

use super::{
    block,
    storage,
    context::Context,
};

pub struct Queue<C> where C: Context {
    serial: usize,
    queue_left: BinaryHeap<Task<C>>,
    queue_right: BinaryHeap<Task<C>>,
}

impl<C> Queue<C> where C: Context {
    pub fn new() -> Queue<C> {
        Queue {
            serial: 0,
            queue_left: BinaryHeap::new(),
            queue_right: BinaryHeap::new(),
        }
    }

    pub fn push(&mut self, current_offset: u64, offset: u64, task: TaskKind<C>) {
        let queue = if offset >= current_offset {
            &mut self.queue_right
        } else {
            &mut self.queue_left
        };
        let serial = self.serial;
        self.serial += 1;
        queue.push(Task { offset, serial, task, });
    }

    pub fn pop(&mut self) -> Option<(u64, TaskKind<C>)> {
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
struct Task<C> where C: Context {
    offset: u64,
    serial: usize,
    task: TaskKind<C>,
}

impl<C> PartialEq for Task<C> where C: Context {
    fn eq(&self, other: &Task<C>) -> bool {
        self.offset == other.offset && self.serial == other.serial
    }
}

impl<C> Eq for Task<C> where C: Context { }

impl<C> PartialOrd for Task<C> where C: Context {
    fn partial_cmp(&self, other: &Task<C>) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<C> Ord for Task<C> where C: Context {
    fn cmp(&self, other: &Task<C>) -> cmp::Ordering {
        other.offset.cmp(&self.offset)
            .then_with(|| self.serial.cmp(&other.serial))
    }
}

#[derive(Debug)]
pub enum TaskKind<C> where C: Context {
    WriteBlock(WriteBlock<C::WriteBlock>),
    ReadBlock(ReadBlock<C::ReadBlock>),
    MarkTombstone(MarkTombstone<C::DeleteBlock>),
}

#[derive(Debug)]
pub struct WriteBlock<C> {
    pub block_id: block::Id,
    pub block_bytes: block::Bytes,
    pub commit_type: CommitType,
    pub context: WriteBlockContext<C>,
}

#[derive(Debug)]
pub enum CommitType {
    CommitOnly,
    CommitAndEof,
}

#[derive(Debug)]
pub enum WriteBlockContext<C> {
    External(C),
}

#[derive(Debug)]
pub struct ReadBlock<C> {
    pub block_header: storage::BlockHeader,
    pub block_bytes: block::BytesMut,
    pub context: ReadBlockContext<C>,
}

#[derive(Debug)]
pub enum ReadBlockContext<C> {
    External(C),
}

#[derive(Debug)]
pub struct MarkTombstone<C> {
    pub block_id: block::Id,
    pub context: MarkTombstoneContext<C>,
}

#[derive(Debug)]
pub enum MarkTombstoneContext<C> {
    External(C),
}

#[derive(Debug)]
pub struct Done<C> where C: Context {
    pub current_offset: u64,
    pub task: TaskDone<C>,
}

#[derive(Debug)]
pub enum TaskDone<C> where C: Context {
    WriteBlock(TaskDoneWriteBlock<C::WriteBlock>),
    ReadBlock(TaskDoneReadBlock<C::ReadBlock>),
    MarkTombstone(TaskDoneMarkTombstone<C::DeleteBlock>),
}

#[derive(Debug)]
pub struct TaskDoneWriteBlock<C> {
    pub block_id: block::Id,
    pub context: WriteBlockContext<C>,
}

#[derive(Debug)]
pub struct TaskDoneReadBlock<C> {
    pub block_id: block::Id,
    pub block_bytes: block::BytesMut,
    pub context: ReadBlockContext<C>,
}

#[derive(Debug)]
pub struct TaskDoneMarkTombstone<C> {
    pub block_id: block::Id,
    pub context: MarkTombstoneContext<C>,
}
