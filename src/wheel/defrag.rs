use std::{
    cmp,
    collections::{
        VecDeque,
        BinaryHeap,
    },
};

use super::{
    gaps,
    proto,
};

#[derive(Debug)]
pub struct PendingQueue<C> {
    queue: VecDeque<proto::RequestWriteBlock<C>>,
}

impl<C> PendingQueue<C> {
    pub fn new() -> PendingQueue<C> {
        PendingQueue {
            queue: VecDeque::new(),
        }
    }

    pub fn push(&mut self, request_write_block: proto::RequestWriteBlock<C>) {
        self.queue.push_back(request_write_block);
    }
}

#[derive(Debug)]
pub struct TaskQueue {
    queue: BinaryHeap<DefragTask>,
}

impl TaskQueue {
    pub fn new() -> TaskQueue {
        TaskQueue {
            queue: BinaryHeap::new(),
        }
    }

    pub fn push(&mut self, offset: u64, space_key: gaps::SpaceKey) {
        self.queue.push(DefragTask { offset, space_key, });
    }

    pub fn pop(&mut self) -> Option<(u64, gaps::SpaceKey)> {
        self.queue.pop()
            .map(|defrag_task| (defrag_task.offset, defrag_task.space_key))
    }
}

#[derive(Clone, Debug)]
struct DefragTask {
    offset: u64,
    space_key: gaps::SpaceKey,
}

impl PartialEq for DefragTask {
    fn eq(&self, other: &DefragTask) -> bool {
        self.offset == other.offset
    }
}

impl Eq for DefragTask { }

impl PartialOrd for DefragTask {
    fn partial_cmp(&self, other: &DefragTask) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for DefragTask {
    fn cmp(&self, other: &DefragTask) -> cmp::Ordering {
        other.offset.cmp(&self.offset)
    }
}
