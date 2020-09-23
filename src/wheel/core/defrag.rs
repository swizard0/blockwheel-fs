use std::{
    cmp,
    collections::{
        BTreeMap,
        BinaryHeap,
    },
};

use super::{
    super::proto,
    SpaceKey,
};

#[derive(Debug)]
pub struct Queues<C> {
    pub pending: PendingQueue<C>,
    pub tasks: TaskQueue,
}

impl<C> Queues<C> {
    pub fn new() -> Queues<C> {
        Queues {
            pending: PendingQueue::new(),
            tasks: TaskQueue::new(),
        }
    }
}

#[derive(Debug)]
pub struct PendingQueue<C> {
    queue: BTreeMap<usize, proto::RequestWriteBlock<C>>,
    bytes: usize,
}

impl<C> PendingQueue<C> {
    pub fn new() -> PendingQueue<C> {
        PendingQueue {
            queue: BTreeMap::new(),
            bytes: 0,
        }
    }

    pub fn push(&mut self, request_write_block: proto::RequestWriteBlock<C>) {
        self.bytes += request_write_block.block_bytes.len();
        self.queue.insert(
            request_write_block.block_bytes.len(),
            request_write_block,
        );
    }

    pub fn pending_bytes(&self) -> usize {
        self.bytes
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

    pub fn push(&mut self, offset: u64, space_key: SpaceKey) {
        self.queue.push(DefragTask { offset, space_key, });
    }

    pub fn pop(&mut self) -> Option<(u64, SpaceKey)> {
        self.queue.pop()
            .map(|defrag_task| (defrag_task.offset, defrag_task.space_key))
    }
}

#[derive(Clone, Debug)]
struct DefragTask {
    offset: u64,
    space_key: SpaceKey,
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
