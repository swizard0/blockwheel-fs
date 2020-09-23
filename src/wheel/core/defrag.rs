use std::{
    cmp,
    collections::{
        BTreeMap,
        BinaryHeap,
        btree_map,
    },
};

use o1::{
    set::Ref,
    forest::Forest1,
};

use super::{
    super::proto,
    SpaceKey,
};

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

pub struct PendingQueue<C> {
    queue: BTreeMap<usize, Ref>,
    requests: Forest1<proto::RequestWriteBlock<C>>,
    bytes: usize,
}

impl<C> PendingQueue<C> {
    pub fn new() -> PendingQueue<C> {
        PendingQueue {
            queue: BTreeMap::new(),
            requests: Forest1::new(),
            bytes: 0,
        }
    }

    pub fn push(&mut self, request_write_block: proto::RequestWriteBlock<C>) {
        let block_bytes_len = request_write_block.block_bytes.len();
        self.bytes += block_bytes_len;
        match self.queue.entry(block_bytes_len) {
            btree_map::Entry::Vacant(ve) => {
                let node_ref = self.requests.make_root(request_write_block);
                ve.insert(node_ref);
            },
            btree_map::Entry::Occupied(mut oe) => {
                let value = oe.get_mut();
                let node_ref = self.requests.make_node(*value, request_write_block);
                *value = node_ref;
            },
        }
    }

    pub fn pop_at_most(&mut self, bytes_available: usize) -> Option<proto::RequestWriteBlock<C>> {

        unimplemented!()
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
