use std::{
    cmp,
    collections::{
        HashSet,
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
    block,
    BlockGet,
    DefragGaps,
    super::proto,
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

    pub fn push(&mut self, request_write_block: proto::RequestWriteBlock<C>, block_bytes_len: usize) {
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
        let mut candidates = self.queue.range(..= bytes_available).rev();
        let (&block_bytes_len, &node_ref) = candidates.next()?;
        let node = self.requests.remove(node_ref).unwrap();
        assert!(block_bytes_len >= node.item.block_bytes.len());
        assert!(self.bytes >= block_bytes_len);
        match node.parent {
            None => {
                self.queue.remove(&block_bytes_len);
            },
            Some(parent_ref) => {
                *self.queue.get_mut(&block_bytes_len).unwrap() = parent_ref;
            },
        }
        self.bytes -= block_bytes_len;
        Some(node.item)
    }

    pub fn pending_bytes(&self) -> usize {
        self.bytes
    }
}

#[derive(Debug)]
pub struct TaskQueue {
    queue: BinaryHeap<DefragTask>,
    postpone: Vec<DefragTask>,
}

impl TaskQueue {
    pub fn new() -> TaskQueue {
        TaskQueue {
            queue: BinaryHeap::new(),
            postpone: Vec::new(),
        }
    }

    pub fn push(&mut self, defrag_gaps: DefragGaps, moving_block_id: block::Id) {
        self.queue.push(DefragTask { defrag_gaps, moving_block_id, });
    }

    pub fn pop<B>(&mut self, pending_write: &HashSet<block::Id>, mut block_get: B) -> Option<(DefragGaps, block::Id)> where B: BlockGet {
        let mut output = None;
        loop {
            if let Some(defrag_task) = self.queue.pop() {
                if defrag_task.defrag_gaps.is_still_relevant(&defrag_task.moving_block_id, &mut block_get) {
                    if !pending_write.contains(&defrag_task.moving_block_id) {
                        output = Some((defrag_task.defrag_gaps, defrag_task.moving_block_id));
                    } else {
                        self.postpone.push(defrag_task);
                        continue;
                    }
                }
            }
            break;
        }
        self.queue.extend(self.postpone.drain(..));
        output
    }
}

#[derive(Clone, Debug)]
struct DefragTask {
    defrag_gaps: DefragGaps,
    moving_block_id: block::Id,
}

impl PartialEq for DefragTask {
    fn eq(&self, other: &DefragTask) -> bool {
        self.defrag_gaps == other.defrag_gaps
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
        match (&self.defrag_gaps, &other.defrag_gaps) {
            (DefragGaps::OnlyLeft { space_key_left: space_self, }, DefragGaps::OnlyLeft { space_key_left: space_other, }) =>
                space_self.cmp(&space_other),
            (DefragGaps::OnlyLeft { .. }, DefragGaps::Both { .. }) =>
                cmp::Ordering::Less,
            (DefragGaps::Both { .. }, DefragGaps::OnlyLeft { .. }) =>
                cmp::Ordering::Greater,
            (
                DefragGaps::Both { space_key_left: space_left_self, space_key_right: space_right_self },
                DefragGaps::Both { space_key_left: space_left_other, space_key_right: space_right_other, },
            ) => {
                let space_sum_self = space_left_self.space_available() + space_right_self.space_available();
                let space_sum_other = space_left_other.space_available() + space_right_other.space_available();
                space_sum_self.cmp(&space_sum_other)
            },
        }
    }
}
