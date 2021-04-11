use std::{
    collections::{
        BTreeMap,
    },
};

use o1::set::Ref;

use super::{
    block,
    Task,
    TaskKind,
    WriteBlock,
    ReadBlock,
    DeleteBlock,
    Flush,
    Context,
    super::{
        BlockGet,
    },
};

mod store;

pub struct Queue<C> where C: Context {
    triggers: BTreeMap<u64, block::Id>,
    tasks: store::Tasks<C>,
    remove_buf: Vec<u64>,
}

impl<C> Queue<C> where C: Context {
    pub fn new() -> Queue<C> {
        Queue {
            triggers: BTreeMap::new(),
            tasks: store::Tasks::new(),
            remove_buf: Vec::new(),
        }
    }

    pub fn focus_block_id<'q>(&'q mut self, block_id: block::Id) -> BlockLens<'q, C> {
        BlockLens { queue: self, block_id, }
    }

    pub fn next_trigger<'q, 'a, B>(
        &'q mut self,
        mut current_offset: u64,
        mut block_get: B,
    )
        -> Option<(u64, BlockLens<'q, C>)>
    where B: BlockGet
    {
        let mut rewind_performed = false;
        enum Outcome {
            Found { block_id: block::Id, offset: u64, },
            Adjust { block_id: block::Id, old_offset: u64, new_offset: u64, },
            Rewind,
        }

        loop {
            let mut outcome = Outcome::Rewind;
            let mut candidates = self.triggers.range(current_offset ..);
            while let Some((&offset, block_id)) = candidates.next() {
                self.remove_buf.push(offset);
                if let Some(block_entry) = block_get.by_id(block_id) {
                    outcome = if offset == block_entry.offset {
                        assert_eq!(block_entry.tasks_head.queue_state, QueueState::Scheduled);
                        block_entry.tasks_head.queue_state = QueueState::Granted;
                        Outcome::Found { block_id: block_id.clone(), offset, }
                    } else {
                        Outcome::Adjust {
                            block_id: block_id.clone(),
                            old_offset: offset,
                            new_offset: block_entry.offset,
                        }
                    };
                    break;
                }
            }
            for offset in self.remove_buf.drain(..) {
                self.triggers.remove(&offset);
            }
            match outcome {
                Outcome::Found { block_id, offset, } =>
                    return Some((offset, BlockLens { queue: self, block_id, })),
                Outcome::Adjust { block_id, old_offset, new_offset, } => {
                    self.triggers.remove(&old_offset);
                    self.triggers.insert(new_offset, block_id);
                },
                Outcome::Rewind if rewind_performed =>
                    return None,
                Outcome::Rewind => {
                    current_offset = 0;
                    rewind_performed = true;
                },
            }
        }
    }

    pub fn is_empty_tasks(&self) -> bool {
        self.triggers.is_empty() && self.tasks.is_empty_tasks()
    }

    pub fn push_flush(&mut self, task: Flush<C::Flush>) {
        self.tasks.push_flush(task);
    }

    pub fn pop_flush(&mut self) -> Option<Flush<C::Flush>> {
        self.tasks.pop_flush()
    }

    pub fn push_pending_read_context(&mut self, read_block: ReadBlock<C>, pending_read_contexts: &mut PendingReadContextBag) {
        self.tasks.push_pending_read_context(read_block, pending_read_contexts);
    }

    pub fn pop_pending_read_context(&mut self, pending_read_contexts: &mut PendingReadContextBag) -> Option<ReadBlock<C>> {
        self.tasks.pop_pending_read_context(pending_read_contexts)
    }
}

pub struct BlockLens<'q, C> where C: Context {
    queue: &'q mut Queue<C>,
    block_id: block::Id,
}

impl<'q, C> BlockLens<'q, C> where C: Context {
    pub fn block_id(&self) -> &block::Id {
        &self.block_id
    }

    pub fn finish<'a, B>(&mut self, mut block_get: B) where B: BlockGet {
        let block_entry = block_get.by_id(&self.block_id).unwrap();
        assert_eq!(block_entry.tasks_head.queue_state, QueueState::Granted);
        block_entry.tasks_head.queue_state = QueueState::Vacant;
    }

    pub fn enqueue<'a, B>(self, mut block_get: B) where B: BlockGet {
        if let Some(block_entry) = block_get.by_id(&self.block_id) {
            if (block_entry.tasks_head.queue_state == QueueState::Vacant) && !block_entry.tasks_head.is_empty() {
                let prev = self.queue.triggers.insert(block_entry.offset, self.block_id.clone());
                assert!(
                    prev.is_none(),
                    "inconsistent scenario: prev value = {:?} for block_id = {:?} offset = {}",
                    prev,
                    self.block_id,
                    block_entry.offset,
                );
                block_entry.tasks_head.queue_state = QueueState::Scheduled;
            }
        }
    }

    pub fn push_task<'a, B>(&mut self, task: Task<C>, mut block_get: B) where B: BlockGet {
        assert_eq!(task.block_id, self.block_id);
        if let Some(block_entry) = block_get.by_id(&self.block_id) {
            self.queue.tasks.push(&mut block_entry.tasks_head, task);
        }
    }

    pub fn pop_task<'a, B>(&mut self, mut block_get: B) -> Option<TaskKind<C>> where B: BlockGet {
        let block_entry = block_get.by_id(&self.block_id)?;
        self.queue.tasks.pop(&mut block_entry.tasks_head)
    }

    pub fn pop_write_task<'a, B>(&mut self, mut block_get: B) -> Option<WriteBlock<C::WriteBlock>> where B: BlockGet {
        let block_entry = block_get.by_id(&self.block_id)?;
        self.queue.tasks.pop_write(&mut block_entry.tasks_head)
    }

    pub fn pop_read_task<'a, B>(&mut self, mut block_get: B) -> Option<ReadBlock<C>> where B: BlockGet {
        let block_entry = block_get.by_id(&self.block_id)?;
        self.queue.tasks.pop_read(&mut block_entry.tasks_head)
    }

    pub fn pop_read_defrag_task<'a, B>(&mut self, mut block_get: B) -> Option<ReadBlock<C>> where B: BlockGet {
        let block_entry = block_get.by_id(&self.block_id)?;
        self.queue.tasks.pop_read_defrag(&mut block_entry.tasks_head)
    }

    pub fn pop_delete_task<'a, B>(&mut self, mut block_get: B) -> Option<DeleteBlock<C::DeleteBlock>> where B: BlockGet {
        let block_entry = block_get.by_id(&self.block_id)?;
        self.queue.tasks.pop_delete(&mut block_entry.tasks_head)
    }
}

#[derive(Clone, PartialEq, Eq, Hash, Default, Debug)]
pub struct PendingReadContextBag {
    head: Option<Ref>,
}

#[derive(Clone, PartialEq, Eq, Hash, Default, Debug)]
pub struct TasksHead {
    head_write: Option<Ref>,
    head_read: Option<Ref>,
    head_read_defrag: Option<Ref>,
    head_delete: Option<Ref>,
    queue_state: QueueState,
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
enum QueueState {
    Vacant,
    Scheduled,
    Granted,
}

impl Default for QueueState {
    fn default() -> QueueState {
        QueueState::Vacant
    }
}

impl TasksHead {
    pub fn is_empty(&self) -> bool {
        self.head_write.is_none()
            && self.head_read.is_none()
            && self.head_read_defrag.is_none()
            && self.head_delete.is_none()
    }
}
