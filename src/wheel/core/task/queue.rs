use std::{
    collections::{
        BTreeMap,
    },
};

use super::{
    block,
    Task,
    TaskKind,
    WriteBlock,
    ReadBlock,
    DeleteBlock,
    Context,
    super::{
        BlockGet,
        TasksHead,
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
                        assert!(block_entry.tasks_head.is_queued);
                        block_entry.tasks_head.is_queued = false;
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
}

pub struct BlockLens<'q, C> where C: Context {
    queue: &'q mut Queue<C>,
    block_id: block::Id,
}

impl<'q, C> BlockLens<'q, C> where C: Context {
    pub fn block_id(&self) -> &block::Id {
        &self.block_id
    }

    pub fn enqueue<'a, B>(self, mut block_get: B) where B: BlockGet {
        if let Some(block_entry) = block_get.by_id(&self.block_id) {
            if !block_entry.tasks_head.is_queued && !block_entry.tasks_head.is_empty() {
                let prev = self.queue.triggers.insert(block_entry.offset, self.block_id.clone());
                assert!(prev.is_none(), "inconsistent scenario: prev value = {:?} for offset = {}", prev, block_entry.offset);
                block_entry.tasks_head.is_queued = true;
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

    pub fn pop_read_task<'a, B>(&mut self, mut block_get: B) -> Option<ReadBlock<C::ReadBlock>> where B: BlockGet {
        let block_entry = block_get.by_id(&self.block_id)?;
        self.queue.tasks.pop_read(&mut block_entry.tasks_head)
    }

    pub fn pop_delete_task<'a, B>(&mut self, mut block_get: B) -> Option<DeleteBlock<C::DeleteBlock>> where B: BlockGet {
        let block_entry = block_get.by_id(&self.block_id)?;
        self.queue.tasks.pop_delete(&mut block_entry.tasks_head)
    }
}
