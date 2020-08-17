use std::{
    mem, cmp,
    collections::{
        BinaryHeap,
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
    super::TasksHead,
};

mod store;

pub struct Queue<C> where C: Context {
    queue_left: BinaryHeap<QueuedTask>,
    queue_right: BinaryHeap<QueuedTask>,
    tasks: store::Tasks<C>,
}

impl<C> Queue<C> where C: Context {
    pub fn new() -> Queue<C> {
        Queue {
            queue_left: BinaryHeap::new(),
            queue_right: BinaryHeap::new(),
            tasks: store::Tasks::new(),
        }
    }

    pub fn push(
        &mut self,
        current_offset: u64,
        offset: u64,
        task: Task<C>,
        tasks_head: &mut TasksHead,
    )
    {
        let block_id = task.block_id.clone();

        let tt = match &task.kind {
            TaskKind::WriteBlock(..) =>
                format!("[write block id = {:?}]", block_id),
            TaskKind::ReadBlock(..) =>
                format!("[read block id = {:?}]", block_id),
            TaskKind::DeleteBlock(..) =>
                format!("[delete block id = {:?}]", block_id),
        };

        match self.tasks.push(tasks_head, task) {
            store::PushStatus::New => {
                println!(" ;; Queue::push NEW @ current_offset = {}, offset = {}, task = {:?}", current_offset, offset, tt);

                let queue = if offset >= current_offset {
                    &mut self.queue_right
                } else {
                    &mut self.queue_left
                };
                queue.push(QueuedTask { offset, block_id, });
            },
            store::PushStatus::Queued =>
                (),
        }
    }

    pub fn pop_block_id(&mut self, current_offset: u64) -> Option<(u64, block::Id)> {
        loop {
            if let Some(queued_task) = self.queue_right.pop() {
                if queued_task.offset >= current_offset {
                    println!(" ;; Queue::pop @ current_offset = {}, offset = {}", current_offset, queued_task.offset);
                    return Some((queued_task.offset, queued_task.block_id));
                } else {
                    self.queue_left.push(queued_task);
                }
            } else if let Some(QueuedTask { offset, block_id, }) = self.queue_left.pop() {
                // rotate
                mem::swap(&mut self.queue_left, &mut self.queue_right);
                println!(" ;; Queue::pop @ current_offset = {}, offset = {}", current_offset, offset);
                return Some((offset, block_id));
            } else {
                return None;
            }
        }
    }

    pub fn pop_task(&mut self, tasks_head: &mut TasksHead) -> Option<TaskKind<C>> {
        self.tasks.pop(tasks_head)
    }

    pub fn pop_write_task(&mut self, tasks_head: &mut TasksHead) -> Option<WriteBlock<C::WriteBlock>> {
        self.tasks.pop_write(tasks_head)
    }

    pub fn pop_read_task(&mut self, tasks_head: &mut TasksHead) -> Option<ReadBlock<C::ReadBlock>> {
        self.tasks.pop_read(tasks_head)
    }

    pub fn pop_delete_task(&mut self, tasks_head: &mut TasksHead) -> Option<DeleteBlock<C::DeleteBlock>> {
        self.tasks.pop_delete(tasks_head)
    }
}

#[derive(Debug)]
struct QueuedTask {
    offset: u64,
    block_id: block::Id,
}

impl PartialEq for QueuedTask {
    fn eq(&self, other: &QueuedTask) -> bool {
        self.offset == other.offset
    }
}

impl Eq for QueuedTask { }

impl PartialOrd for QueuedTask {
    fn partial_cmp(&self, other: &QueuedTask) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for QueuedTask {
    fn cmp(&self, other: &QueuedTask) -> cmp::Ordering {
        other.offset.cmp(&self.offset)
    }
}
