use std::{
    mem, cmp,
    collections::{
        BinaryHeap,
    },
};

use super::{
    block,
    store,
    TaskKind,
    Context,
};

pub struct Queue<C> where C: Context {
    queue_left: BinaryHeap<Task>,
    queue_right: BinaryHeap<Task>,
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
        task: TaskKind<C>,
        block_id: block::Id,
        tasks_head: &mut store::TasksHead,
    )
    {
        let queue = if offset >= current_offset {
            &mut self.queue_right
        } else {
            &mut self.queue_left
        };

        self.tasks.push(tasks_head, task);
        queue.push(Task { offset, block_id, });
    }

    pub fn pop(&mut self, current_offset: u64) -> Option<(u64, TaskKind<C>)> {
        loop {
            if let Some(task) = self.queue_right.pop() {
                if task.offset >= current_offset {
                    return todo!(); // Some((task.offset, task.task));
                } else {
                    self.queue_left.push(task);
                }
            } else if let Some(Task { offset, block_id, }) = self.queue_left.pop() {
                // rotate
                mem::swap(&mut self.queue_left, &mut self.queue_right);
                return todo!(); // Some((offset, task));
            } else {
                return None;
            }
        }
    }
}

#[derive(Debug)]
struct Task {
    offset: u64,
    block_id: block::Id,
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
