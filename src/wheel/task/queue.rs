use std::{
    mem,
    collections::{
        BinaryHeap,
    },
};

use super::{
    Task,
    TaskKind,
    ReadBlock,
    MarkTombstone,
    Context,
    pending,
};

pub struct Queue<C> where C: Context {
    serial: usize,
    queue_left: BinaryHeap<Task<C>>,
    queue_right: BinaryHeap<Task<C>>,
    pending_read: pending::Tasks<ReadBlock<C::ReadBlock>>,
    pending_delete: pending::Tasks<MarkTombstone<C::DeleteBlock>>,
}

impl<C> Queue<C> where C: Context {
    pub fn new() -> Queue<C> {
        Queue {
            serial: 0,
            queue_left: BinaryHeap::new(),
            queue_right: BinaryHeap::new(),
            pending_read: pending::Tasks::new(),
            pending_delete: pending::Tasks::new(),
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

    pub fn pop(&mut self, current_offset: u64) -> Option<(u64, TaskKind<C>)> {
        loop {
            if let Some(task) = self.queue_right.pop() {
                if task.offset >= current_offset {
                    return Some((task.offset, task.task));
                } else {
                    self.queue_left.push(task);
                }
            } else if let Some(Task { offset, task, .. }) = self.queue_left.pop() {
                // rotate
                mem::swap(&mut self.queue_left, &mut self.queue_right);
                return Some((offset, task));
            } else {
                return None;
            }
        }
    }
}
