use std::{
    collections::{
        VecDeque,
    },
};

use super::{
    task,
};

#[derive(Debug)]
pub struct PendingDefragQueue {
    queue: VecDeque<task::WriteBlock>,
}

impl PendingDefragQueue {
    pub fn new() -> PendingDefragQueue {
        PendingDefragQueue {
            queue: VecDeque::new(),
        }
    }

    pub fn push(&mut self, task_write_block: task::WriteBlock) {
        self.queue.push_back(task_write_block);
    }
}
