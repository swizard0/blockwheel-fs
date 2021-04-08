use o1::{
    set::Set,
    forest::Forest1,
};

use crate::wheel::core::{
    task::{
        queue::{
            TasksHead,
        },
        Task,
        TaskKind,
        WriteBlock,
        ReadBlock,
        DeleteBlock,
        Flush,
        Context,
        ReadBlockContext,
    },
};

pub struct Tasks<C> where C: Context {
    tasks_write: Set<WriteBlock<C::WriteBlock>>,
    tasks_read: Forest1<ReadBlock<C>>,
    tasks_read_defrag: Forest1<ReadBlock<C>>,
    tasks_delete: Forest1<DeleteBlock<C::DeleteBlock>>,
    tasks_flush: Vec<Flush<C::Flush>>,
}

impl<C> Tasks<C> where C: Context {
    pub fn new() -> Tasks<C> {
        Tasks {
            tasks_write: Set::new(),
            tasks_read: Forest1::new(),
            tasks_read_defrag: Forest1::new(),
            tasks_delete: Forest1::new(),
            tasks_flush: Vec::new(),
        }
    }

    pub fn push(&mut self, tasks_head: &mut TasksHead, task: Task<C>) {
        match task.kind {
            TaskKind::WriteBlock(write_block) => {
                assert!(tasks_head.head_write.is_none());
                let node_ref = self.tasks_write.insert(write_block);
                tasks_head.head_write = Some(node_ref);
            },
            TaskKind::ReadBlock(read_block @ ReadBlock { context: ReadBlockContext::Process(..), }) =>
                if let Some(prev_ref) = tasks_head.head_read.take() {
                    let node_ref = self.tasks_read.make_node(prev_ref, read_block);
                    tasks_head.head_read = Some(node_ref);
                } else {
                    let node_ref = self.tasks_read.make_root(read_block);
                    tasks_head.head_read = Some(node_ref);
                },
            TaskKind::ReadBlock(read_block @ ReadBlock { context: ReadBlockContext::Defrag(..), }) =>
                if let Some(prev_ref) = tasks_head.head_read_defrag.take() {
                    let node_ref = self.tasks_read_defrag.make_node(prev_ref, read_block);
                    tasks_head.head_read_defrag = Some(node_ref);
                } else {
                    let node_ref = self.tasks_read_defrag.make_root(read_block);
                    tasks_head.head_read_defrag = Some(node_ref);
                },
            TaskKind::DeleteBlock(delete_block) =>
                if let Some(prev_ref) = tasks_head.head_delete.take() {
                    let node_ref = self.tasks_delete.make_node(prev_ref, delete_block);
                    tasks_head.head_delete = Some(node_ref);
                } else {
                    let node_ref = self.tasks_delete.make_root(delete_block);
                    tasks_head.head_delete = Some(node_ref);
                },
        }
    }

    pub fn is_empty_tasks(&self) -> bool {
        self.tasks_write.is_empty()
            && self.tasks_read.is_empty()
            && self.tasks_read_defrag.is_empty()
            && self.tasks_delete.is_empty()
    }

    pub fn push_flush(&mut self, task: Flush<C::Flush>) {
        self.tasks_flush.push(task);
    }

    pub fn pop(&mut self, tasks_head: &mut TasksHead) -> Option<TaskKind<C>> {
        if let Some(task) = self.pop_write(tasks_head) {
            return Some(TaskKind::WriteBlock(task));
        }

        if let Some(task) = self.pop_read(tasks_head) {
            return Some(TaskKind::ReadBlock(task));
        }

        if let Some(task) = self.pop_read_defrag(tasks_head) {
            return Some(TaskKind::ReadBlock(task));
        }

        if let Some(task) = self.pop_delete(tasks_head) {
            return Some(TaskKind::DeleteBlock(task));
        }

        None
    }

    pub fn pop_write(&mut self, tasks_head: &mut TasksHead) -> Option<WriteBlock<C::WriteBlock>> {
        if let Some(node_ref) = tasks_head.head_write.take() {
            Some(self.tasks_write.remove(node_ref).unwrap())
        } else {
            None
        }
    }

    pub fn pop_read(&mut self, tasks_head: &mut TasksHead) -> Option<ReadBlock<C>> {
        if let Some(node_ref) = tasks_head.head_read.take() {
            let node = self.tasks_read.remove(node_ref).unwrap();
            tasks_head.head_read = node.parent;
            Some(node.item)
        } else {
            None
        }
    }

    pub fn pop_read_defrag(&mut self, tasks_head: &mut TasksHead) -> Option<ReadBlock<C>> {
        if let Some(node_ref) = tasks_head.head_read_defrag.take() {
            let node = self.tasks_read_defrag.remove(node_ref).unwrap();
            tasks_head.head_read_defrag = node.parent;
            Some(node.item)
        } else {
            None
        }
    }

    pub fn pop_delete(&mut self, tasks_head: &mut TasksHead) -> Option<DeleteBlock<C::DeleteBlock>> {
        if let Some(node_ref) = tasks_head.head_delete.take() {
            let node = self.tasks_delete.remove(node_ref).unwrap();
            tasks_head.head_delete = node.parent;
            Some(node.item)
        } else {
            None
        }
    }

    pub fn pop_flush(&mut self) -> Option<Flush<C::Flush>> {
        self.tasks_flush.pop()
    }
}
