use o1::{
    set::{
        Ref,
        Set,
    },
    forest::Forest1,
};

use super::{
    Task,
    TaskKind,
    WriteBlock,
    ReadBlock,
    DeleteBlock,
    Context,
};

pub struct Tasks<C> where C: Context {
    tasks_write: Set<WriteBlock<C::WriteBlock>>,
    tasks_read: Forest1<ReadBlock<C::ReadBlock>>,
    tasks_delete: Forest1<DeleteBlock<C::DeleteBlock>>,
}

#[derive(Clone, PartialEq, Eq, Hash, Default, Debug)]
pub struct TasksHead {
    head_write: Option<Ref>,
    head_read: Option<Ref>,
    head_delete: Option<Ref>,
}

pub enum PushStatus {
    New,
    Queued,
}

impl<C> Tasks<C> where C: Context {
    pub fn new() -> Tasks<C> {
        Tasks {
            tasks_write: Set::new(),
            tasks_read: Forest1::new(),
            tasks_delete: Forest1::new(),
        }
    }

    pub fn push(&mut self, tasks_head: &mut TasksHead, task: Task<C>) -> PushStatus {
        match task.kind {
            TaskKind::WriteBlock(write_block) => {
                assert!(tasks_head.head_write.is_none());
                let node_ref = self.tasks_write.insert(write_block);
                tasks_head.head_write = Some(node_ref);
                PushStatus::New
            },
            TaskKind::ReadBlock(read_block) =>
                if let Some(prev_ref) = tasks_head.head_read.take() {
                    let node_ref = self.tasks_read.make_node(prev_ref, read_block);
                    tasks_head.head_read = Some(node_ref);
                    PushStatus::Queued
                } else {
                    let node_ref = self.tasks_read.make_root(read_block);
                    tasks_head.head_read = Some(node_ref);
                    PushStatus::New
                },
            TaskKind::DeleteBlock(delete_block) =>
                if let Some(prev_ref) = tasks_head.head_delete.take() {
                    let node_ref = self.tasks_delete.make_node(prev_ref, delete_block);
                    tasks_head.head_delete = Some(node_ref);
                    PushStatus::Queued
                } else {
                    let node_ref = self.tasks_delete.make_root(delete_block);
                    tasks_head.head_delete = Some(node_ref);
                    PushStatus::New
                },
        }
    }

    pub fn pop(&mut self, tasks_head: &mut TasksHead) -> Option<TaskKind<C>> {
        if let Some(node_ref) = tasks_head.head_write.take() {
            let task = self.tasks_write.remove(node_ref).unwrap();
            return Some(TaskKind::WriteBlock(task));
        }

        if let Some(node_ref) = tasks_head.head_read.take() {
            let node = self.tasks_read.remove(node_ref).unwrap();
            tasks_head.head_read = node.parent;
            return Some(TaskKind::ReadBlock(node.item));
        }

        if let Some(node_ref) = tasks_head.head_delete.take() {
            let node = self.tasks_delete.remove(node_ref).unwrap();
            tasks_head.head_delete = node.parent;
            return Some(TaskKind::DeleteBlock(node.item));
        }

        None
    }
}
