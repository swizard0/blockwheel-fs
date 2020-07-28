use std::{
    mem,
    collections::{
        HashMap,
        hash_map::Entry,
    },
};

use o1::{
    set::Ref,
    forest::Forest1,
};

use super::{
    block,
    TaskKind,
    WriteBlock,
    ReadBlock,
    DeleteBlock,
    Context,
};

pub struct Tasks<C> where C: Context {
    tasks_write: Forest1<WriteBlock<C::WriteBlock>>,
    tasks_read: Forest1<ReadBlock<C::ReadBlock>>,
    tasks_delete: Forest1<DeleteBlock<C::DeleteBlock>>,
}

pub struct TasksHead {
    head_write: Ref,
    head_read: Ref,
    head_delete: Ref,
}

impl<C> Tasks<C> where C: Context {
    pub fn new() -> Tasks<C> {
        Tasks {
            tasks_write: Forest1::new(),
            tasks_read: Forest1::new(),
            tasks_delete: Forest1::new(),
        }
    }

    pub fn push(&mut self, tasks_head: &mut TasksHead, task: TaskKind<C>) {
        match task {
            TaskKind::WriteBlock(write_block) =>
                todo!(),
            TaskKind::ReadBlock(read_block) =>
                todo!(),
            TaskKind::DeleteBlock(delete_block) =>
                todo!(),
        }
    }
    //     match self.index.entry(block_id) {
    //         Entry::Vacant(mut ve) => {
    //             let head_ref = self.entries.make_root(item);
    //             ve.insert(head_ref);
    //         },
    //         Entry::Occupied(mut oe) => {
    //             let prev_ref = oe.get().clone();
    //             let item_ref = self.entries.make_node(prev_ref, item);
    //             *oe.get_mut() = item_ref;
    //         },
    //     }
    // }

    // pub fn pop(&mut self, block_id: block::Id) -> Option<T> {
    //     match self.index.entry(block_id) {
    //         Entry::Vacant(..) =>
    //             None,
    //         Entry::Occupied(mut oe) => {
    //             let node_ref = oe.get().clone();
    //             let node = self.entries.remove(node_ref).unwrap();
    //             if let Some(prev_ref) = node.parent {
    //                 *oe.get_mut() = prev_ref;
    //             } else {
    //                 oe.remove();
    //             }
    //             Some(node.item)
    //         },

    //     }
    // }
}
