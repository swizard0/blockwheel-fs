use o1::set::Ref;

use super::{
    block,
    storage,
};

pub mod task;

mod gaps;
mod blocks;
mod defrag;
mod schema;

#[derive(Clone, PartialEq, Debug)]
pub struct BlockEntry {
    pub offset: u64,
    pub header: storage::BlockHeader,
    pub block_bytes: Option<block::Bytes>,
    pub environs: Environs,
    pub tasks_head: TasksHead,
}

#[derive(Clone, PartialEq, Debug)]
pub struct BlockInfo<'a> {
    pub block_id: block::Id,
    pub block_entry: &'a BlockEntry,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct Environs {
    pub left: LeftEnvirons,
    pub right: RightEnvirons,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub enum LeftEnvirons {
    Start,
    Space { space_key: SpaceKey, },
    Block { block_id: block::Id, },
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub enum RightEnvirons {
    End,
    Space { space_key: SpaceKey, },
    Block { block_id: block::Id, },
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct SpaceKey {
    space_available: usize,
    serial: usize,
}

impl SpaceKey {
    pub fn space_available(&self) -> usize {
        self.space_available
    }
}

#[derive(Clone, PartialEq, Eq, Hash, Default, Debug)]
pub struct TasksHead {
    head_write: Option<Ref>,
    head_read: Option<Ref>,
    head_delete: Option<Ref>,
}
