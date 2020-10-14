use super::{
    block,
    storage,
};

pub mod task;

mod gaps;
mod blocks;
mod defrag;

pub mod schema;
pub mod performer;

#[derive(Clone, PartialEq, Debug)]
pub struct BlockEntry {
    pub offset: u64,
    pub header: storage::BlockHeader,
    pub block_bytes: Option<block::Bytes>,
    pub environs: Environs,
    pub tasks_head: task::queue::TasksHead,
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

pub trait BlockGet {
    fn by_id<'s>(&'s mut self, block_id: &block::Id) -> Option<&'s mut BlockEntry>;
}

impl<'a, T> BlockGet for &'a mut T where T: BlockGet {
    fn by_id<'s>(&'s mut self, block_id: &block::Id) -> Option<&'s mut BlockEntry> {
        (**self).by_id(block_id)
    }
}

pub struct BlockEntryGet<'a> {
    block_entry: &'a mut BlockEntry,
}

impl<'a> BlockEntryGet<'a> {
    fn new(block_entry: &'a mut BlockEntry) -> BlockEntryGet<'a> {
        BlockEntryGet { block_entry, }
    }
}

impl<'a> BlockGet for BlockEntryGet<'a> {
    fn by_id<'s>(&'s mut self, block_id: &block::Id) -> Option<&'s mut BlockEntry> {
        assert_eq!(&self.block_entry.header.block_id, block_id);
        Some(self.block_entry)
    }
}
