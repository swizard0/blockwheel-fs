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

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum DefragGaps {
    OnlyLeft { space_key_left: SpaceKey, },
    Both { space_key_left: SpaceKey, space_key_right: SpaceKey, },
}

impl DefragGaps {
    pub fn is_still_relevant<B>(&self, block_id: &block::Id, mut block_get: B) -> bool where B: BlockGet {
        if let Some(block_entry) = block_get.by_id(block_id) {
            match self {
                DefragGaps::OnlyLeft { space_key_left, } =>
                    match &block_entry.environs.left {
                        LeftEnvirons::Space { space_key, } if space_key == space_key_left =>
                            return true,
                        _ =>
                            (),
                    },
                DefragGaps::Both { space_key_left, space_key_right, } =>
                    match &block_entry.environs {
                        Environs {
                            left: LeftEnvirons::Space { space_key: env_space_key_left, },
                            right: RightEnvirons::Space { space_key: env_space_key_right, },
                        } if env_space_key_left == space_key_left && env_space_key_right == space_key_right =>
                            return true,
                        _ =>
                            (),
                    },
            }
        }
        false
    }
}

pub trait BlockGet {
    fn by_id<'s>(&'s mut self, block_id: &block::Id) -> Option<&'s mut BlockEntry>;

    fn with_mut<F, T>(&mut self, block_id: &block::Id, action: F) -> Option<T> where F: FnOnce(&mut BlockEntry) -> T {
        if let Some(value) = self.by_id(block_id) {
            Some(action(value))
        } else {
            None
        }
     }
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
