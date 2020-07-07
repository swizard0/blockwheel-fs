use std::{
    collections::{
        HashMap,
    },
};

use super::{
    gaps,
    block,
    storage,
};

#[derive(Debug)]
pub struct Blocks {
    index: HashMap<block::Id, BlockEntry>,
}

#[derive(Clone, PartialEq, Debug)]
pub struct BlockEntry {
    pub offset: u64,
    pub header: storage::BlockHeader,
    pub environs: Environs,
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
    Space { space_key: gaps::SpaceKey, },
    Block { block_id: block::Id, },
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub enum RightEnvirons {
    End,
    Space { space_key: gaps::SpaceKey, },
    Block { block_id: block::Id, },
}

impl Blocks {
    pub fn new() -> Blocks {
        Blocks {
            index: HashMap::new(),
        }
    }

    pub fn insert(&mut self, block_id: block::Id, block_entry: BlockEntry) {
        self.index.insert(block_id, block_entry);
    }

    pub fn get(&self, block_id: &block::Id) -> Option<&BlockEntry> {
        self.index.get(block_id)
    }

    pub fn get_mut(&mut self, block_id: &block::Id) -> Option<&mut BlockEntry> {
        self.index.get_mut(block_id)
    }

    pub fn update_env_left(&mut self, block_id: &block::Id, env: LeftEnvirons) {
        let block_entry = self.get_mut(block_id).unwrap();
        block_entry.environs.left = env;
    }

    pub fn update_env_right(&mut self, block_id: &block::Id, env: RightEnvirons) {
        let block_entry = self.get_mut(block_id).unwrap();
        block_entry.environs.right = env;
    }

    pub fn remove(&mut self, block_id: &block::Id) -> Option<BlockEntry> {
        self.index.remove(block_id)
    }
}
