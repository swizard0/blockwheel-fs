use std::{
    collections::{
        HashMap,
    },
};

use super::{
    block,
    super::storage,
};

#[derive(Debug)]
pub struct Blocks {
    index: HashMap<block::Id, BlockEntry>,
}

#[derive(Clone, PartialEq, Debug)]
pub struct BlockEntry {
    pub offset: u64,
    pub header: storage::BlockHeader,
}

#[derive(Clone, PartialEq, Debug)]
pub struct BlockInfo<'a> {
    pub block_id: block::Id,
    pub block_entry: &'a BlockEntry,
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

    pub fn remove(&mut self, block_id: block::Id) -> Option<BlockEntry> {
        self.index.remove(&block_id)
    }
}
