use std::{
    collections::{
        HashMap,
    },
};

use super::{
    gaps,
    task,
    block,
    storage,
    primitives::{
        BlockHeader,
        BlockInfo,
    },
};

#[derive(Debug)]
pub struct Blocks {
    index: HashMap<block::Id, BlockEntry>,
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
