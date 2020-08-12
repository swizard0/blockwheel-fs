use std::{
    collections::{
        HashMap,
    },
};

use super::{
    block,
    BlockEntry,
    LeftEnvirons,
    RightEnvirons,
};

#[derive(Debug)]
pub struct Index {
    index: HashMap<block::Id, BlockEntry>,
}

impl Index {
    pub fn new() -> Index {
        Index {
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

    pub fn with_mut<F, T>(&mut self, block_id: &block::Id, action: F) -> Option<T> where F: FnOnce(&mut BlockEntry) -> T {
        if let Some(value) = self.get_mut(block_id) {
            Some(action(value))
        } else {
            None
        }
     }

    pub fn update_env_left(&mut self, block_id: &block::Id, env: LeftEnvirons) {
        self.with_mut(block_id, |block_entry| block_entry.environs.left = env).unwrap()
    }

    pub fn update_env_right(&mut self, block_id: &block::Id, env: RightEnvirons) {
        self.with_mut(block_id, |block_entry| block_entry.environs.right = env).unwrap()
    }

    pub fn remove(&mut self, block_id: &block::Id) -> Option<BlockEntry> {
        self.index.remove(block_id)
    }
}