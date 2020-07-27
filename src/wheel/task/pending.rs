use std::{
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
};

pub struct Tasks<T> {
    index: HashMap<block::Id, Ref>,
    entries: Forest1<T>,
}

impl<T> Tasks<T> {
    pub fn new() -> Tasks<T> {
        Tasks {
            index: HashMap::new(),
            entries: Forest1::new(),
        }
    }

    pub fn push(&mut self, block_id: block::Id, item: T) {
        match self.index.entry(block_id) {
            Entry::Vacant(mut ve) => {
                let head_ref = self.entries.make_root(item);
                ve.insert(head_ref);
            },
            Entry::Occupied(mut oe) => {
                let prev_ref = oe.get().clone();
                let item_ref = self.entries.make_node(prev_ref, item);
                *oe.get_mut() = item_ref;
            },
        }
    }

    pub fn pop(&mut self, block_id: block::Id) -> Option<T> {
        match self.index.entry(block_id) {
            Entry::Vacant(..) =>
                None,
            Entry::Occupied(mut oe) => {
                let node_ref = oe.get().clone();
                let node = self.entries.remove(node_ref).unwrap();
                if let Some(prev_ref) = node.parent {
                    *oe.get_mut() = prev_ref;
                } else {
                    oe.remove();
                }
                Some(node.item)
            },

        }
    }
}
