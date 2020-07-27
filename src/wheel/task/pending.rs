use std::{
    collections::{
        HashMap,
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
}
