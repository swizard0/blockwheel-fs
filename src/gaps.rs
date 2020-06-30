use std::{
    collections::{
        BTreeMap,
    },
};

use super::{
    block,
    index,
};

#[derive(Debug)]
pub struct Index {
    serial: usize,
    gaps: BTreeMap<SpaceKey, GapBetween<block::Id>>,
    space_total: usize,
    remove_buf: Vec<SpaceKey>,
}

#[derive(Debug)]
pub enum GapBetween<B> {
    StartAndBlock {
        right_block: B,
    },
    TwoBlocks {
        left_block: B,
        right_block: B,
    },
    BlockAndEnd {
        left_block: B,
    },
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
struct SpaceKey {
    space_available: usize,
    serial: usize,
}

pub enum Allocated<'a> {
    Success {
        space_available: usize,
        between: GapBetween<index::BlockInfo<'a>>,
    },
    PendingDefragmentation,
}

#[derive(Debug)]
pub enum Error {
    NoSpaceLeft,
}

impl Index {
    pub fn new() -> Index {
        Index {
            gaps: BTreeMap::new(),
            serial: 0,
            space_total: 0,
            remove_buf: Vec::new(),
        }
    }

    pub fn insert(&mut self, space_available: usize, between: GapBetween<block::Id>) {
        self.serial += 1;
        self.gaps.insert(SpaceKey { space_available, serial: self.serial, }, between);
        self.space_total += space_available;
    }

    pub fn allocate<'a>(
        &mut self,
        space_required: usize,
        blocks_index: &'a index::Blocks,
    )
        -> Result<Allocated<'a>, Error>
    {
        let mut maybe_result = None;
        let mut candidates = self.gaps.range(SpaceKey { space_available: space_required, serial: 0, } ..);
        loop {
            match candidates.next() {
                None =>
                    break,
                Some((key, between)) => {
                    self.remove_buf.push(*key);
                    match between {
                        GapBetween::StartAndBlock { right_block, } =>
                            if let Some(block_entry) = blocks_index.get(right_block) {
                                maybe_result = Some(Allocated::Success {
                                    space_available: key.space_available,
                                    between: GapBetween::StartAndBlock {
                                        right_block: index::BlockInfo {
                                            block_id: right_block.clone(),
                                            block_entry,
                                        },
                                    },
                                });
                                break;
                            },
                        GapBetween::TwoBlocks { left_block, right_block, } => {
                            let maybe_left = blocks_index.get(left_block);
                            let maybe_right = blocks_index.get(right_block);
                            if let (Some(left_block_entry), Some(right_block_entry)) = (maybe_left, maybe_right) {
                                maybe_result = Some(Allocated::Success {
                                    space_available: key.space_available,
                                    between: GapBetween::TwoBlocks {
                                        left_block: index::BlockInfo {
                                            block_id: left_block.clone(),
                                            block_entry: left_block_entry,
                                        },
                                        right_block: index::BlockInfo {
                                            block_id: right_block.clone(),
                                            block_entry: right_block_entry,
                                        },
                                    },
                                });
                                break;
                            }
                        },
                        GapBetween::BlockAndEnd { left_block, } =>
                            if let Some(block_entry) = blocks_index.get(left_block) {
                                maybe_result = Some(Allocated::Success {
                                    space_available: key.space_available,
                                    between: GapBetween::BlockAndEnd {
                                        left_block: index::BlockInfo {
                                            block_id: left_block.clone(),
                                            block_entry,
                                        },
                                    },
                                });
                                break;
                            },
                    }
                },
            }
        }

        for key in self.remove_buf.drain(..) {
            self.gaps.remove(&key);
        }

        if let Some(result) = maybe_result {
            Ok(result)
        } else if space_required <= self.space_total {
            Ok(Allocated::PendingDefragmentation)
        } else {
            Err(Error::NoSpaceLeft)
        }
    }
}
