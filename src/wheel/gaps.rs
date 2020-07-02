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

#[derive(Clone, PartialEq, Debug)]
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
    StartAndEnd,
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
struct SpaceKey {
    space_available: usize,
    serial: usize,
}

#[derive(Clone, PartialEq, Debug)]
pub enum Allocated<'a> {
    Success {
        space_available: usize,
        between: GapBetween<index::BlockInfo<'a>>,
    },
    PendingDefragmentation,
}

#[derive(Clone, PartialEq, Debug)]
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

    pub fn space_total(&self) -> usize {
        self.space_total
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
                        GapBetween::StartAndEnd => {
                            maybe_result = Some(Allocated::Success {
                                space_available: key.space_available,
                                between: GapBetween::StartAndEnd,
                            });
                            assert!(key.space_available <= self.space_total);
                            self.space_total -= key.space_available;
                            break;
                        },
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
                                assert!(key.space_available <= self.space_total);
                                self.space_total -= key.space_available;
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
                                assert!(key.space_available <= self.space_total);
                                self.space_total -= key.space_available;
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
                                assert!(key.space_available <= self.space_total);
                                self.space_total -= key.space_available;
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

#[cfg(test)]
mod tests {
    use super::{
        super::{
            super::{
                block,
                storage,
            },
        },
        index,
        Index,
        Error,
        Allocated,
        GapBetween,
    };

    #[test]
    fn allocated_success_between_two_blocks() {
        let Init { block_a_id, block_b_id, blocks_index, mut gaps, } = Init::new();

        assert_eq!(
            gaps.allocate(3, &blocks_index),
            Ok(Allocated::Success {
                space_available: 4,
                between: GapBetween::TwoBlocks {
                    left_block: index::BlockInfo {
                        block_id: block_a_id.clone(),
                        block_entry: &index::BlockEntry {
                            offset: 0,
                            header: storage::BlockHeader::Regular(storage::BlockHeaderRegular {
                                block_id: block_a_id.clone(),
                                block_size: 4,
                            }),
                        },
                    },
                    right_block: index::BlockInfo {
                        block_id: block_b_id.clone(),
                        block_entry: &index::BlockEntry {
                            offset: 8,
                            header: storage::BlockHeader::EndOfFile,
                        },
                    },
                },
            }),
        );
    }

    #[test]
    fn allocated_success_between_block_and_end() {
        let Init { block_b_id, blocks_index, mut gaps, .. } = Init::new();

        assert_eq!(
            gaps.allocate(33, &blocks_index),
            Ok(Allocated::Success {
                space_available: 60,
                between: GapBetween::BlockAndEnd {
                    left_block: index::BlockInfo {
                        block_id: block_b_id.clone(),
                        block_entry: &index::BlockEntry {
                            offset: 8,
                            header: storage::BlockHeader::EndOfFile,
                        },
                    },
                },
            }),
        );
    }

    #[test]
    fn allocated_success_pending_defragmentation() {
        let Init { blocks_index, mut gaps, .. } = Init::new();

        assert_eq!(
            gaps.allocate(61, &blocks_index),
            Ok(Allocated::PendingDefragmentation),
        );
    }

    #[test]
    fn allocated_error_no_space_left() {
        let Init { blocks_index, mut gaps, .. } = Init::new();

        assert_eq!(
            gaps.allocate(65, &blocks_index),
            Err(Error::NoSpaceLeft),
        );
    }

    #[test]
    fn allocate_until_no_space() {
        let Init { block_a_id, block_b_id, blocks_index, mut gaps, } = Init::new();

        assert_eq!(
            gaps.allocate(3, &blocks_index),
            Ok(Allocated::Success {
                space_available: 4,
                between: GapBetween::TwoBlocks {
                    left_block: index::BlockInfo {
                        block_id: block_a_id.clone(),
                        block_entry: &index::BlockEntry {
                            offset: 0,
                            header: storage::BlockHeader::Regular(storage::BlockHeaderRegular {
                                block_id: block_a_id.clone(),
                                block_size: 4,
                            }),
                        },
                    },
                    right_block: index::BlockInfo {
                        block_id: block_b_id.clone(),
                        block_entry: &index::BlockEntry {
                            offset: 8,
                            header: storage::BlockHeader::EndOfFile,
                        },
                    },
                },
            }),
        );
        assert_eq!(
            gaps.allocate(3, &blocks_index),
            Ok(Allocated::Success {
                space_available: 60,
                between: GapBetween::BlockAndEnd {
                    left_block: index::BlockInfo {
                        block_id: block_b_id.clone(),
                        block_entry: &index::BlockEntry {
                            offset: 8,
                            header: storage::BlockHeader::EndOfFile,
                        },
                    },
                },
            }),
        );
        assert_eq!(
            gaps.allocate(3, &blocks_index),
            Err(Error::NoSpaceLeft),
        );
    }

    struct Init {
        block_a_id: block::Id,
        block_b_id: block::Id,
        blocks_index: index::Blocks,
        gaps: Index,
    }

    impl Init {
        fn new() -> Init {
            let block_a_id = block::Id::init();

            let mut blocks_index = index::Blocks::new();
            blocks_index.insert(block_a_id.clone(), index::BlockEntry {
                offset: 0,
                header: storage::BlockHeader::Regular(storage::BlockHeaderRegular {
                    block_id: block_a_id.clone(),
                    block_size: 4,
                }),
            });
            let block_b_id = block_a_id.next();
            blocks_index.insert(block_b_id.clone(), index::BlockEntry {
                offset: 8,
                header: storage::BlockHeader::EndOfFile,
            });

            let mut gaps = Index::new();
            gaps.insert(4, GapBetween::TwoBlocks { left_block: block_a_id.clone(), right_block: block_b_id.clone(), });
            gaps.insert(60, GapBetween::BlockAndEnd { left_block: block_b_id.clone(), });

            Init { block_a_id, block_b_id, blocks_index, gaps, }
        }
    }
}
