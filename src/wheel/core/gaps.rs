use std::{
    collections::{
        BTreeMap,
    },
};

use super::{
    block,
    SpaceKey,
    BlockInfo,
    BlockEntry,
};

#[derive(Debug)]
pub struct Index {
    serial: usize,
    gaps: BTreeMap<SpaceKey, Gap>,
    space_total: usize,
    remove_buf: Vec<SpaceKey>,
}

#[derive(Debug)]
struct Gap {
    between: GapBetween<block::Id>,
    state: GapState,
}

#[derive(Debug)]
enum GapState {
    Regular,
    LockedDefrag,
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

#[derive(Clone, PartialEq, Debug)]
pub enum Allocated<'a> {
    Success {
        space_available: usize,
        between: GapBetween<BlockInfo<'a>>,
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

    pub fn insert(&mut self, space_available: usize, between: GapBetween<block::Id>) -> SpaceKey {
        self.serial += 1;
        let space_key = SpaceKey { space_available, serial: self.serial, };

        self.gaps.insert(space_key, Gap { between, state: GapState::Regular, });
        self.space_total += space_available;
        space_key
    }

    pub fn get(&self, space_key: &SpaceKey) -> Option<&GapBetween<block::Id>> {
        self.gaps.get(space_key).map(|gap| &gap.between)
    }

    pub fn allocate<'a, G>(
        &mut self,
        space_required: usize,
        defrag_pending_bytes: Option<usize>,
        block_get: G,
    )
        -> Result<Allocated<'a>, Error>
    where G: Fn(&block::Id) -> Option<&'a BlockEntry>
    {
        let mut maybe_result = None;
        let mut candidates = self.gaps.range(SpaceKey { space_available: space_required, serial: 0, } ..);
        loop {
            match candidates.next() {
                None =>
                    break,
                Some((key, Gap { between, state: GapState::Regular, })) => {
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
                            if let Some(block_entry) = block_get(right_block) {
                                maybe_result = Some(Allocated::Success {
                                    space_available: key.space_available,
                                    between: GapBetween::StartAndBlock {
                                        right_block: BlockInfo {
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
                            let maybe_left = block_get(left_block);
                            let maybe_right = block_get(right_block);
                            if let (Some(left_block_entry), Some(right_block_entry)) = (maybe_left, maybe_right) {
                                maybe_result = Some(Allocated::Success {
                                    space_available: key.space_available,
                                    between: GapBetween::TwoBlocks {
                                        left_block: BlockInfo {
                                            block_id: left_block.clone(),
                                            block_entry: left_block_entry,
                                        },
                                        right_block: BlockInfo {
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
                            if let Some(block_entry) = block_get(left_block) {
                                maybe_result = Some(Allocated::Success {
                                    space_available: key.space_available,
                                    between: GapBetween::BlockAndEnd {
                                        left_block: BlockInfo {
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
                Some((_key, Gap { state: GapState::LockedDefrag, .. })) =>
                    (),
            }
        }

        for key in self.remove_buf.drain(..) {
            self.gaps.remove(&key);
        }

        if let Some(result) = maybe_result {
            return Ok(result);
        }

        if let Some(pending_bytes) = defrag_pending_bytes {
            if space_required + pending_bytes <= self.space_total {
                return Ok(Allocated::PendingDefragmentation);
            }
        }

        Err(Error::NoSpaceLeft)
    }

    pub fn lock_defrag(&mut self, key: &SpaceKey) {
        if let Some(gap) = self.gaps.get_mut(key) {
            gap.state = GapState::LockedDefrag;
        }
    }

    pub fn remove(&mut self, key: &SpaceKey) -> Option<GapBetween<block::Id>> {
        if let Some(gap) = self.gaps.remove(key) {
            self.space_total -= key.space_available();
            Some(gap.between)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{
            HashMap,
        },
    };

    use super::{
        super::{
            storage,
            Environs,
            LeftEnvirons,
            RightEnvirons,
            BlockEntry,
            BlockInfo,
        },
        block,
        Index,
        Error,
        SpaceKey,
        Allocated,
        GapBetween,
    };

    #[test]
    fn allocated_success_between_two_blocks() {
        let Init { block_a_id, block_b_id, blocks_index, mut gaps_index, .. } = Init::new();

        assert_eq!(
            gaps_index.allocate(3, None, |key| blocks_index.get(key)),
            Ok(Allocated::Success {
                space_available: 4,
                between: GapBetween::TwoBlocks {
                    left_block: BlockInfo {
                        block_id: block_a_id.clone(),
                        block_entry: &BlockEntry {
                            offset: 0,
                            header: storage::BlockHeader {
                                block_id: block_a_id.clone(),
                                block_size: 4,
                                ..Default::default()
                            },
                            block_bytes: None,
                            environs: Environs {
                                left: LeftEnvirons::Start,
                                right: RightEnvirons::Space { space_key: SpaceKey { space_available: 4, serial: 1, }, },
                            },
                            tasks_head: Default::default(),
                        },
                    },
                    right_block: BlockInfo {
                        block_id: block_b_id.clone(),
                        block_entry: &BlockEntry {
                            offset: 8,
                            header: storage::BlockHeader {
                                block_id: block_b_id.clone(),
                                block_size: 0,
                                ..Default::default()
                            },
                            block_bytes: None,
                            environs: Environs {
                                left: LeftEnvirons::Space { space_key: SpaceKey { space_available: 4, serial: 1, }, },
                                right: RightEnvirons::Space { space_key: SpaceKey { space_available: 60, serial: 2, }, },
                            },
                            tasks_head: Default::default(),
                        },
                    },
                },
            }),
        );
    }

    #[test]
    fn allocated_success_between_block_and_end() {
        let Init { block_b_id, blocks_index, mut gaps_index, .. } = Init::new();

        assert_eq!(
            gaps_index.allocate(33, None, |key| blocks_index.get(key)),
            Ok(Allocated::Success {
                space_available: 60,
                between: GapBetween::BlockAndEnd {
                    left_block: BlockInfo {
                        block_id: block_b_id.clone(),
                        block_entry: &BlockEntry {
                            offset: 8,
                            header: storage::BlockHeader {
                                block_id: block_b_id.clone(),
                                block_size: 0,
                                ..Default::default()
                            },
                            block_bytes: None,
                            environs: Environs {
                                left: LeftEnvirons::Space { space_key: SpaceKey { space_available: 4, serial: 1, }, },
                                right: RightEnvirons::Space { space_key: SpaceKey { space_available: 60, serial: 2, }, },
                            },
                            tasks_head: Default::default(),
                        },
                    },
                },
            }),
        );
    }

    #[test]
    fn allocated_success_pending_defragmentation() {
        let Init { blocks_index, mut gaps_index, .. } = Init::new();

        assert_eq!(
            gaps_index.allocate(61, Some(0), |key| blocks_index.get(key)),
            Ok(Allocated::PendingDefragmentation),
        );
    }

    #[test]
    fn allocated_error_no_space_left() {
        let Init { blocks_index, mut gaps_index, .. } = Init::new();

        assert_eq!(
            gaps_index.allocate(65, None, |key| blocks_index.get(key)),
            Err(Error::NoSpaceLeft),
        );
    }

    #[test]
    fn allocate_until_no_space() {
        let Init { block_a_id, block_b_id, blocks_index, mut gaps_index, .. } = Init::new();

        assert_eq!(
            gaps_index.allocate(3, None, |key| blocks_index.get(key)),
            Ok(Allocated::Success {
                space_available: 4,
                between: GapBetween::TwoBlocks {
                    left_block: BlockInfo {
                        block_id: block_a_id.clone(),
                        block_entry: &BlockEntry {
                            offset: 0,
                            header: storage::BlockHeader {
                                block_id: block_a_id.clone(),
                                block_size: 4,
                                ..Default::default()
                            },
                            block_bytes: None,
                            environs: Environs {
                                left: LeftEnvirons::Start,
                                right: RightEnvirons::Space { space_key: SpaceKey { space_available: 4, serial: 1, }, },
                            },
                            tasks_head: Default::default(),
                        },
                    },
                    right_block: BlockInfo {
                        block_id: block_b_id.clone(),
                        block_entry: &BlockEntry {
                            offset: 8,
                            header: storage::BlockHeader {
                                block_id: block_b_id.clone(),
                                block_size: 0,
                                ..Default::default()
                            },
                            block_bytes: None,
                            environs: Environs {
                                left: LeftEnvirons::Space { space_key: SpaceKey { space_available: 4, serial: 1, }, },
                                right: RightEnvirons::Space { space_key: SpaceKey { space_available: 60, serial: 2, }, },
                            },
                            tasks_head: Default::default(),
                        },
                    },
                },
            }),
        );
        assert_eq!(
            gaps_index.allocate(3, None, |key| blocks_index.get(key)),
            Ok(Allocated::Success {
                space_available: 60,
                between: GapBetween::BlockAndEnd {
                    left_block: BlockInfo {
                        block_id: block_b_id.clone(),
                        block_entry: &BlockEntry {
                            offset: 8,
                            header: storage::BlockHeader {
                                block_id: block_b_id.clone(),
                                block_size: 0,
                                ..Default::default()
                            },
                            block_bytes: None,
                            environs: Environs {
                                left: LeftEnvirons::Space { space_key: SpaceKey { space_available: 4, serial: 1, }, },
                                right: RightEnvirons::Space { space_key: SpaceKey { space_available: 60, serial: 2, }, },
                            },
                            tasks_head: Default::default(),
                        },
                    },
                },
            }),
        );
        assert_eq!(
            gaps_index.allocate(3, None, |key| blocks_index.get(key)),
            Err(Error::NoSpaceLeft),
        );
    }

    #[test]
    fn lock_unlock_defrag() {
        let Init { block_b_id, space_key_a, blocks_index, mut gaps_index, .. } = Init::new();

        gaps_index.lock_defrag(&space_key_a);
        assert_eq!(
            gaps_index.allocate(3, Some(0), |key| blocks_index.get(key)),
            Ok(Allocated::Success {
                space_available: 60,
                between: GapBetween::BlockAndEnd {
                    left_block: BlockInfo {
                        block_id: block_b_id.clone(),
                        block_entry: &BlockEntry {
                            offset: 8,
                            header: storage::BlockHeader {
                                block_id: block_b_id.clone(),
                                block_size: 0,
                                ..Default::default()
                            },
                            block_bytes: None,
                            environs: Environs {
                                left: LeftEnvirons::Space { space_key: SpaceKey { space_available: 4, serial: 1, }, },
                                right: RightEnvirons::Space { space_key: SpaceKey { space_available: 60, serial: 2, }, },
                            },
                            tasks_head: Default::default(),
                        },
                    },
                },
            }),
        );

        assert_eq!(
            gaps_index.allocate(3, Some(0), |key| blocks_index.get(key)),
            Ok(Allocated::PendingDefragmentation),
        );
    }


    struct Init {
        block_a_id: block::Id,
        block_b_id: block::Id,
        space_key_a: SpaceKey,
        blocks_index: HashMap<block::Id, BlockEntry>,
        gaps_index: Index,
    }

    impl Init {
        fn new() -> Init {
            let block_a_id = block::Id::init();
            let block_b_id = block_a_id.next();

            let mut gaps_index = Index::new();
            let space_key_a = gaps_index.insert(4, GapBetween::TwoBlocks {
                left_block: block_a_id.clone(),
                right_block: block_b_id.clone(),
            });
            let space_key_b = gaps_index.insert(60, GapBetween::BlockAndEnd {
                left_block: block_b_id.clone(),
            });

            let mut blocks_index = HashMap::new();
            blocks_index.insert(block_a_id.clone(), BlockEntry {
                offset: 0,
                header: storage::BlockHeader {
                    block_id: block_a_id.clone(),
                    block_size: 4,
                    ..Default::default()
                },
                block_bytes: None,
                environs: Environs {
                    left: LeftEnvirons::Start,
                    right: RightEnvirons::Space { space_key: space_key_a, },
                },
                tasks_head: Default::default(),
            });
            blocks_index.insert(block_b_id.clone(), BlockEntry {
                offset: 8,
                header: storage::BlockHeader {
                    block_id: block_b_id.clone(),
                    block_size: 0,
                    ..Default::default()
                },
                block_bytes: None,
                environs: Environs {
                    left: LeftEnvirons::Space { space_key: space_key_a, },
                    right: RightEnvirons::Space { space_key: space_key_b, },
                },
                tasks_head: Default::default(),
            });

            Init { block_a_id, block_b_id, space_key_a, blocks_index, gaps_index, }
        }
    }
}
