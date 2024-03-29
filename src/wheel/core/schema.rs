use std::mem::drop;

use alloc_pool::bytes::Bytes;

use super::{
    gaps,
    block,
    blocks,
    storage,
    SpaceKey,
    DefragGaps,
    BlockEntry,
    Environs,
    LeftEnvirons,
    RightEnvirons,
};

use crate::{
    Info,
};

#[derive(Debug)]
pub struct Schema {
    next_block_id: block::Id,
    storage_layout: storage::Layout,
    blocks_index: blocks::Index,
    gaps_index: gaps::Index,
    read_block_cache_hits: usize,
    read_block_cache_misses: usize,
}

#[derive(Debug)]
pub enum WriteBlockOp {
    Perform(WriteBlockPerform),
    QueuePendingDefrag { space_required: usize, },
    ReplyNoSpaceLeft,
}

#[derive(Debug)]
pub struct WriteBlockPerform {
    pub defrag_op: DefragOp,
    pub task_op: WriteBlockTaskOp,
    pub right_space_key: Option<SpaceKey>,
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum DefragOp {
    None,
    Queue { defrag_gaps: DefragGaps, moving_block_id: block::Id, },
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct WriteBlockTaskOp {
    pub block_id: block::Id,
    pub block_offset: u64,
}

#[derive(Debug)]
pub enum ReadBlockOp<'a> {
    Perform(ReadBlockPerform<'a>),
    CacheHit(ReadBlockCacheHit<'a>),
    NotFound,
}

#[derive(Debug)]
pub struct ReadBlockPerform<'a> {
    pub block_header: &'a storage::BlockHeader,
}

#[derive(Debug)]
pub struct ReadBlockCacheHit<'a> {
    pub block_header: &'a storage::BlockHeader,
    pub block_bytes: Bytes,
}

#[derive(Debug)]
pub enum DeleteBlockOp {
    Perform(DeleteBlockPerform),
    NotFound,
}

#[derive(Debug)]
pub struct DeleteBlockPerform;

#[derive(Debug)]
pub enum ReadBlockTaskDoneOp {
    NotFound,
    Perform(ReadBlockTaskDonePerform),
}

#[derive(Debug)]
pub struct ReadBlockTaskDonePerform;

#[derive(Debug)]
pub enum DeleteBlockTaskDoneOp {
    Perform(DeleteBlockTaskDonePerform),
}

#[derive(Debug)]
pub struct DeleteBlockTaskDonePerform {
    pub defrag_op: DefragOp,
    pub block_entry: BlockEntry,
    pub freed_space_key: SpaceKey,
}

#[derive(Debug)]
pub enum DeleteBlockTaskDoneDefragOp {
    Perform(DeleteBlockTaskDoneDefragPerform),
}

#[derive(Debug)]
pub struct DeleteBlockTaskDoneDefragPerform {
    pub block_offset: u64,
    pub defrag_op: DefragOp,
    pub freed_space_key: Option<SpaceKey>,
}

impl Schema {
    pub fn storage_layout(&self) -> &storage::Layout {
        &self.storage_layout
    }

    pub fn info(&self) -> Info {
        let blocks_count = self.blocks_index.count();
        let service_bytes_used = self.storage_layout.service_size_min()
            + (blocks_count * self.storage_layout.data_size_block_min());
        let data_bytes_used = self.blocks_index.blocks_total_size() as usize;
        let bytes_free = self.gaps_index.space_total();
        let read_block_cache_hits = self.read_block_cache_hits;
        let read_block_cache_misses = self.read_block_cache_misses;
        Info {
            blocks_count,
            service_bytes_used,
            data_bytes_used,
            defrag_write_pending_bytes: 0,
            bytes_free,
            wheel_size_bytes: service_bytes_used
                + data_bytes_used
                + bytes_free,
            read_block_cache_hits,
            read_block_cache_misses,
            ..Default::default()
        }
    }

    pub fn process_write_block_request(
        &mut self,
        block_bytes: &Bytes,
        defrag_pending_bytes: Option<usize>,
    )
        -> WriteBlockOp
    {
        let block_id = self.next_block_id.clone();
        self.next_block_id = self.next_block_id.next();

        let mut defrag_op = DefragOp::None;
        let mut right_space_key = None;

        let space_required = block_bytes.len()
            + self.storage_layout.data_size_block_min();

        let blocks_index = &self.blocks_index;
        let block_offset = match self.gaps_index.allocate(space_required, defrag_pending_bytes, |block_id| blocks_index.get(block_id)) {

            // before: ^| ....... | A | ... |$
            // after:  ^| R | ... | A | ... |$
            Ok(gaps::Allocated::Success { space_available, between: gaps::GapBetween::StartAndBlock { right_block, }, }) => {
                let block_offset = self.storage_layout.wheel_header_size as u64;
                let right_block_id = right_block.block_id;

                let space_left = space_available - space_required;
                let (self_env, right_env) = if space_left > 0 {
                    let space_key = self.gaps_index.insert(
                        space_left,
                        gaps::GapBetween::TwoBlocks {
                            left_block: block_id.clone(),
                            right_block: right_block_id.clone(),
                        },
                    );
                    right_space_key = Some(space_key);
                    defrag_op = self.make_defrag_op(space_key, right_block_id.clone());
                    (
                        RightEnvirons::Space { space_key, },
                        LeftEnvirons::Space { space_key, },
                    )
                } else {
                    (
                        RightEnvirons::Block { block_id: right_block_id.clone(), },
                        LeftEnvirons::Block { block_id: block_id.clone(), },
                    )
                };
                self.blocks_index.insert(
                    block_id.clone(),
                    BlockEntry {
                        offset: block_offset,
                        header: storage::BlockHeader {
                            block_id: block_id.clone(),
                            block_size: block_bytes.len() as u64,
                            ..Default::default()
                        },
                        environs: Environs {
                            left: LeftEnvirons::Start,
                            right: self_env,
                        },
                        tasks_head: Default::default(),
                        cached: Some(block_bytes.downgrade()),
                    },
                );
                self.blocks_index.update_env_left(&right_block_id, right_env);

                block_offset
            },

            // before: ^| ... | A | ....... | B | ... |$
            // after:  ^| ... | A | R | ... | B | ... |$
            Ok(gaps::Allocated::Success { space_available, between: gaps::GapBetween::TwoBlocks { left_block, right_block, }, }) => {
                let block_offset = left_block.block_entry.offset
                    + self.storage_layout.data_size_block_min() as u64
                    + left_block.block_entry.header.block_size as u64;
                let left_block_id = left_block.block_id;
                let right_block_id = right_block.block_id;

                let space_left = space_available - space_required;
                let (self_env, left_env, right_env) = if space_left > 0 {
                    let space_key = self.gaps_index.insert(
                        space_left,
                        gaps::GapBetween::TwoBlocks {
                            left_block: block_id.clone(),
                            right_block: right_block_id.clone(),
                        },
                    );
                    right_space_key = Some(space_key);
                    defrag_op = self.make_defrag_op(space_key, right_block_id.clone());
                    (
                        RightEnvirons::Space { space_key, },
                        RightEnvirons::Block { block_id: block_id.clone(), },
                        LeftEnvirons::Space { space_key, },
                    )
                } else {
                    (
                        RightEnvirons::Block { block_id: right_block_id.clone(), },
                        RightEnvirons::Block { block_id: block_id.clone(), },
                        LeftEnvirons::Block { block_id: block_id.clone(), },
                    )
                };
                self.blocks_index.insert(
                    block_id.clone(),
                    BlockEntry {
                        offset: block_offset,
                        header: storage::BlockHeader {
                            block_id: block_id.clone(),
                            block_size: block_bytes.len() as u64,
                            ..Default::default()
                        },
                        environs: Environs {
                            left: LeftEnvirons::Block { block_id: left_block_id.clone(), },
                            right: self_env,
                        },
                        tasks_head: Default::default(),
                        cached: Some(block_bytes.downgrade()),
                    },
                );
                self.blocks_index.update_env_right(&left_block_id, left_env);
                self.blocks_index.update_env_left(&right_block_id, right_env);

                block_offset
            },

            // before: ^| ... | A | ....... |$
            // after:  ^| ... | A | R | ... |$
            Ok(gaps::Allocated::Success { space_available, between: gaps::GapBetween::BlockAndEnd { left_block, }, }) => {
                let block_offset = left_block.block_entry.offset
                    + self.storage_layout.data_size_block_min() as u64
                    + left_block.block_entry.header.block_size as u64;
                let left_block_id = left_block.block_id;

                let space_left = space_available - space_required;
                let self_env = if space_left > 0 {
                    let space_key = self.gaps_index.insert(
                        space_left,
                        gaps::GapBetween::BlockAndEnd {
                            left_block: block_id.clone(),
                        },
                    );
                    right_space_key = Some(space_key);
                    RightEnvirons::Space { space_key, }
                } else {
                    RightEnvirons::End
                };
                self.blocks_index.insert(
                    block_id.clone(),
                    BlockEntry {
                        offset: block_offset,
                        header: storage::BlockHeader {
                            block_id: block_id.clone(),
                            block_size: block_bytes.len() as u64,
                            ..Default::default()
                        },
                        environs: Environs {
                            left: LeftEnvirons::Block { block_id: left_block_id.clone(), },
                            right: self_env,
                        },
                        tasks_head: Default::default(),
                        cached: Some(block_bytes.downgrade()),
                    },
                );
                self.blocks_index.update_env_right(&left_block_id, RightEnvirons::Block { block_id: block_id.clone(), });
                block_offset
            },

            // before: ^| ....... |$
            // after:  ^| R | ... |$
            Ok(gaps::Allocated::Success { space_available, between: gaps::GapBetween::StartAndEnd, }) => {
                let block_offset = self.storage_layout.wheel_header_size as u64;

                let space_left = space_available - space_required;
                let environs = if space_left > 0 {
                    let space_key = self.gaps_index.insert(
                        space_left,
                        gaps::GapBetween::BlockAndEnd {
                            left_block: block_id.clone(),
                        },
                    );
                    right_space_key = Some(space_key);
                    Environs {
                        left: LeftEnvirons::Start,
                        right: RightEnvirons::Space { space_key, },
                    }
                } else {
                    Environs {
                        left: LeftEnvirons::Start,
                        right: RightEnvirons::End,
                    }
                };
                self.blocks_index.insert(
                    block_id.clone(),
                    BlockEntry {
                        offset: block_offset,
                        header: storage::BlockHeader {
                            block_id: block_id.clone(),
                            block_size: block_bytes.len() as u64,
                            ..Default::default()
                        },
                        environs,
                        tasks_head: Default::default(),
                        cached: Some(block_bytes.downgrade()),
                    },
                );
                block_offset
            },

            Ok(gaps::Allocated::PendingDefragmentation) =>
                return WriteBlockOp::QueuePendingDefrag { space_required, },

            Err(gaps::Error::NoSpaceLeft) =>
                return WriteBlockOp::ReplyNoSpaceLeft,

        };

        WriteBlockOp::Perform(
            WriteBlockPerform {
                defrag_op,
                task_op: WriteBlockTaskOp {
                    block_id,
                    block_offset,
                },
                right_space_key,
            },
        )
    }

    pub fn process_read_block_request<'a>(&'a mut self, block_id: &block::Id) -> ReadBlockOp<'a> {
        match self.blocks_index.get_mut(block_id) {
            Some(block_entry) => {
                if let Some(weak) = &block_entry.cached {
                    if let Some(block_bytes) = weak.upgrade() {
                        self.read_block_cache_hits += 1;
                        return ReadBlockOp::CacheHit(ReadBlockCacheHit {
                            block_header: &block_entry.header,
                            block_bytes,
                        });
                    }
                    self.read_block_cache_misses += 1;
                    block_entry.cached = None;
                }

                ReadBlockOp::Perform(ReadBlockPerform {
                    block_header: &block_entry.header,
                })
            },
            None =>
                ReadBlockOp::NotFound,
        }
    }

    pub fn process_delete_block_request(&mut self, block_id: &block::Id) -> DeleteBlockOp {
        match self.blocks_index.get(block_id) {
            Some(..) =>
                DeleteBlockOp::Perform(DeleteBlockPerform),
            None =>
                DeleteBlockOp::NotFound,
        }
    }

    pub fn process_read_block_task_done(&mut self, read_block_id: &block::Id, maybe_block_bytes: Option<&Bytes>) -> ReadBlockTaskDoneOp {
        match self.blocks_index.get_mut(read_block_id) {
            Some(block_entry) => {
                if let Some(block_bytes) = maybe_block_bytes {
                    block_entry.cached = Some(block_bytes.downgrade());
                }
                ReadBlockTaskDoneOp::Perform(ReadBlockTaskDonePerform)
            },
            None =>
                ReadBlockTaskDoneOp::NotFound,
        }
    }

    pub fn process_delete_block_task_done(&mut self, removed_block_id: block::Id) -> DeleteBlockTaskDoneOp {
        let block_entry = self.blocks_index.remove(&removed_block_id).unwrap();
        let mut defrag_op = DefragOp::None;

        let freed_space_key = match &block_entry.environs {

            // before: ^| R |$
            // after:  ^| . |$
            Environs { left: LeftEnvirons::Start, right: RightEnvirons::End, } => {
                assert_eq!(self.gaps_index.space_total(), 0);
                assert_eq!(block_entry.offset, self.storage_layout.wheel_header_size as u64);
                let space_available = block_entry.header.block_size as usize
                    + self.storage_layout.data_size_block_min();
                self.gaps_index.insert(space_available, gaps::GapBetween::StartAndEnd)
            },

            Environs { left: LeftEnvirons::Start, right: RightEnvirons::Space { space_key, }, } =>
                match self.gaps_index.remove(space_key) {
                    value @ None | value @ Some(gaps::GapBetween::StartAndEnd) | value @ Some(gaps::GapBetween::StartAndBlock { .. }) =>
                        unreachable!("delete inconsistent environs Start/Space with right space = {:?}", value),
                    // before: ^| R | ... | A | ... |$
                    // after:  ^| ........| A | ... |$
                    Some(gaps::GapBetween::TwoBlocks { left_block, right_block, }) => {
                        assert_eq!(left_block, removed_block_id);
                        #[allow(clippy::drop_non_drop)]
                        drop(left_block);
                        let space_available = block_entry.header.block_size as usize
                            + self.storage_layout.data_size_block_min()
                            + space_key.space_available();
                        let space_key = self.gaps_index.insert(
                            space_available,
                            gaps::GapBetween::StartAndBlock { right_block: right_block.clone(), },
                        );
                        self.blocks_index.update_env_left(&right_block, LeftEnvirons::Space { space_key, });
                        assert_eq!(block_entry.offset, self.storage_layout.wheel_header_size as u64);
                        defrag_op = self.make_defrag_op(space_key, right_block.clone());
                        space_key
                    },
                    // before: ^| R | ... |$
                    // after:  ^| ....... |$
                    Some(gaps::GapBetween::BlockAndEnd { left_block, }) => {
                        assert_eq!(left_block, removed_block_id);
                        #[allow(clippy::drop_non_drop)]
                        drop(left_block);
                        assert_eq!(block_entry.offset, self.storage_layout.wheel_header_size as u64);
                        let space_available = block_entry.header.block_size as usize
                            + self.storage_layout.data_size_block_min()
                            + space_key.space_available();
                        self.gaps_index.insert(space_available, gaps::GapBetween::StartAndEnd)
                    },
                },

            // before: ^| R | | A | ... |$
            // after:  ^| ... | A | ... |$
            Environs {
                left: LeftEnvirons::Start,
                right: RightEnvirons::Block { block_id, },
            } => {
                let space_available = block_entry.header.block_size as usize
                    + self.storage_layout.data_size_block_min();
                let space_key = self.gaps_index.insert(space_available, gaps::GapBetween::StartAndBlock { right_block: block_id.clone(), });
                self.blocks_index.update_env_left(block_id, LeftEnvirons::Space { space_key, });
                assert_eq!(block_entry.offset, self.storage_layout.wheel_header_size as u64);
                defrag_op = self.make_defrag_op(space_key, block_id.clone());
                space_key
            },

            Environs {
                left: LeftEnvirons::Space { space_key, },
                right: RightEnvirons::End,
            } =>
                match self.gaps_index.remove(space_key) {
                    value @ None | value @ Some(gaps::GapBetween::StartAndEnd) | value @ Some(gaps::GapBetween::BlockAndEnd { .. }) =>
                        unreachable!("delete inconsistent environs Space/End with left space = {:?}", value),
                    // before: ^| ... | A | ... | R |$
                    // after:  ^| ... | A | ....... |$
                    Some(gaps::GapBetween::TwoBlocks { left_block, right_block, }) => {
                        assert_eq!(right_block, removed_block_id);
                        #[allow(clippy::drop_non_drop)]
                        drop(right_block);
                        let space_available = block_entry.header.block_size as usize
                            + self.storage_layout.data_size_block_min()
                            + space_key.space_available();
                        let space_key = self.gaps_index.insert(
                            space_available,
                            gaps::GapBetween::BlockAndEnd { left_block: left_block.clone(), },
                        );
                        self.blocks_index.update_env_right(&left_block, RightEnvirons::Space { space_key, });
                        space_key
                    },
                    // before: ^| ... | R |$
                    // after:  ^| ....... |$
                    Some(gaps::GapBetween::StartAndBlock { right_block, }) => {
                        assert_eq!(right_block, removed_block_id);
                        #[allow(clippy::drop_non_drop)]
                        drop(right_block);
                        let space_available = block_entry.header.block_size as usize
                            + self.storage_layout.data_size_block_min()
                            + space_key.space_available();
                        self.gaps_index.insert(space_available, gaps::GapBetween::StartAndEnd)
                    },
                },

            Environs {
                left: LeftEnvirons::Space { space_key: space_key_left, },
                right: RightEnvirons::Space { space_key: space_key_right, },
            } =>
                match (self.gaps_index.remove(space_key_left), self.gaps_index.remove(space_key_right)) {
                    (lvalue @ None, rvalue) | (lvalue, rvalue @ None) |
                    (lvalue @ Some(gaps::GapBetween::StartAndEnd), rvalue) |
                    (lvalue, rvalue @ Some(gaps::GapBetween::StartAndEnd)) |
                    (lvalue @ Some(gaps::GapBetween::BlockAndEnd { .. }), rvalue) |
                    (lvalue, rvalue @ Some(gaps::GapBetween::StartAndBlock { .. })) =>
                        unreachable!("delete inconsistent environs Space/Space with left space = {:?} and right space = {:?}", lvalue, rvalue),
                    // before: ^| ... | R | ... |$
                    // after:  ^| ..............|$
                    (
                        Some(gaps::GapBetween::StartAndBlock { right_block: right_block_left, }),
                        Some(gaps::GapBetween::BlockAndEnd { left_block: left_block_right, }),
                    ) => {
                        assert_eq!(right_block_left, removed_block_id);
                        #[allow(clippy::drop_non_drop)]
                        drop(right_block_left);
                        assert_eq!(left_block_right, removed_block_id);
                        #[allow(clippy::drop_non_drop)]
                        drop(left_block_right);
                        let space_available = block_entry.header.block_size as usize
                            + self.storage_layout.data_size_block_min()
                            + space_key_left.space_available()
                            + space_key_right.space_available();
                        self.gaps_index.insert(space_available, gaps::GapBetween::StartAndEnd)
                    },
                    // before: ^| ... | R | ... | A | ... |$
                    // after:  ^| ............. | A | ... |$
                    (
                        Some(gaps::GapBetween::StartAndBlock { right_block: right_block_left, }),
                        Some(gaps::GapBetween::TwoBlocks { left_block: left_block_right, right_block: right_block_right, }),
                    ) => {
                        assert_eq!(right_block_left, removed_block_id);
                        #[allow(clippy::drop_non_drop)]
                        drop(right_block_left);
                        assert_eq!(left_block_right, removed_block_id);
                        #[allow(clippy::drop_non_drop)]
                        drop(left_block_right);
                        let space_available = block_entry.header.block_size as usize
                            + self.storage_layout.data_size_block_min()
                            + space_key_left.space_available()
                            + space_key_right.space_available();
                        let space_key = self.gaps_index.insert(
                            space_available,
                            gaps::GapBetween::StartAndBlock { right_block: right_block_right.clone(), },
                        );
                        self.blocks_index.update_env_left(&right_block_right, LeftEnvirons::Space { space_key, });
                        defrag_op = self.make_defrag_op(space_key, right_block_right.clone());
                        space_key
                    },
                    // before: ^| ... | A | ... | R | ... |$
                    // after:  ^| ... | A | ............. |$
                    (
                        Some(gaps::GapBetween::TwoBlocks { left_block: left_block_left, right_block: right_block_left, }),
                        Some(gaps::GapBetween::BlockAndEnd { left_block: left_block_right, }),
                    ) => {
                        assert_eq!(right_block_left, removed_block_id);
                        #[allow(clippy::drop_non_drop)]
                        drop(right_block_left);
                        assert_eq!(left_block_right, removed_block_id);
                        #[allow(clippy::drop_non_drop)]
                        drop(left_block_right);
                        let space_available = block_entry.header.block_size as usize
                            + self.storage_layout.data_size_block_min()
                            + space_key_left.space_available()
                            + space_key_right.space_available();
                        let space_key = self.gaps_index.insert(
                            space_available,
                            gaps::GapBetween::BlockAndEnd { left_block: left_block_left.clone(), },
                        );
                        self.blocks_index.update_env_right(&left_block_left, RightEnvirons::Space { space_key, });
                        space_key
                    },
                    // before: ^| ... | A | ... | R | ... | B | ... |$
                    // after:  ^| ... | A | ............. | B | ... |$
                    (
                        Some(gaps::GapBetween::TwoBlocks { left_block: left_block_left, right_block: right_block_left, }),
                        Some(gaps::GapBetween::TwoBlocks { left_block: left_block_right, right_block: right_block_right, }),
                    ) => {
                        assert_eq!(right_block_left, removed_block_id);
                        #[allow(clippy::drop_non_drop)]
                        drop(right_block_left);
                        assert_eq!(left_block_right, removed_block_id);
                        #[allow(clippy::drop_non_drop)]
                        drop(left_block_right);
                        let space_available = block_entry.header.block_size as usize
                            + self.storage_layout.data_size_block_min()
                            + space_key_left.space_available()
                            + space_key_right.space_available();
                        let space_key = self.gaps_index.insert(
                            space_available,
                            gaps::GapBetween::TwoBlocks {
                                left_block: left_block_left.clone(),
                                right_block: right_block_right.clone(),
                            },
                        );
                        self.blocks_index.update_env_right(&left_block_left, RightEnvirons::Space { space_key, });
                        self.blocks_index.update_env_left(&right_block_right, LeftEnvirons::Space { space_key, });
                        defrag_op = self.make_defrag_op(space_key, right_block_right.clone());
                        space_key
                    },
                },

            Environs {
                left: LeftEnvirons::Space { space_key, },
                right: RightEnvirons::Block { block_id, },
            } =>
                match self.gaps_index.remove(space_key) {
                    value @ None | value @ Some(gaps::GapBetween::StartAndEnd) | value @ Some(gaps::GapBetween::BlockAndEnd { .. }) =>
                        unreachable!("delete inconsistent environs Space/Block with left space = {:?}", value),
                    // before: ^| ... | A | ... | R || B | ... |$
                    // after:  ^| ... | A | .........| B | ... |$
                    Some(gaps::GapBetween::TwoBlocks { left_block, right_block, }) => {
                        assert_eq!(right_block, removed_block_id);
                        #[allow(clippy::drop_non_drop)]
                        drop(right_block);
                        let space_available = block_entry.header.block_size as usize
                            + self.storage_layout.data_size_block_min()
                            + space_key.space_available();
                        let space_key = self.gaps_index.insert(
                            space_available,
                            gaps::GapBetween::TwoBlocks {
                                left_block: left_block.clone(),
                                right_block: block_id.clone(),
                            },
                        );
                        self.blocks_index.update_env_right(&left_block, RightEnvirons::Space { space_key, });
                        self.blocks_index.update_env_left(block_id, LeftEnvirons::Space { space_key, });
                        defrag_op = self.make_defrag_op(space_key, block_id.clone());
                        space_key
                    },
                    // before: ^| ... | R || B | ... |$
                    // after:  ^| ........ | B | ... |$
                    Some(gaps::GapBetween::StartAndBlock { right_block, }) => {
                        assert_eq!(right_block, removed_block_id);
                        #[allow(clippy::drop_non_drop)]
                        drop(right_block);
                        let space_available = block_entry.header.block_size as usize
                            + self.storage_layout.data_size_block_min()
                            + space_key.space_available();
                        let space_key = self.gaps_index.insert(
                            space_available,
                            gaps::GapBetween::StartAndBlock { right_block: block_id.clone(), },
                        );
                        self.blocks_index.update_env_left(block_id, LeftEnvirons::Space { space_key, });
                        defrag_op = self.make_defrag_op(space_key, block_id.clone());
                        space_key
                    },
                },

            // before: ^| ... | A || R ||$
            // after:  ^| ... | A | ... |$
            Environs {
                left: LeftEnvirons::Block { block_id, },
                right: RightEnvirons::End,
            } => {
                let space_available = block_entry.header.block_size as usize
                    + self.storage_layout.data_size_block_min();
                let space_key = self.gaps_index.insert(
                    space_available,
                    gaps::GapBetween::BlockAndEnd { left_block: block_id.clone(), },
                );
                self.blocks_index.update_env_right(block_id, RightEnvirons::Space { space_key, });
                space_key
            },

            Environs {
                left: LeftEnvirons::Block { block_id, },
                right: RightEnvirons::Space { space_key, },
            } =>
                match self.gaps_index.remove(space_key) {
                    value @ None | value @ Some(gaps::GapBetween::StartAndEnd) | value @ Some(gaps::GapBetween::StartAndBlock { .. }) =>
                        unreachable!("delete inconsistent environs Block/Space with right space = {:?}", value),
                    // before: ^| ... | A || R | ... | B | ... |$
                    // after:  ^| ... | A | ........ | B | ... |$
                    Some(gaps::GapBetween::TwoBlocks { left_block, right_block, }) => {
                        assert_eq!(left_block, removed_block_id);
                        #[allow(clippy::drop_non_drop)]
                        drop(left_block);
                        let space_available = block_entry.header.block_size as usize
                            + self.storage_layout.data_size_block_min()
                            + space_key.space_available();
                        let space_key = self.gaps_index.insert(
                            space_available,
                            gaps::GapBetween::TwoBlocks {
                                left_block: block_id.clone(),
                                right_block: right_block.clone(),
                            },
                        );
                        self.blocks_index.update_env_right(block_id, RightEnvirons::Space { space_key, });
                        self.blocks_index.update_env_left(&right_block, LeftEnvirons::Space { space_key, });
                        defrag_op = self.make_defrag_op(space_key, right_block.clone());
                        space_key
                    },
                    // before: ^| ... | A || R | ... |$
                    // after:  ^| ... | A | ........ |$
                    Some(gaps::GapBetween::BlockAndEnd { left_block, }) => {
                        assert_eq!(left_block, removed_block_id);
                        #[allow(clippy::drop_non_drop)]
                        drop(left_block);
                        let space_available = block_entry.header.block_size as usize
                            + self.storage_layout.data_size_block_min()
                            + space_key.space_available();
                        let space_key = self.gaps_index.insert(
                            space_available,
                            gaps::GapBetween::BlockAndEnd { left_block: block_id.clone(), },
                        );
                        self.blocks_index.update_env_right(block_id, RightEnvirons::Space { space_key, });
                        space_key
                    },
                },

            // before: ^| ... | A || R || B | ... |$
            // after:  ^| ... | A | ... | B | ... |$
            Environs {
                left: LeftEnvirons::Block { block_id: block_id_left, },
                right: RightEnvirons::Block { block_id: block_id_right, },
            } => {
                let space_available = block_entry.header.block_size as usize
                    + self.storage_layout.data_size_block_min();
                let space_key = self.gaps_index.insert(
                    space_available,
                    gaps::GapBetween::TwoBlocks {
                        left_block: block_id_left.clone(),
                        right_block: block_id_right.clone(),
                    },
                );
                self.blocks_index.update_env_right(block_id_left, RightEnvirons::Space { space_key, });
                self.blocks_index.update_env_left(block_id_right, LeftEnvirons::Space { space_key, });
                defrag_op = self.make_defrag_op(space_key, block_id_right.clone());
                space_key
            },
        };

        DeleteBlockTaskDoneOp::Perform(DeleteBlockTaskDonePerform { defrag_op, block_entry, freed_space_key, })
    }

    pub fn process_delete_block_task_done_defrag(
        &mut self,
        removed_block_id: block::Id,
    )
        -> DeleteBlockTaskDoneDefragOp
    {
        let block_entry = self.blocks_index.get_mut(&removed_block_id).unwrap();
        let start_offset = self.storage_layout.wheel_header_size as u64;
        let data_size_block_min = self.storage_layout.data_size_block_min() as u64;
        let mut defrag_op = DefragOp::None;

        let freed_space_key = match block_entry.environs.clone() {

            value @ Environs { left: LeftEnvirons::Start, .. } =>
                unreachable!("inconsistent defrag delete for block_id = {:?} with environs = {:?}", removed_block_id, value),

            Environs { left: LeftEnvirons::Block { block_id }, .. } => {
                log::debug!(
                    "defrag delete done for block id = {:?} but there is no space already (occupied by block id = {:?} on the left)",
                    removed_block_id,
                    block_id,
                );
                None
            }

            Environs { left: LeftEnvirons::Space { space_key, }, right: RightEnvirons::End, } =>
                match self.gaps_index.remove(&space_key) {
                    value @ None | value @ Some(gaps::GapBetween::StartAndEnd) | value @ Some(gaps::GapBetween::BlockAndEnd { .. }) =>
                        unreachable!("inconsistent defrag delete for block_id = {:?} with gaps = {:?}", removed_block_id, value),
                    // before: ^| ... | R |$
                    // after:  ^| R | ... |$
                    Some(gaps::GapBetween::StartAndBlock { right_block, }) => {
                        assert_eq!(right_block, removed_block_id);
                        #[allow(clippy::drop_non_drop)]
                        drop(right_block);
                        let moved_space_key = self.gaps_index.insert(
                            space_key.space_available(),
                            gaps::GapBetween::BlockAndEnd { left_block: removed_block_id.clone(), },
                        );
                        self.blocks_index.with_mut(&removed_block_id, |block_entry| {
                            block_entry.offset = start_offset;
                            block_entry.environs.left = LeftEnvirons::Start;
                            block_entry.environs.right = RightEnvirons::Space { space_key: moved_space_key, };
                        }).unwrap();
                        Some(moved_space_key)
                    },
                    // before: ^| ... | A | ... | R |$
                    // after:  ^| ... | A | R | ... |$
                    Some(gaps::GapBetween::TwoBlocks { left_block, right_block, }) => {
                        assert_eq!(right_block, removed_block_id);
                        #[allow(clippy::drop_non_drop)]
                        drop(right_block);
                        let moved_space_key = self.gaps_index.insert(
                            space_key.space_available(),
                            gaps::GapBetween::BlockAndEnd { left_block: removed_block_id.clone(), },
                        );
                        let block_offset = self.blocks_index.with_mut(&left_block, |block_entry| {
                            block_entry.environs.right = RightEnvirons::Block { block_id: removed_block_id.clone(), };
                            block_entry.offset
                                + data_size_block_min
                                + block_entry.header.block_size as u64
                        }).unwrap();
                        self.blocks_index.with_mut(&removed_block_id, |block_entry| {
                            block_entry.offset = block_offset;
                            block_entry.environs.left = LeftEnvirons::Block { block_id: left_block.clone(), };
                            block_entry.environs.right = RightEnvirons::Space { space_key: moved_space_key, };
                        }).unwrap();
                        Some(moved_space_key)
                    },
                },

            Environs {
                left: LeftEnvirons::Space { space_key: space_key_left, },
                right: RightEnvirons::Space { space_key: space_key_right, },
            } =>
                match (self.gaps_index.remove(&space_key_left), self.gaps_index.remove(&space_key_right)) {
                    (lvalue @ None, rvalue) | (lvalue, rvalue @ None) |
                    (lvalue @ Some(gaps::GapBetween::StartAndEnd), rvalue) | (lvalue, rvalue @ Some(gaps::GapBetween::StartAndEnd)) |
                    (lvalue @ Some(gaps::GapBetween::BlockAndEnd { .. }), rvalue) | (lvalue, rvalue @ Some(gaps::GapBetween::StartAndBlock { .. })) =>
                        unreachable!("delete defrag inconsistent environs Space/Space with left space = {:?} right space = {:?}", lvalue, rvalue),
                    // before: ^| ... | R | ... |$
                    // after:  ^| R | ......... |$
                    (
                        Some(gaps::GapBetween::StartAndBlock { right_block: right_block_left, }),
                        Some(gaps::GapBetween::BlockAndEnd { left_block: left_block_right, }),
                    ) => {
                        assert_eq!(right_block_left, removed_block_id);
                        #[allow(clippy::drop_non_drop)]
                        drop(right_block_left);
                        assert_eq!(left_block_right, removed_block_id);
                        #[allow(clippy::drop_non_drop)]
                        drop(left_block_right);
                        let moved_space_key = self.gaps_index.insert(
                            space_key_left.space_available() + space_key_right.space_available(),
                            gaps::GapBetween::BlockAndEnd { left_block: removed_block_id.clone(), },
                        );
                        self.blocks_index.with_mut(&removed_block_id, |block_entry| {
                            block_entry.offset = start_offset;
                            block_entry.environs.left = LeftEnvirons::Start;
                            block_entry.environs.right = RightEnvirons::Space { space_key: moved_space_key, };
                        }).unwrap();
                        Some(moved_space_key)
                    },
                    // before: ^| ... | R | ... | A | ... |$
                    // after:  ^| R | ......... | A | ... |$
                    (
                        Some(gaps::GapBetween::StartAndBlock { right_block: right_block_left, }),
                        Some(gaps::GapBetween::TwoBlocks { left_block: left_block_right, right_block: right_block_right, }),
                    ) => {
                        assert_eq!(right_block_left, removed_block_id);
                        #[allow(clippy::drop_non_drop)]
                        drop(right_block_left);
                        assert_eq!(left_block_right, removed_block_id);
                        #[allow(clippy::drop_non_drop)]
                        drop(left_block_right);
                        let moved_space_key = self.gaps_index.insert(
                            space_key_left.space_available() + space_key_right.space_available(),
                            gaps::GapBetween::TwoBlocks { left_block: removed_block_id.clone(), right_block: right_block_right.clone(), },
                        );
                        self.blocks_index.update_env_left(&right_block_right, LeftEnvirons::Space { space_key: moved_space_key, });
                        self.blocks_index.with_mut(&removed_block_id, |block_entry| {
                            block_entry.offset = start_offset;
                            block_entry.environs.left = LeftEnvirons::Start;
                            block_entry.environs.right = RightEnvirons::Space { space_key: moved_space_key, };
                        }).unwrap();
                        defrag_op = self.make_defrag_op(moved_space_key, right_block_right.clone());
                        Some(moved_space_key)
                    },
                    // before: ^| ... | A | ... | R | ... |$
                    // after:  ^| ... | A | R | ......... |$
                    (
                        Some(gaps::GapBetween::TwoBlocks { left_block: left_block_left, right_block: right_block_left, }),
                        Some(gaps::GapBetween::BlockAndEnd { left_block: left_block_right, }),
                    ) => {
                        assert_eq!(right_block_left, removed_block_id);
                        #[allow(clippy::drop_non_drop)]
                        drop(right_block_left);
                        assert_eq!(left_block_right, removed_block_id);
                        #[allow(clippy::drop_non_drop)]
                        drop(left_block_right);
                        let moved_space_key = self.gaps_index.insert(
                            space_key_left.space_available() + space_key_right.space_available(),
                            gaps::GapBetween::BlockAndEnd { left_block: removed_block_id.clone(), },
                        );
                        let block_offset = self.blocks_index.with_mut(&left_block_left, |block_entry| {
                            block_entry.environs.right = RightEnvirons::Block { block_id: removed_block_id.clone(), };
                            block_entry.offset
                                + data_size_block_min
                                + block_entry.header.block_size as u64
                        }).unwrap();
                        self.blocks_index.with_mut(&removed_block_id, |block_entry| {
                            block_entry.offset = block_offset;
                            block_entry.environs.left = LeftEnvirons::Block { block_id: left_block_left, };
                            block_entry.environs.right = RightEnvirons::Space { space_key: moved_space_key, };
                        }).unwrap();
                        Some(moved_space_key)
                    },
                    // before: ^| ... | A | ... | R | ... | B | ... |$
                    // after:  ^| ... | A | R | ......... | B | ... |$
                    (
                        Some(gaps::GapBetween::TwoBlocks { left_block: left_block_left, right_block: right_block_left, }),
                        Some(gaps::GapBetween::TwoBlocks { left_block: left_block_right, right_block: right_block_right, }),
                    ) => {
                        assert_eq!(right_block_left, removed_block_id);
                        #[allow(clippy::drop_non_drop)]
                        drop(right_block_left);
                        assert_eq!(left_block_right, removed_block_id);
                        #[allow(clippy::drop_non_drop)]
                        drop(left_block_right);
                        let moved_space_key = self.gaps_index.insert(
                            space_key_left.space_available() + space_key_right.space_available(),
                            gaps::GapBetween::TwoBlocks {
                                left_block: removed_block_id.clone(),
                                right_block: right_block_right.clone(),
                            },
                        );
                        let block_offset = self.blocks_index.with_mut(&left_block_left, |block_entry| {
                            block_entry.environs.right = RightEnvirons::Block { block_id: removed_block_id.clone(), };
                            block_entry.offset
                                + data_size_block_min
                                + block_entry.header.block_size as u64
                        }).unwrap();
                        self.blocks_index.update_env_left(&right_block_right, LeftEnvirons::Space { space_key: moved_space_key, });
                        self.blocks_index.with_mut(&removed_block_id, |block_entry| {
                            block_entry.offset = block_offset;
                            block_entry.environs.left = LeftEnvirons::Block { block_id: left_block_left.clone(), };
                            block_entry.environs.right = RightEnvirons::Space { space_key: moved_space_key, };
                        }).unwrap();
                        defrag_op = self.make_defrag_op(moved_space_key, right_block_right.clone());
                        Some(moved_space_key)
                    },
                },

            Environs { left: LeftEnvirons::Space { space_key, }, right: RightEnvirons::Block { block_id, }, } =>
                match self.gaps_index.remove(&space_key) {
                    None | Some(gaps::GapBetween::StartAndEnd) | Some(gaps::GapBetween::BlockAndEnd { .. }) =>
                        unreachable!(),
                    // before: ^| ... | R | B | ... |$
                    // after:  ^| R | ... | B | ... |$
                    Some(gaps::GapBetween::StartAndBlock { right_block, }) => {
                        assert_eq!(right_block, removed_block_id);
                        #[allow(clippy::drop_non_drop)]
                        drop(right_block);
                        let moved_space_key = self.gaps_index.insert(
                            space_key.space_available(),
                            gaps::GapBetween::TwoBlocks { left_block: removed_block_id.clone(), right_block: block_id.clone(), },
                        );
                        self.blocks_index.update_env_left(&block_id, LeftEnvirons::Space { space_key: moved_space_key, });
                        self.blocks_index.with_mut(&removed_block_id, |block_entry| {
                            block_entry.offset = start_offset;
                            block_entry.environs.left = LeftEnvirons::Start;
                            block_entry.environs.right = RightEnvirons::Space { space_key: moved_space_key, };
                        }).unwrap();
                        defrag_op = self.make_defrag_op(moved_space_key, block_id.clone());
                        Some(moved_space_key)
                    },
                    // before: ^| ... | A | ... | R | B | ... |$
                    // after:  ^| ... | A | R | ... | B | ... |$
                    Some(gaps::GapBetween::TwoBlocks { left_block, right_block, }) => {
                        assert_eq!(right_block, removed_block_id);
                        #[allow(clippy::drop_non_drop)]
                        drop(right_block);
                        let moved_space_key = self.gaps_index.insert(
                            space_key.space_available(),
                            gaps::GapBetween::TwoBlocks {
                                left_block: removed_block_id.clone(),
                                right_block: block_id.clone(),
                            },
                        );
                        let block_offset = self.blocks_index.with_mut(&left_block, |block_entry| {
                            block_entry.environs.right = RightEnvirons::Block { block_id: removed_block_id.clone(), };
                            block_entry.offset
                                + data_size_block_min
                                + block_entry.header.block_size as u64
                        }).unwrap();
                        self.blocks_index.update_env_left(&block_id, LeftEnvirons::Space { space_key: moved_space_key, });
                        self.blocks_index.with_mut(&removed_block_id, |block_entry| {
                            block_entry.offset = block_offset;
                            block_entry.environs.left = LeftEnvirons::Block { block_id: left_block.clone(), };
                            block_entry.environs.right = RightEnvirons::Space { space_key: moved_space_key, };
                        }).unwrap();
                        defrag_op = self.make_defrag_op(moved_space_key, block_id.clone());
                        Some(moved_space_key)
                    },
                },

        };

        let block_entry = self.blocks_index.get_mut(&removed_block_id).unwrap();
        DeleteBlockTaskDoneDefragOp::Perform(DeleteBlockTaskDoneDefragPerform {
            block_offset: block_entry.offset,
            defrag_op,
            freed_space_key,
        })
    }

    pub fn block_get(&mut self) -> BlockGet<'_> {
        BlockGet { blocks_index: &mut self.blocks_index, }
    }

    pub fn next_block_id_from(&self, offset: block::Id) -> Option<block::Id> {
        self.blocks_index.next_block_id_from(offset)
    }

    pub fn is_last_block(&self, block_id: &block::Id) -> bool {
        if let Some(block_entry) = self.blocks_index.get(block_id) {
            match block_entry.environs.right {
                RightEnvirons::End =>
                    true,
                RightEnvirons::Space { ref space_key, } if self.gaps_index.is_last(space_key) =>
                    true,
                RightEnvirons::Space { .. } |
                RightEnvirons::Block { .. } =>
                    false,
            }
        } else {
            false
        }
    }

    fn make_defrag_op(&mut self, space_key_left: SpaceKey, moving_block_id: block::Id) -> DefragOp {
        let defrag_gaps = self.blocks_index.with_mut(&moving_block_id, |block_entry| {
            match block_entry.environs.right {
                RightEnvirons::End | RightEnvirons::Block { .. } =>
                    DefragGaps::OnlyLeft { block_offset: block_entry.offset, space_key_left, },
                RightEnvirons::Space { space_key: space_key_right, } =>
                    DefragGaps::Both { block_offset: block_entry.offset, space_key_left, space_key_right, },
            }
        }).unwrap();
        DefragOp::Queue { defrag_gaps, moving_block_id, }
    }
}

pub struct BlockGet<'a> {
    blocks_index: &'a mut blocks::Index,
}

impl<'a> super::BlockGet for BlockGet<'a> {
    fn by_id<'s>(&'s mut self, block_id: &block::Id) -> Option<&'s mut BlockEntry> {
        self.blocks_index.get_mut(block_id)
    }
}


pub struct Builder {
    storage_layout: storage::Layout,
    blocks_index: blocks::Index,
    gaps_index: gaps::Index,
    tracker: Option<BlocksTracker>,
}

struct BlocksTracker {
    prev_block_id: block::Id,
    prev_block_left_env: LeftEnvirons,
    prev_block_offset: u64,
    prev_block_size: usize,
    max_block_id: block::Id,
}

impl Builder {
    pub fn new(storage_layout: storage::Layout) -> Builder {
        Builder {
            storage_layout,
            blocks_index: blocks::Index::new(),
            gaps_index: gaps::Index::new(),
            tracker: None,
        }
    }

    pub fn storage_layout(&self) -> &storage::Layout {
        &self.storage_layout
    }

    pub fn push_block(&mut self, offset: u64, block_header: storage::BlockHeader) -> DefragOp {
        let (left, max_block_id) = match self.tracker.take() {
            None => {
                assert!(offset >= self.storage_layout.wheel_header_size as u64);
                match (offset - self.storage_layout.wheel_header_size as u64) as usize {
                    0 =>
                        (LeftEnvirons::Start, block_header.block_id.clone()),
                    space_available => {
                        let space_key = self.gaps_index.insert(
                            space_available,
                            gaps::GapBetween::StartAndBlock {
                                right_block: block_header.block_id.clone(),
                            },
                        );
                        (LeftEnvirons::Space { space_key, }, block_header.block_id.clone())
                    },
                }
            },
            Some(tracker) => {
                let prev_block_end_offset = tracker.prev_block_offset + tracker.prev_block_size as u64;
                assert!(offset >= prev_block_end_offset);
                match (offset - prev_block_end_offset) as usize {
                    0 => {
                        self.blocks_index.with_mut(&tracker.prev_block_id, |block_entry| {
                            block_entry.environs.right =
                                RightEnvirons::Block { block_id: block_header.block_id.clone(), };
                        }).unwrap();
                        (
                            LeftEnvirons::Block { block_id: tracker.prev_block_id.clone(), },
                            block_header.block_id.clone().max(tracker.max_block_id),
                        )
                    },
                    space_available => {
                        let space_key = self.gaps_index.insert(
                            space_available,
                            gaps::GapBetween::TwoBlocks {
                                left_block: tracker.prev_block_id.clone(),
                                right_block: block_header.block_id.clone(),
                            },
                        );
                        self.blocks_index.with_mut(&tracker.prev_block_id, |block_entry| {
                            block_entry.environs.right = RightEnvirons::Space { space_key, };
                        }).unwrap();
                        (
                            LeftEnvirons::Space { space_key, },
                            block_header.block_id.clone().max(tracker.max_block_id),
                        )
                    },
                }
            },
        };
        let defrag_op = match self.tracker {
            Some(BlocksTracker { prev_block_left_env: LeftEnvirons::Space { space_key: space_key_left, }, ref prev_block_id, prev_block_offset, .. }) =>
                match left {
                    LeftEnvirons::Start =>
                        unreachable!(),
                    LeftEnvirons::Space { space_key: space_key_right, } =>
                        DefragOp::Queue {
                            defrag_gaps: DefragGaps::Both { block_offset: prev_block_offset, space_key_left, space_key_right, },
                            moving_block_id: prev_block_id.clone(),
                        },
                    LeftEnvirons::Block { .. } =>
                        DefragOp::Queue {
                            defrag_gaps: DefragGaps::OnlyLeft { block_offset: prev_block_offset, space_key_left, },
                            moving_block_id: prev_block_id.clone(),
                        },
                },
            Some(..) | None =>
                DefragOp::None,
        };

        self.tracker = Some(BlocksTracker {
            prev_block_id: block_header.block_id.clone(),
            prev_block_left_env: left.clone(),
            prev_block_offset: offset,
            prev_block_size: self.storage_layout.data_size_block_min()
                + block_header.block_size as usize,
            max_block_id,
        });
        self.blocks_index.insert(
            block_header.block_id.clone(),
            BlockEntry {
                offset,
                header: block_header,
                environs: Environs { left, right: RightEnvirons::End, },
                tasks_head: Default::default(),
                cached: None,
            },
        );

        defrag_op
    }

    pub fn finish(mut self, size_bytes_total: usize) -> (DefragOp, Schema) {
        let total_service_size = self.storage_layout.service_size_min();
        let (defrag_op, next_block_id) = match self.tracker {
            None => {
                if size_bytes_total > total_service_size {
                    let space_available = size_bytes_total - total_service_size;
                    self.gaps_index.insert(
                        space_available,
                        gaps::GapBetween::StartAndEnd,
                    );
                }
                (DefragOp::None, block::Id::init())
            },
            Some(tracker) => {
                let space_total = size_bytes_total as u64 - self.storage_layout.terminator_tag_size as u64;
                let prev_block_end_offset = tracker.prev_block_offset + tracker.prev_block_size as u64;
                assert!(space_total >= prev_block_end_offset);
                match space_total - prev_block_end_offset {
                    0 =>
                    // set nothing: there should be already RightEnvirons::End set
                        (
                            match tracker.prev_block_left_env {
                                LeftEnvirons::Start | LeftEnvirons::Block { .. } =>
                                    DefragOp::None,
                                LeftEnvirons::Space { space_key: space_key_left, } =>
                                    DefragOp::Queue {
                                        defrag_gaps: DefragGaps::OnlyLeft { block_offset: tracker.prev_block_offset, space_key_left, },
                                        moving_block_id: tracker.prev_block_id,
                                    },
                            },
                            tracker.max_block_id.next(),
                        ),
                    space_available => {
                        let space_key = self.gaps_index.insert(
                            space_available as usize,
                            gaps::GapBetween::BlockAndEnd {
                                left_block: tracker.prev_block_id.clone(),
                            },
                        );
                        self.blocks_index.with_mut(&tracker.prev_block_id, |block_entry| {
                            block_entry.environs.right = RightEnvirons::Space {
                                space_key,
                            };
                        }).unwrap();
                        (
                            match tracker.prev_block_left_env {
                                LeftEnvirons::Start | LeftEnvirons::Block { .. } =>
                                    DefragOp::None,
                                LeftEnvirons::Space { space_key: space_key_left, } =>
                                    DefragOp::Queue {
                                        defrag_gaps: DefragGaps::Both {
                                            block_offset: tracker.prev_block_offset,
                                            space_key_left,
                                            space_key_right: space_key,
                                        },
                                        moving_block_id: tracker.prev_block_id,
                                    },
                            },
                            tracker.max_block_id.next(),
                        )
                    },
                }
            },
        };

        let schema = Schema {
            next_block_id,
            storage_layout: self.storage_layout,
            blocks_index: self.blocks_index,
            gaps_index: self.gaps_index,
            read_block_cache_hits: 0,
            read_block_cache_misses: 0,
        };
        (defrag_op, schema)
    }
}

#[cfg(test)]
mod tests {
    use alloc_pool::{
        bytes::{
            Bytes,
            BytesMut,
        },
    };

    use super::{
        block,
        storage,
        Builder,
        BlockEntry,
        SpaceKey,
        Environs,
        LeftEnvirons,
        RightEnvirons,
        Schema,
        WriteBlockOp,
        WriteBlockPerform,
        DefragOp,
        DefragGaps,
        WriteBlockTaskOp,
        ReadBlockOp,
        ReadBlockPerform,
        DeleteBlockOp,
        DeleteBlockPerform,
        DeleteBlockTaskDoneOp,
        DeleteBlockTaskDonePerform,
        DeleteBlockTaskDoneDefragOp,
        DeleteBlockTaskDoneDefragPerform,
    };

    fn init() -> Schema {
        let storage_layout = storage::Layout::calculate();
        Builder::new(storage_layout).finish(160).1
    }

    fn sample_hello_world() -> Bytes {
        let mut block_bytes_mut = BytesMut::new_detached(Vec::new());
        block_bytes_mut.extend("hello, world!".as_bytes().iter().cloned());
        block_bytes_mut.freeze()
    }

    #[test]
    fn process_write_block_request() {
        let mut schema = init();
        assert_eq!(schema.gaps_index.space_total(), 134);

        let op = schema.process_write_block_request(&sample_hello_world(), None);
        assert!(matches!(op, WriteBlockOp::Perform(WriteBlockPerform {
            defrag_op: DefragOp::None,
            task_op: WriteBlockTaskOp {
                block_id,
                block_offset: 18,
            },
            ..
        }) if block_id == block::Id::init()));

        assert_eq!(schema.next_block_id, block::Id::init().next());
        assert!(matches!(
            schema.blocks_index.get(&block::Id::init()),
            Some(&BlockEntry {
                offset: 18,
                header: storage::BlockHeader {
                    ref block_id,
                    block_size: 13,
                    ..
                },
                environs: Environs {
                    left: LeftEnvirons::Start,
                    right: RightEnvirons::Space { space_key: SpaceKey { space_available: 73, serial: 2, }, },
                },
                ..
            }) if block_id == &block::Id::init()
        ));
        assert_eq!(schema.blocks_index.get(&block::Id::init().next()), None);
        assert_eq!(schema.gaps_index.space_total(), 73);

        let op = schema.process_write_block_request(&sample_hello_world(), None);
        assert!(matches!(op, WriteBlockOp::Perform(
            WriteBlockPerform {
                defrag_op: DefragOp::None,
                task_op: WriteBlockTaskOp {
                    block_id,
                    block_offset: 79,
                },
                ..
            },
        ) if block_id == block::Id::init().next()));

        assert_eq!(schema.next_block_id, block::Id::init().next().next());
        assert!(matches!(
            schema.blocks_index.get(&block::Id::init()),
            Some(&BlockEntry {
                offset: 18,
                header: storage::BlockHeader {
                    block_id: ref block_id_a,
                    block_size: 13,
                    ..
                },
                environs: Environs {
                    left: LeftEnvirons::Start,
                    right: RightEnvirons::Block { block_id: ref block_id_b, },
                },
                ..
            }) if block_id_a == &block::Id::init() && block_id_b == &block::Id::init().next()
        ));
        assert!(matches!(
            schema.blocks_index.get(&block::Id::init().next()),
            Some(&BlockEntry {
                offset: 79,
                header: storage::BlockHeader {
                    block_id: ref block_id_a,
                    block_size: 13,
                    ..
                },
                environs: Environs {
                    left: LeftEnvirons::Block { block_id: ref block_id_b, },
                    right: RightEnvirons::Space { space_key: SpaceKey { space_available: 12, serial: 3, }, },
                },
                ..
            }) if block_id_a == &block::Id::init().next() && block_id_b == &block::Id::init()
        ));
        assert_eq!(schema.blocks_index.get(&block::Id::init().next().next()), None);
        assert_eq!(schema.gaps_index.space_total(), 12);

        let op = schema.process_write_block_request(&sample_hello_world(), None);
        assert!(matches!(op, WriteBlockOp::ReplyNoSpaceLeft));
    }

    #[test]
    fn process_write_read_block_requests() {
        let mut schema = init();
        assert_eq!(schema.gaps_index.space_total(), 134);

        let op = schema.process_read_block_request(&block::Id::init());
        assert!(matches!(op, ReadBlockOp::NotFound));

        let op = schema.process_write_block_request(&sample_hello_world(), None);
        assert!(matches!(op, WriteBlockOp::Perform(
            WriteBlockPerform {
                defrag_op: DefragOp::None,
                task_op: WriteBlockTaskOp {
                    ref block_id,
                    block_offset: 18,
                },
                ..
            },
        ) if block_id == &block::Id::init()));

        assert_eq!(schema.next_block_id, block::Id::init().next());
        assert!(matches!(
            schema.blocks_index.get(&block::Id::init()),
            Some(&BlockEntry {
                offset: 18,
                header: storage::BlockHeader {
                    ref block_id,
                    block_size: 13,
                    ..
                },
                environs: Environs {
                    left: LeftEnvirons::Start,
                    right: RightEnvirons::Space { space_key: SpaceKey { space_available: 73, serial: 2, }, },
                },
                ..
            }) if block_id == &block::Id::init()
        ));
        assert_eq!(schema.blocks_index.get(&block::Id::init().next()), None);
        assert_eq!(schema.gaps_index.space_total(), 73);

        let op = schema.process_read_block_request(&block::Id::init());
        assert!(matches!(op, ReadBlockOp::Perform(ReadBlockPerform { .. })));
    }

    #[test]
    fn process_delete_block_request() {
        let mut schema = init();
        assert_eq!(schema.gaps_index.space_total(), 134);

        let op = schema.process_write_block_request(&sample_hello_world(), None);
        assert!(matches!(op, WriteBlockOp::Perform(..)));
        let op = schema.process_write_block_request(&sample_hello_world(), None);
        assert!(matches!(op, WriteBlockOp::Perform(..)));

        let op = schema.process_delete_block_request(&block::Id::init());
        assert!(matches!(op, DeleteBlockOp::Perform(DeleteBlockPerform { .. })));

        assert!(matches!(
            schema.blocks_index.get(&block::Id::init()),
            Some(&BlockEntry {
                offset: 18,
                header: storage::BlockHeader {
                    block_id: ref block_id_a,
                    block_size: 13,
                    ..
                },
                environs: Environs {
                    left: LeftEnvirons::Start,
                    right: RightEnvirons::Block { block_id: ref block_id_b, },
                },
                ..
            }) if block_id_a == &block::Id::init() && block_id_b == &block::Id::init().next()
        ));

        let op = schema.process_delete_block_task_done(block::Id::init());

        assert!(matches!(op, DeleteBlockTaskDoneOp::Perform(DeleteBlockTaskDonePerform {
            defrag_op: DefragOp::Queue {
                defrag_gaps: DefragGaps::Both {
                    space_key_left: SpaceKey { space_available: 61, serial: 4, },
                    space_key_right: SpaceKey { space_available: 12, serial: 3, },
                    ..
                },
                moving_block_id,
            },
            ..
        }) if moving_block_id == block::Id::init().next()));

        assert_eq!(schema.blocks_index.get(&block::Id::init()), None);

        assert!(matches!(
            schema.blocks_index.get(&block::Id::init().next()),
            Some(&BlockEntry {
                offset: 79,
                header: storage::BlockHeader {
                    ref block_id,
                    block_size: 13,
                    ..
                },
                environs: Environs {
                    left: LeftEnvirons::Space { space_key: SpaceKey { space_available: 61, serial: 4, }, },
                    right: RightEnvirons::Space { space_key: SpaceKey { space_available: 12, serial: 3, }, },
                },
                ..
            }) if block_id == &block::Id::init().next()
        ));
        assert_eq!(schema.gaps_index.space_total(), 73);

        let op = schema.process_write_block_request(&sample_hello_world(), None);
        assert!(matches!(op, WriteBlockOp::Perform(..)));

        let op = schema.process_delete_block_request(&block::Id::init().next());
        assert!(matches!(op, DeleteBlockOp::Perform(DeleteBlockPerform { .. })));

        assert!(matches!(
            schema.blocks_index.get(&block::Id::init().next()),
            Some(&BlockEntry {
                offset: 79,
                header: storage::BlockHeader {
                    block_id: ref block_id_a,
                    block_size: 13,
                    ..
                },
                environs: Environs {
                    left: LeftEnvirons::Block { block_id: ref block_id_b, },
                    right: RightEnvirons::Space { space_key: SpaceKey { space_available: 12, serial: 3, }, },
                },
                ..
            }) if block_id_a == &block::Id::init().next() && block_id_b == &block::Id::init().next().next()
        ));

        let op = schema.process_delete_block_task_done(block::Id::init().next());
        assert!(matches!(op, DeleteBlockTaskDoneOp::Perform(DeleteBlockTaskDonePerform { defrag_op: DefragOp::None, .. })));

        assert_eq!(schema.blocks_index.get(&block::Id::init().next()), None);
        assert!(matches!(
            schema.blocks_index.get(&block::Id::init().next().next()),
            Some(&BlockEntry {
                offset: 18,
                header: storage::BlockHeader {
                    ref block_id,
                    block_size: 13,
                    ..
                },
                environs: Environs {
                    left: LeftEnvirons::Start,
                    right: RightEnvirons::Space { space_key: SpaceKey { space_available: 73, serial: 5, }, },
                },
                ..
            }) if block_id == &block::Id::init().next().next()
        ));
        assert_eq!(schema.gaps_index.space_total(), 73);
    }

    #[test]
    fn process_delete_block_task_done_defrag() {
        let mut schema = init();
        assert_eq!(schema.gaps_index.space_total(), 134);

        let op = schema.process_write_block_request(&sample_hello_world(), Some(0));
        assert!(matches!(op, WriteBlockOp::Perform(..)));
        let op = schema.process_write_block_request(&sample_hello_world(), Some(0));
        assert!(matches!(op, WriteBlockOp::Perform(..)));

        let op = schema.process_delete_block_request(&block::Id::init());
        assert!(matches!(op, DeleteBlockOp::Perform(DeleteBlockPerform { .. })));

        let op = schema.process_delete_block_task_done(block::Id::init());
        assert!(matches!(op, DeleteBlockTaskDoneOp::Perform(DeleteBlockTaskDonePerform {
            defrag_op: DefragOp::Queue {
                defrag_gaps: DefragGaps::Both {
                    space_key_left: SpaceKey { space_available: 61, serial: 4, },
                    space_key_right: SpaceKey { space_available: 12, serial: 3, },
                    ..
                },
                moving_block_id,
            },
            ..
        }) if moving_block_id == block::Id::init().next()));

        // defrag delete
        assert_eq!(schema.gaps_index.space_total(), 73);

        let op = schema.process_delete_block_request(&block::Id::init().next());
        assert!(matches!(op, DeleteBlockOp::Perform(DeleteBlockPerform { .. })));

        let op = schema.process_delete_block_task_done_defrag(block::Id::init().next());
        assert!(matches!(op, DeleteBlockTaskDoneDefragOp::Perform(DeleteBlockTaskDoneDefragPerform {
            block_offset: 18,
            ..
        })));

        assert!(matches!(
            schema.blocks_index.get(&block::Id::init().next()),
            Some(&BlockEntry {
                offset: 18,
                header: storage::BlockHeader {
                    block_id: ref block_id_a,
                    block_size: 13,
                    ..
                },
                environs: Environs {
                    left: LeftEnvirons::Start,
                    right: RightEnvirons::Space { space_key: SpaceKey { space_available: 73, serial: 5, }, },
                },
                ..
            }) if block_id_a == &block::Id::init().next()
        ));

        assert_eq!(schema.gaps_index.space_total(), 73);
    }
}
