use super::{
    gaps,
    block,
    blocks,
    storage,
    SpaceKey,
    TasksHead,
    BlockEntry,
    Environs,
    LeftEnvirons,
    RightEnvirons,
};

use crate::Info;

#[derive(Debug)]
pub struct Schema {
    next_block_id: block::Id,
    storage_layout: storage::Layout,
    blocks_index: blocks::Index,
    gaps_index: gaps::Index,
}

#[derive(Debug)]
pub enum WriteBlockOp<'a> {
    Perform(WriteBlockPerform<'a>),
    QueuePendingDefrag,
    ReplyNoSpaceLeft,
}

#[derive(Debug)]
pub struct WriteBlockPerform<'a> {
    pub defrag_op: DefragOp,
    pub task_op: WriteBlockTaskOp,
    pub tasks_head: &'a mut TasksHead,
}

#[derive(Clone, PartialEq, Debug)]
pub enum DefragOp {
    None,
    Queue { free_space_offset: u64, space_key: SpaceKey, },
}

#[derive(Clone, PartialEq, Debug)]
pub struct WriteBlockTaskOp {
    pub block_id: block::Id,
    pub block_offset: u64,
}

#[derive(Debug)]
pub enum ReadBlockOp<'a> {
    Perform(ReadBlockPerform<'a>),
    Cached { block_bytes: block::Bytes, },
    NotFound,
}

#[derive(Debug)]
pub struct ReadBlockPerform<'a> {
    pub block_offset: u64,
    pub block_header: &'a storage::BlockHeader,
    pub tasks_head: &'a mut TasksHead,
}

#[derive(Debug)]
pub enum DeleteBlockOp<'a> {
    Perform(DeleteBlockPerform<'a>),
    NotFound,
}

#[derive(Debug)]
pub struct DeleteBlockPerform<'a> {
    pub block_offset: u64,
    pub tasks_head: &'a mut TasksHead,
}

#[derive(Debug)]
pub enum ReadBlockTaskDoneOp<'a> {
    Perform(ReadBlockTaskDonePerform<'a>),
}

#[derive(Debug)]
pub struct ReadBlockTaskDonePerform<'a> {
    pub block_offset: u64,
    pub block_bytes_cached: &'a mut Option<block::Bytes>,
    pub tasks_head: &'a mut TasksHead,
}

#[derive(Debug)]
pub enum DeleteBlockTaskDoneOp {
    Perform(DeleteBlockTaskDonePerform),
}

#[derive(Debug)]
pub struct DeleteBlockTaskDonePerform {
    pub defrag_op: DefragOp,
    pub block_entry: BlockEntry,
}

#[derive(Debug)]
pub enum DeleteBlockTaskDoneDefragOp<'a> {
    Perform(DeleteBlockTaskDoneDefragPerform<'a>),
}

#[derive(Debug)]
pub struct DeleteBlockTaskDoneDefragPerform<'a> {
    pub block_offset: u64,
    pub block_bytes: block::Bytes,
    pub tasks_head: &'a mut TasksHead,
    pub defrag_op: DefragOp,
}

impl Schema {
    pub fn new(storage_layout: storage::Layout) -> Schema {
        Schema {
            next_block_id: block::Id::init(),
            storage_layout,
            blocks_index: blocks::Index::new(),
            gaps_index: gaps::Index::new(),
        }
    }

    pub fn initialize_empty(&mut self, size_bytes_total: usize) {
        let total_service_size = self.storage_layout.service_size_min();
        if size_bytes_total > total_service_size {
            let space_available = size_bytes_total - total_service_size;
            self.gaps_index.insert(
                space_available,
                gaps::GapBetween::StartAndEnd,
            );
        }
    }

    pub fn storage_layout(&self) -> &storage::Layout {
        &self.storage_layout
    }

    pub fn info(&self) -> Info {
        let blocks_count = self.blocks_index.count();
        let service_bytes_used = self.storage_layout.service_size_min()
            + (blocks_count * self.storage_layout.data_size_block_min());
        let data_bytes_used = self.blocks_index.blocks_total_size();
        let bytes_free = self.gaps_index.space_total();
        Info {
            blocks_count,
            service_bytes_used,
            data_bytes_used,
            bytes_free,
            wheel_size_bytes: service_bytes_used
                + data_bytes_used
                + bytes_free,
        }
    }

    pub fn process_write_block_request<'a>(
        &'a mut self,
        block_bytes: &block::Bytes,
        defrag_pending_bytes: Option<usize>,
    )
        -> WriteBlockOp<'a>
    {
        let block_id = self.next_block_id.clone();
        self.next_block_id = self.next_block_id.next();

        let mut defrag_op = DefragOp::None;

        let space_required = block_bytes.len()
            + self.storage_layout.data_size_block_min();

        let blocks_index = &self.blocks_index;
        let block_offset = match self.gaps_index.allocate(space_required, defrag_pending_bytes, |block_id| blocks_index.get(block_id)) {

            // before: ^| ....... | A | ... |$
            // after:  ^| R | ... | A | ... |$
            Ok(gaps::Allocated::Success { space_available, between: gaps::GapBetween::StartAndBlock { right_block, }, }) => {
                let block_offset = self.storage_layout.wheel_header_size as u64;
                let right_block_id = right_block.block_id.clone();

                let space_left = space_available - space_required;
                let (self_env, right_env) = if space_left > 0 {
                    let space_key = self.gaps_index.insert(
                        space_left,
                        gaps::GapBetween::TwoBlocks {
                            left_block: block_id.clone(),
                            right_block: right_block_id.clone(),
                        },
                    );
                    let free_space_offset = block_offset + space_required as u64;
                    defrag_op = DefragOp::Queue { free_space_offset, space_key, };
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
                            block_size: block_bytes.len(),
                            ..Default::default()
                        },
                        block_bytes: Some(block_bytes.clone()),
                        environs: Environs {
                            left: LeftEnvirons::Start,
                            right: self_env,
                        },
                        tasks_head: Default::default(),
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
                let left_block_id = left_block.block_id.clone();
                let right_block_id = right_block.block_id.clone();

                let space_left = space_available - space_required;
                let (self_env, left_env, right_env) = if space_left > 0 {
                    let space_key = self.gaps_index.insert(
                        space_left,
                        gaps::GapBetween::TwoBlocks {
                            left_block: block_id.clone(),
                            right_block: right_block_id.clone(),
                        },
                    );
                    let free_space_offset = block_offset + space_required as u64;
                    defrag_op = DefragOp::Queue { free_space_offset, space_key, };
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
                            block_size: block_bytes.len(),
                            ..Default::default()
                        },
                        block_bytes: Some(block_bytes.clone()),
                        environs: Environs {
                            left: LeftEnvirons::Block { block_id: left_block_id.clone(), },
                            right: self_env,
                        },
                        tasks_head: Default::default(),
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
                let left_block_id = left_block.block_id.clone();

                let space_left = space_available - space_required;
                let (self_env, left_env) = if space_left > 0 {
                    let space_key = self.gaps_index.insert(
                        space_left,
                        gaps::GapBetween::BlockAndEnd {
                            left_block: block_id.clone(),
                        },
                    );
                    (
                        RightEnvirons::Space { space_key, },
                        RightEnvirons::Block { block_id: block_id.clone(), },
                    )
                } else {
                    (
                        RightEnvirons::End,
                        RightEnvirons::Block { block_id: block_id.clone(), },
                    )
                };
                self.blocks_index.insert(
                    block_id.clone(),
                    BlockEntry {
                        offset: block_offset,
                        header: storage::BlockHeader {
                            block_id: block_id.clone(),
                            block_size: block_bytes.len(),
                            ..Default::default()
                        },
                        block_bytes: Some(block_bytes.clone()),
                        environs: Environs {
                            left: LeftEnvirons::Block { block_id: left_block_id.clone(), },
                            right: self_env,
                        },
                        tasks_head: Default::default(),
                    },
                );
                self.blocks_index.update_env_right(&left_block_id, left_env);
                block_offset
            },

            // before: ^| ....... |$
            // after:  ^| A | ... |$
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
                            block_size: block_bytes.len(),
                            ..Default::default()
                        },
                        block_bytes: Some(block_bytes.clone()),
                        environs,
                        tasks_head: Default::default(),
                    },
                );
                block_offset
            },

            Ok(gaps::Allocated::PendingDefragmentation) =>
                return WriteBlockOp::QueuePendingDefrag,

            Err(gaps::Error::NoSpaceLeft) =>
                return WriteBlockOp::ReplyNoSpaceLeft,

        };

        WriteBlockOp::Perform(
            WriteBlockPerform {
                tasks_head: &mut self.blocks_index.get_mut(&block_id).unwrap().tasks_head,
                defrag_op,
                task_op: WriteBlockTaskOp {
                    block_id,
                    block_offset,
                },
            },
        )
    }

    pub fn process_read_block_request<'a>(&'a mut self, block_id: &block::Id) -> ReadBlockOp<'a> {
        match self.blocks_index.get_mut(block_id) {
            Some(block_entry) =>
                if let Some(ref block_bytes) = block_entry.block_bytes {
                    ReadBlockOp::Cached { block_bytes: block_bytes.clone(), }
                } else {
                    ReadBlockOp::Perform(ReadBlockPerform {
                        block_offset: block_entry.offset,
                        block_header: &block_entry.header,
                        tasks_head: &mut block_entry.tasks_head,
                    })
                },
            None =>
                ReadBlockOp::NotFound,
        }
    }

    pub fn process_delete_block_request<'a>(&'a mut self, block_id: &block::Id) -> DeleteBlockOp<'a> {
        match self.blocks_index.get_mut(&block_id) {
            Some(block_entry) =>
                DeleteBlockOp::Perform(DeleteBlockPerform {
                    block_offset: block_entry.offset,
                    tasks_head: &mut block_entry.tasks_head,
                }),
            None =>
                DeleteBlockOp::NotFound,
        }
    }

    pub fn process_write_block_task_done(&mut self, written_block_id: &block::Id) {
        self.blocks_index.with_mut(written_block_id, |block_entry| {
            block_entry.block_bytes = None;
        }).unwrap();
    }

    pub fn process_read_block_task_done<'a>(&'a mut self, read_block_id: &block::Id) -> ReadBlockTaskDoneOp<'a> {
        let block_entry = self.blocks_index.get_mut(read_block_id).unwrap();
        ReadBlockTaskDoneOp::Perform(ReadBlockTaskDonePerform {
            block_offset: block_entry.offset,
            block_bytes_cached: &mut block_entry.block_bytes,
            tasks_head: &mut block_entry.tasks_head,
        })
    }

    pub fn process_delete_block_task_done(&mut self, removed_block_id: block::Id) -> DeleteBlockTaskDoneOp {
        let block_entry = self.blocks_index.remove(&removed_block_id).unwrap();
        let mut defrag_op = DefragOp::None;

        match &block_entry.environs {

            // before: ^| ... | R | ... |$
            // after:  ^| ............. |$
            Environs { left: LeftEnvirons::Start, right: RightEnvirons::End, } => {
                assert_eq!(self.gaps_index.space_total(), 0);
                let space_available = block_entry.header.block_size
                    + self.storage_layout.data_size_block_min();
                self.gaps_index.insert(space_available, gaps::GapBetween::StartAndEnd);
            },

            Environs { left: LeftEnvirons::Start, right: RightEnvirons::Space { space_key, }, } =>
                match self.gaps_index.remove(&space_key) {
                    None | Some(gaps::GapBetween::StartAndEnd) | Some(gaps::GapBetween::StartAndBlock { .. }) =>
                        unreachable!(),
                    // before: ^| R | ... | A | ... |$
                    // after:  ^| ........| A | ... |$
                    Some(gaps::GapBetween::TwoBlocks { left_block, right_block, }) => {
                        assert_eq!(left_block, removed_block_id);
                        let space_available = block_entry.header.block_size
                            + self.storage_layout.data_size_block_min()
                            + space_key.space_available();
                        let space_key = self.gaps_index.insert(
                            space_available,
                            gaps::GapBetween::StartAndBlock { right_block: right_block.clone(), },
                        );
                        self.blocks_index.update_env_left(&right_block, LeftEnvirons::Start);
                        assert_eq!(block_entry.offset, self.storage_layout.wheel_header_size as u64);
                        defrag_op = DefragOp::Queue { free_space_offset: block_entry.offset, space_key, };
                    },
                    // before: ^| R | ... |$
                    // after:  ^| ....... |$
                    Some(gaps::GapBetween::BlockAndEnd { left_block, }) => {
                        assert_eq!(left_block, removed_block_id);
                        let space_available = block_entry.header.block_size
                            + self.storage_layout.data_size_block_min()
                            + space_key.space_available();
                        self.gaps_index.insert(space_available, gaps::GapBetween::StartAndEnd);
                    },
                },

            // before: ^| R | | A | ... |$
            // after:  ^| ... | A | ... |$
            Environs {
                left: LeftEnvirons::Start,
                right: RightEnvirons::Block { block_id, },
            } => {
                let space_available = block_entry.header.block_size
                    + self.storage_layout.data_size_block_min();
                let space_key = self.gaps_index.insert(space_available, gaps::GapBetween::StartAndBlock { right_block: block_id.clone(), });
                self.blocks_index.update_env_left(&block_id, LeftEnvirons::Space { space_key, });
                assert_eq!(block_entry.offset, self.storage_layout.wheel_header_size as u64);
                defrag_op = DefragOp::Queue { free_space_offset: block_entry.offset, space_key, };
            },

            Environs {
                left: LeftEnvirons::Space { space_key, },
                right: RightEnvirons::End,
            } =>
                match self.gaps_index.remove(&space_key) {
                    None | Some(gaps::GapBetween::StartAndEnd) | Some(gaps::GapBetween::BlockAndEnd { .. }) =>
                        unreachable!(),
                    // before: ^| ... | A | ... | R |$
                    // after:  ^| ... | A | ....... |$
                    Some(gaps::GapBetween::TwoBlocks { left_block, right_block, }) => {
                        assert_eq!(right_block, removed_block_id);
                        let space_available = block_entry.header.block_size
                            + self.storage_layout.data_size_block_min()
                            + space_key.space_available();
                        self.gaps_index.insert(
                            space_available,
                            gaps::GapBetween::BlockAndEnd { left_block: left_block.clone(), },
                        );
                        self.blocks_index.update_env_right(&left_block, RightEnvirons::End);
                    },
                    // before: ^| ... | R |$
                    // after:  ^| ....... |$
                    Some(gaps::GapBetween::StartAndBlock { right_block, }) => {
                        assert_eq!(right_block, removed_block_id);
                        let space_available = block_entry.header.block_size
                            + self.storage_layout.data_size_block_min()
                            + space_key.space_available();
                        self.gaps_index.insert(space_available, gaps::GapBetween::StartAndEnd);
                    },
                },

            Environs {
                left: LeftEnvirons::Space { space_key: space_key_left, },
                right: RightEnvirons::Space { space_key: space_key_right, },
            } =>
                match (self.gaps_index.remove(&space_key_left), self.gaps_index.remove(&space_key_right)) {
                    (None, _) | (_, None) |
                    (Some(gaps::GapBetween::StartAndEnd), _) | (_, Some(gaps::GapBetween::StartAndEnd)) |
                    (Some(gaps::GapBetween::BlockAndEnd { .. }), _) | (_, Some(gaps::GapBetween::StartAndBlock { .. })) =>
                        unreachable!(),
                    // before: ^| ... | R | ... |$
                    // after:  ^| ..............|$
                    (
                        Some(gaps::GapBetween::StartAndBlock { right_block: right_block_left, }),
                        Some(gaps::GapBetween::BlockAndEnd { left_block: left_block_right, }),
                    ) => {
                        assert_eq!(right_block_left, removed_block_id);
                        assert_eq!(left_block_right, removed_block_id);
                        let space_available = block_entry.header.block_size
                            + self.storage_layout.data_size_block_min()
                            + space_key_left.space_available()
                            + space_key_right.space_available();
                        self.gaps_index.insert(space_available, gaps::GapBetween::StartAndEnd);
                    },
                    // before: ^| ... | R | ... | A | ... |$
                    // after:  ^| ............. | A | ... |$
                    (
                        Some(gaps::GapBetween::StartAndBlock { right_block: right_block_left, }),
                        Some(gaps::GapBetween::TwoBlocks { left_block: left_block_right, right_block: right_block_right, }),
                    ) => {
                        assert_eq!(right_block_left, removed_block_id);
                        assert_eq!(left_block_right, removed_block_id);
                        let space_available = block_entry.header.block_size
                            + self.storage_layout.data_size_block_min()
                            + space_key_left.space_available()
                            + space_key_right.space_available();
                        let space_key = self.gaps_index.insert(
                            space_available,
                            gaps::GapBetween::StartAndBlock { right_block: right_block_right.clone(), },
                        );
                        self.blocks_index.update_env_left(&right_block_right, LeftEnvirons::Space { space_key, });
                        let free_space_offset = self.storage_layout.wheel_header_size as u64;
                        defrag_op = DefragOp::Queue { free_space_offset, space_key, };
                    },
                    // before: ^| ... | A | ... | R | ... |$
                    // after:  ^| ... | A | ............. |$
                    (
                        Some(gaps::GapBetween::TwoBlocks { left_block: left_block_left, right_block: right_block_left, }),
                        Some(gaps::GapBetween::BlockAndEnd { left_block: left_block_right, }),
                    ) => {
                        assert_eq!(right_block_left, removed_block_id);
                        assert_eq!(left_block_right, removed_block_id);
                        let space_available = block_entry.header.block_size
                            + self.storage_layout.data_size_block_min()
                            + space_key_left.space_available()
                            + space_key_right.space_available();
                        let space_key = self.gaps_index.insert(
                            space_available,
                            gaps::GapBetween::BlockAndEnd { left_block: left_block_left.clone(), },
                        );
                        self.blocks_index.update_env_right(&left_block_left, RightEnvirons::Space { space_key, });
                    },
                    // before: ^| ... | A | ... | R | ... | B | ... |$
                    // after:  ^| ... | A | ............. | B | ... |$
                    (
                        Some(gaps::GapBetween::TwoBlocks { left_block: left_block_left, right_block: right_block_left, }),
                        Some(gaps::GapBetween::TwoBlocks { left_block: left_block_right, right_block: right_block_right, }),
                    ) => {
                        assert_eq!(right_block_left, removed_block_id);
                        assert_eq!(left_block_right, removed_block_id);
                        let space_available = block_entry.header.block_size
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
                        let block_entry_left = self.blocks_index.get(&left_block_left).unwrap();
                        let free_space_offset = block_entry_left.offset
                            + self.storage_layout.data_size_block_min() as u64
                            + block_entry_left.header.block_size as u64;
                        defrag_op = DefragOp::Queue { free_space_offset, space_key, };
                    },
                },

            Environs {
                left: LeftEnvirons::Space { space_key, },
                right: RightEnvirons::Block { block_id, },
            } =>
                match self.gaps_index.remove(&space_key) {
                    None | Some(gaps::GapBetween::StartAndEnd) | Some(gaps::GapBetween::BlockAndEnd { .. }) =>
                        unreachable!(),
                    // before: ^| ... | A | ... | R || B | ... |$
                    // after:  ^| ... | A | .........| B | ... |$
                    Some(gaps::GapBetween::TwoBlocks { left_block, right_block, }) => {
                        assert_eq!(right_block, removed_block_id);
                        let space_available = block_entry.header.block_size
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
                        self.blocks_index.update_env_left(&block_id, LeftEnvirons::Space { space_key, });
                        let block_entry_left = self.blocks_index.get(&left_block).unwrap();
                        let free_space_offset = block_entry_left.offset
                            + self.storage_layout.data_size_block_min() as u64
                            + block_entry_left.header.block_size as u64;
                        defrag_op = DefragOp::Queue { free_space_offset, space_key, };
                    },
                    // before: ^| ... | R || B | ... |$
                    // after:  ^| ........ | B | ... |$
                    Some(gaps::GapBetween::StartAndBlock { right_block, }) => {
                        assert_eq!(right_block, removed_block_id);
                        let space_available = block_entry.header.block_size
                            + self.storage_layout.data_size_block_min()
                            + space_key.space_available();
                        let space_key = self.gaps_index.insert(
                            space_available,
                            gaps::GapBetween::StartAndBlock { right_block: block_id.clone(), },
                        );
                        self.blocks_index.update_env_left(&block_id, LeftEnvirons::Space { space_key, });
                        let free_space_offset = self.storage_layout.wheel_header_size as u64;
                        defrag_op = DefragOp::Queue { free_space_offset, space_key, };
                    },
                },

            // before: ^| ... | A || R | ... |$
            // after:  ^| ... | A | ........ |$
            Environs {
                left: LeftEnvirons::Block { block_id, },
                right: RightEnvirons::End,
            } => {
                let space_available = block_entry.header.block_size
                    + self.storage_layout.data_size_block_min();
                let space_key = self.gaps_index.insert(
                    space_available,
                    gaps::GapBetween::BlockAndEnd { left_block: block_id.clone(), },
                );
                self.blocks_index.update_env_right(&block_id, RightEnvirons::Space { space_key, });
            },

            Environs {
                left: LeftEnvirons::Block { block_id, },
                right: RightEnvirons::Space { space_key, },
            } =>
                match self.gaps_index.remove(&space_key) {
                    None | Some(gaps::GapBetween::StartAndEnd) | Some(gaps::GapBetween::StartAndBlock { .. }) =>
                        unreachable!(),
                    // before: ^| ... | A || R | ... | B | ... |$
                    // after:  ^| ... | A | ........ | B | ... |$
                    Some(gaps::GapBetween::TwoBlocks { left_block, right_block, }) => {
                        assert_eq!(left_block, removed_block_id);
                        let space_available = block_entry.header.block_size
                            + self.storage_layout.data_size_block_min()
                            + space_key.space_available();
                        let space_key = self.gaps_index.insert(
                            space_available,
                            gaps::GapBetween::TwoBlocks {
                                left_block: block_id.clone(),
                                right_block: right_block.clone(),
                            },
                        );
                        self.blocks_index.update_env_right(&block_id, RightEnvirons::Space { space_key, });
                        self.blocks_index.update_env_left(&right_block, LeftEnvirons::Space { space_key, });
                        let block_entry_left = self.blocks_index.get(&block_id).unwrap();
                        let free_space_offset = block_entry_left.offset
                            + self.storage_layout.data_size_block_min() as u64
                            + block_entry_left.header.block_size as u64;
                        defrag_op = DefragOp::Queue { free_space_offset, space_key, };
                    },
                    // before: ^| ... | A || R | ... |$
                    // after:  ^| ... | A | ........ |$
                    Some(gaps::GapBetween::BlockAndEnd { left_block, }) => {
                        assert_eq!(left_block, removed_block_id);
                        let space_available = block_entry.header.block_size
                            + self.storage_layout.data_size_block_min()
                            + space_key.space_available();
                        let space_key = self.gaps_index.insert(
                            space_available,
                            gaps::GapBetween::BlockAndEnd { left_block: block_id.clone(), },
                        );
                        self.blocks_index.update_env_right(&block_id, RightEnvirons::Space { space_key, });
                    },
                },

            // before: ^| ... | A || R || B | ... |$
            // after:  ^| ... | A | ... | B | ... |$
            Environs {
                left: LeftEnvirons::Block { block_id: block_id_left, },
                right: RightEnvirons::Block { block_id: block_id_right, },
            } => {
                let space_available = block_entry.header.block_size
                    + self.storage_layout.data_size_block_min();
                let space_key = self.gaps_index.insert(
                    space_available,
                    gaps::GapBetween::TwoBlocks {
                        left_block: block_id_left.clone(),
                        right_block: block_id_right.clone(),
                    },
                );
                self.blocks_index.update_env_right(&block_id_left, RightEnvirons::Space { space_key, });
                self.blocks_index.update_env_left(&block_id_right, LeftEnvirons::Space { space_key, });
                defrag_op = DefragOp::Queue { free_space_offset: block_entry.offset, space_key, };
            },
        }

        DeleteBlockTaskDoneOp::Perform(DeleteBlockTaskDonePerform { defrag_op, block_entry, })
    }

    pub fn process_delete_block_task_done_defrag<'a>(
        &'a mut self,
        removed_block_id: block::Id,
        defrag_space_key: SpaceKey,
    )
        -> DeleteBlockTaskDoneDefragOp<'a>
    {
        let block_entry = self.blocks_index.get_mut(&removed_block_id).unwrap();
        let start_offset = self.storage_layout.wheel_header_size as u64;
        let data_size_block_min = self.storage_layout.data_size_block_min() as u64;
        let mut defrag_op = DefragOp::None;

        match block_entry.environs.clone() {

            Environs { left: LeftEnvirons::Start, .. } |
            Environs { left: LeftEnvirons::Block { .. }, .. }=>
                unreachable!(),

            Environs { left: LeftEnvirons::Space { space_key, }, right: RightEnvirons::End, } if space_key == defrag_space_key =>
                match self.gaps_index.remove(&space_key) {
                    None | Some(gaps::GapBetween::StartAndEnd) | Some(gaps::GapBetween::BlockAndEnd { .. }) =>
                        unreachable!(),
                    // before: ^| ... | R |$
                    // after:  ^| R | ....|$
                    Some(gaps::GapBetween::StartAndBlock { right_block, }) => {
                        assert_eq!(right_block, removed_block_id);
                        let moved_space_key = self.gaps_index.insert(
                            space_key.space_available(),
                            gaps::GapBetween::BlockAndEnd { left_block: removed_block_id.clone(), },
                        );
                        self.blocks_index.with_mut(&removed_block_id, |block_entry| {
                            block_entry.offset = start_offset;
                            block_entry.environs.left = LeftEnvirons::Start;
                            block_entry.environs.right = RightEnvirons::Space { space_key: moved_space_key, };
                        }).unwrap();
                    },
                    // before: ^| ... | A | ... | R |$
                    // after:  ^| ... | A | R | ... |$
                    Some(gaps::GapBetween::TwoBlocks { left_block, right_block, }) => {
                        assert_eq!(right_block, removed_block_id);
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
                    },
                },

            Environs { left: LeftEnvirons::Space { space_key, }, right: RightEnvirons::End, } =>
                panic!("right end, unexpected left gap {:?}, expected {:?}", space_key, defrag_space_key),

            Environs {
                left: LeftEnvirons::Space { space_key: space_key_left, },
                right: RightEnvirons::Space { space_key: space_key_right, },
            } if space_key_left == defrag_space_key =>
                match (self.gaps_index.remove(&space_key_left), self.gaps_index.remove(&space_key_right)) {
                    (None, _) | (_, None) |
                    (Some(gaps::GapBetween::StartAndEnd), _) | (_, Some(gaps::GapBetween::StartAndEnd)) |
                    (Some(gaps::GapBetween::BlockAndEnd { .. }), _) | (_, Some(gaps::GapBetween::StartAndBlock { .. })) =>
                        unreachable!(),
                    // before: ^| ... | R | ... |$
                    // after:  ^| R | ......... |$
                    (
                        Some(gaps::GapBetween::StartAndBlock { right_block: right_block_left, }),
                        Some(gaps::GapBetween::BlockAndEnd { left_block: left_block_right, }),
                    ) => {
                        assert_eq!(right_block_left, removed_block_id);
                        assert_eq!(left_block_right, removed_block_id);
                        let moved_space_key = self.gaps_index.insert(
                            space_key_left.space_available() + space_key_right.space_available(),
                            gaps::GapBetween::BlockAndEnd { left_block: removed_block_id.clone(), },
                        );
                        self.blocks_index.with_mut(&removed_block_id, |block_entry| {
                            block_entry.offset = start_offset;
                            block_entry.environs.left = LeftEnvirons::Start;
                            block_entry.environs.right = RightEnvirons::Space { space_key: moved_space_key, };
                        }).unwrap();
                    },
                    // before: ^| ... | R | ... | A | ... |$
                    // after:  ^| R | ......... | A | ... |$
                    (
                        Some(gaps::GapBetween::StartAndBlock { right_block: right_block_left, }),
                        Some(gaps::GapBetween::TwoBlocks { left_block: left_block_right, right_block: right_block_right, }),
                    ) => {
                        assert_eq!(right_block_left, removed_block_id);
                        assert_eq!(left_block_right, removed_block_id);
                        let moved_space_key = self.gaps_index.insert(
                            space_key_left.space_available() + space_key_right.space_available(),
                            gaps::GapBetween::TwoBlocks { left_block: removed_block_id.clone(), right_block: right_block_right.clone(), },
                        );
                        self.blocks_index.update_env_left(&right_block_right, LeftEnvirons::Space { space_key: moved_space_key, });
                        let block_end_offset = self.blocks_index.with_mut(&removed_block_id, |block_entry| {
                            block_entry.offset = start_offset;
                            block_entry.environs.left = LeftEnvirons::Start;
                            block_entry.environs.right = RightEnvirons::Space { space_key: moved_space_key, };
                            block_entry.offset
                                + data_size_block_min
                                + block_entry.header.block_size as u64
                        }).unwrap();
                        defrag_op = DefragOp::Queue { free_space_offset: block_end_offset, space_key: moved_space_key, };
                    },
                    // before: ^| ... | A | ... | R | ... |$
                    // after:  ^| ... | A | R | ......... |$
                    (
                        Some(gaps::GapBetween::TwoBlocks { left_block: left_block_left, right_block: right_block_left, }),
                        Some(gaps::GapBetween::BlockAndEnd { left_block: left_block_right, }),
                    ) => {
                        assert_eq!(right_block_left, removed_block_id);
                        assert_eq!(left_block_right, removed_block_id);
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
                            block_entry.environs.left = LeftEnvirons::Block { block_id: right_block_left, };
                            block_entry.environs.right = RightEnvirons::Space { space_key: moved_space_key, };
                        }).unwrap();
                    },
                    // before: ^| ... | A | ... | R | ... | B | ... |$
                    // after:  ^| ... | A | R | ......... | B | ... |$
                    (
                        Some(gaps::GapBetween::TwoBlocks { left_block: left_block_left, right_block: right_block_left, }),
                        Some(gaps::GapBetween::TwoBlocks { left_block: left_block_right, right_block: right_block_right, }),
                    ) => {
                        assert_eq!(right_block_left, removed_block_id);
                        assert_eq!(left_block_right, removed_block_id);
                        let moved_space_key = self.gaps_index.insert(
                            space_key_left.space_available() + space_key_right.space_available(),
                            gaps::GapBetween::TwoBlocks {
                                left_block: removed_block_id.clone(),
                                right_block: right_block_right.clone(),
                            },
                        );
                        let block_offset = self.blocks_index.with_mut(&left_block_left, |block_entry| {
                            RightEnvirons::Block { block_id: removed_block_id.clone(), };
                            block_entry.offset
                                + data_size_block_min
                                + block_entry.header.block_size as u64
                        }).unwrap();
                        self.blocks_index.update_env_left(&right_block_right, LeftEnvirons::Space { space_key: moved_space_key, });
                        let block_end_offset = self.blocks_index.with_mut(&removed_block_id, |block_entry| {
                            block_entry.offset = block_offset;
                            block_entry.environs.left = LeftEnvirons::Block { block_id: left_block_left.clone(), };
                            block_entry.environs.right = RightEnvirons::Space { space_key: moved_space_key, };
                            block_entry.offset
                                + data_size_block_min
                                + block_entry.header.block_size as u64
                        }).unwrap();
                        defrag_op = DefragOp::Queue { free_space_offset: block_end_offset, space_key: moved_space_key, };
                    },
                },

            Environs { left: LeftEnvirons::Space { space_key, }, right: RightEnvirons::Space { .. }, } =>
                panic!("right gap, unexpected left gap {:?}, expected {:?}", space_key, defrag_space_key),

            Environs { left: LeftEnvirons::Space { space_key, }, right: RightEnvirons::Block { block_id, }, } if space_key == defrag_space_key =>
                match self.gaps_index.remove(&space_key) {
                    None | Some(gaps::GapBetween::StartAndEnd) | Some(gaps::GapBetween::BlockAndEnd { .. }) =>
                        unreachable!(),
                    // before: ^| ... | R | B | ... |$
                    // after:  ^| R | ... | B | ... |$
                    Some(gaps::GapBetween::StartAndBlock { right_block, }) => {
                        assert_eq!(right_block, removed_block_id);
                        let moved_space_key = self.gaps_index.insert(
                            space_key.space_available(),
                            gaps::GapBetween::TwoBlocks { left_block: removed_block_id.clone(), right_block: block_id.clone(), },
                        );
                        self.blocks_index.update_env_left(&right_block, LeftEnvirons::Space { space_key: moved_space_key, });
                        let block_end_offset = self.blocks_index.with_mut(&removed_block_id, |block_entry| {
                            block_entry.offset = start_offset;
                            block_entry.environs.left = LeftEnvirons::Start;
                            block_entry.environs.right = RightEnvirons::Space { space_key: moved_space_key, };
                            block_entry.offset
                                + data_size_block_min
                                + block_entry.header.block_size as u64
                        }).unwrap();
                        defrag_op = DefragOp::Queue { free_space_offset: block_end_offset, space_key: moved_space_key, };
                    },
                    // before: ^| ... | A | ... | R | B | ... |$
                    // after:  ^| ... | A | R | ... | B | ... |$
                    Some(gaps::GapBetween::TwoBlocks { left_block, right_block, }) => {
                        assert_eq!(right_block, removed_block_id);
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
                        let block_end_offset = self.blocks_index.with_mut(&removed_block_id, |block_entry| {
                            block_entry.offset = block_offset;
                            block_entry.environs.left = LeftEnvirons::Block { block_id: left_block.clone(), };
                            block_entry.environs.right = RightEnvirons::Space { space_key: moved_space_key, };
                            block_entry.offset
                                + data_size_block_min
                                + block_entry.header.block_size as u64
                        }).unwrap();
                        defrag_op = DefragOp::Queue { free_space_offset: block_end_offset, space_key: moved_space_key, };
                    },
                },

            Environs { left: LeftEnvirons::Space { space_key, }, right: RightEnvirons::Block { .. }, } =>
                panic!("right block, unexpected left gap {:?}, expected {:?}", space_key, defrag_space_key),
        }

        let block_entry = self.blocks_index.get_mut(&removed_block_id).unwrap();
        DeleteBlockTaskDoneDefragOp::Perform(DeleteBlockTaskDoneDefragPerform {
            block_offset: block_entry.offset,
            block_bytes: block_entry
                .block_bytes
                .as_ref()
                .map(Clone::clone)
                .unwrap(),
            tasks_head: &mut block_entry.tasks_head,
            defrag_op,
        })
    }

    pub fn block_tasks_head(&mut self, block_id: &block::Id) -> Option<&mut TasksHead> {
        self.block_offset_tasks_head(block_id).map(|pair| pair.1)
    }

    pub fn block_offset_tasks_head(&mut self, block_id: &block::Id) -> Option<(u64, &mut TasksHead)> {
        self.blocks_index.get_mut(block_id)
            .map(|block_entry| (block_entry.offset, &mut block_entry.tasks_head))
    }

    pub fn pick_defrag_space_key(&mut self, space_key: &SpaceKey) -> Option<&mut BlockEntry> {
        let block_id = match self.gaps_index.get(space_key)? {
            gaps::GapBetween::StartAndBlock { right_block, } =>
                right_block,
            gaps::GapBetween::TwoBlocks { right_block, .. } =>
                right_block,
            gaps::GapBetween::BlockAndEnd { .. } | gaps::GapBetween::StartAndEnd =>
                unreachable!(),
        };
        let block_entry = self.blocks_index.get_mut(block_id).unwrap();
        self.gaps_index.lock_defrag(space_key);
        Some(block_entry)
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

    pub fn push_block(&mut self, offset: u64, block_header: storage::BlockHeader) -> &mut Self {
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
                            block_header.block_id.clone().max(tracker.prev_block_id),
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
                            block_header.block_id.clone().max(tracker.prev_block_id),
                        )
                    },
                }
            },
        };
        self.tracker = Some(BlocksTracker {
            prev_block_id: block_header.block_id.clone(),
            prev_block_offset: offset,
            prev_block_size: self.storage_layout.block_header_size
                + block_header.block_size
                + self.storage_layout.commit_tag_size,
            max_block_id,
        });
        self.blocks_index.insert(
            block_header.block_id.clone(),
            BlockEntry {
                offset,
                header: block_header,
                block_bytes: None,
                environs: Environs { left, right: RightEnvirons::End, },
                tasks_head: Default::default(),
            },
        );
        self
    }

    pub fn finish(mut self, wheel_header: &storage::WheelHeader) -> Schema {
        let next_block_id = match self.tracker {
            None => {
                let space_available = wheel_header.size_bytes
                    - self.storage_layout.wheel_header_size as u64;
                self.gaps_index.insert(space_available as usize, gaps::GapBetween::StartAndEnd);
                block::Id::init()
            },
            Some(tracker) => {
                let space_total = wheel_header.size_bytes
                    - self.storage_layout.wheel_header_size as u64;
                let prev_block_end_offset = tracker.prev_block_offset + tracker.prev_block_size as u64;
                assert!(space_total >= prev_block_end_offset);
                match space_total - prev_block_end_offset {
                    0 =>
                    // do nothing: there should be already RightEnvirons::End set
                        (),
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
                    },
                }
                tracker.max_block_id.next()
            },
        };

        Schema {
            next_block_id,
            storage_layout: self.storage_layout,
            blocks_index: self.blocks_index,
            gaps_index: self.gaps_index,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        block,
        storage,
        BlockEntry,
        SpaceKey,
        Environs,
        LeftEnvirons,
        RightEnvirons,
        Schema,
        WriteBlockOp,
        WriteBlockPerform,
        DefragOp,
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
        let storage_layout = storage::Layout::calculate(&mut Vec::new()).unwrap();
        let mut schema = Schema::new(storage_layout);
        schema.initialize_empty(160);
        schema
    }

    fn sample_hello_world() -> block::Bytes {
        let mut block_bytes_mut = block::BytesMut::new_detached();
        block_bytes_mut.extend("hello, world!".as_bytes().iter().cloned());
        block_bytes_mut.freeze()
    }

    #[test]
    fn process_write_block_request() {
        let mut schema = init();
        assert_eq!(schema.gaps_index.space_total(), 136);

        let op = schema.process_write_block_request(&sample_hello_world(), None);
        assert!(matches!(op, WriteBlockOp::Perform(WriteBlockPerform {
            defrag_op: DefragOp::None,
            task_op: WriteBlockTaskOp {
                block_id,
                block_offset: 24,
            },
            ..
        }) if block_id == block::Id::init()));

        assert_eq!(schema.next_block_id, block::Id::init().next());
        assert!(matches!(
            schema.blocks_index.get(&block::Id::init()),
            Some(&BlockEntry {
                offset: 24,
                header: storage::BlockHeader {
                    ref block_id,
                    block_size: 13,
                    ..
                },
                environs: Environs {
                    left: LeftEnvirons::Start,
                    right: RightEnvirons::Space { space_key: SpaceKey { space_available: 75, serial: 2, }, },
                },
                ..
            }) if block_id == &block::Id::init()
        ));
        assert_eq!(schema.blocks_index.get(&block::Id::init().next()), None);
        assert_eq!(schema.gaps_index.space_total(), 75);

        let op = schema.process_write_block_request(&sample_hello_world(), None);
        assert!(matches!(op, WriteBlockOp::Perform(
            WriteBlockPerform {
                defrag_op: DefragOp::None,
                task_op: WriteBlockTaskOp {
                    block_id,
                    block_offset: 85,
                },
                ..
            },
        ) if block_id == block::Id::init().next()));

        assert_eq!(schema.next_block_id, block::Id::init().next().next());
        assert!(matches!(
            schema.blocks_index.get(&block::Id::init()),
            Some(&BlockEntry {
                offset: 24,
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
                offset: 85,
                header: storage::BlockHeader {
                    block_id: ref block_id_a,
                    block_size: 13,
                    ..
                },
                environs: Environs {
                    left: LeftEnvirons::Block { block_id: ref block_id_b, },
                    right: RightEnvirons::Space { space_key: SpaceKey { space_available: 14, serial: 3, }, },
                },
                ..
            }) if block_id_a == &block::Id::init().next() && block_id_b == &block::Id::init()
        ));
        assert_eq!(schema.blocks_index.get(&block::Id::init().next().next()), None);
        assert_eq!(schema.gaps_index.space_total(), 14);

        let op = schema.process_write_block_request(&sample_hello_world(), None);
        assert!(matches!(op, WriteBlockOp::ReplyNoSpaceLeft));
    }

    #[test]
    fn process_write_read_block_requests() {
        let mut schema = init();
        assert_eq!(schema.gaps_index.space_total(), 136);

        let op = schema.process_read_block_request(&block::Id::init());
        assert!(matches!(op, ReadBlockOp::NotFound));

        let op = schema.process_write_block_request(&sample_hello_world(), None);
        assert!(matches!(op, WriteBlockOp::Perform(
            WriteBlockPerform {
                defrag_op: DefragOp::None,
                task_op: WriteBlockTaskOp {
                    ref block_id,
                    block_offset: 24,
                },
                ..
            },
        ) if block_id == &block::Id::init()));

        assert_eq!(schema.next_block_id, block::Id::init().next());
        assert!(matches!(
            schema.blocks_index.get(&block::Id::init()),
            Some(&BlockEntry {
                offset: 24,
                header: storage::BlockHeader {
                    ref block_id,
                    block_size: 13,
                    ..
                },
                environs: Environs {
                    left: LeftEnvirons::Start,
                    right: RightEnvirons::Space { space_key: SpaceKey { space_available: 75, serial: 2, }, },
                },
                ..
            }) if block_id == &block::Id::init()
        ));
        assert_eq!(schema.blocks_index.get(&block::Id::init().next()), None);
        assert_eq!(schema.gaps_index.space_total(), 75);

        let op = schema.process_read_block_request(&block::Id::init());
        assert!(matches!(op, ReadBlockOp::Cached { ref block_bytes, } if block_bytes == &sample_hello_world()));

        schema.process_write_block_task_done(&block::Id::init());

        let op = schema.process_read_block_request(&block::Id::init());
        assert!(matches!(op, ReadBlockOp::Perform(ReadBlockPerform { block_offset: 24, .. })));
    }

    #[test]
    fn process_delete_block_request() {
        let mut schema = init();
        assert_eq!(schema.gaps_index.space_total(), 136);

        let op = schema.process_write_block_request(&sample_hello_world(), None);
        assert!(matches!(op, WriteBlockOp::Perform(..)));
        let op = schema.process_write_block_request(&sample_hello_world(), None);
        assert!(matches!(op, WriteBlockOp::Perform(..)));

        let op = schema.process_delete_block_request(&block::Id::init());
        assert!(matches!(op, DeleteBlockOp::Perform(DeleteBlockPerform { block_offset: 24, .. })));

        assert!(matches!(
            schema.blocks_index.get(&block::Id::init()),
            Some(&BlockEntry {
                offset: 24,
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
                free_space_offset: 24,
                space_key: SpaceKey { space_available: 61, serial: 4, },
            },
            ..
        })));

        assert_eq!(schema.blocks_index.get(&block::Id::init()), None);
        assert!(matches!(
            schema.blocks_index.get(&block::Id::init().next()),
            Some(&BlockEntry {
                offset: 85,
                header: storage::BlockHeader {
                    ref block_id,
                    block_size: 13,
                    ..
                },
                environs: Environs {
                    left: LeftEnvirons::Space { space_key: SpaceKey { space_available: 61, serial: 4, }, },
                    right: RightEnvirons::Space { space_key: SpaceKey { space_available: 14, serial: 3, }, },
                },
                ..
            }) if block_id == &block::Id::init().next()
        ));
        assert_eq!(schema.gaps_index.space_total(), 75);

        let op = schema.process_write_block_request(&sample_hello_world(), None);
        assert!(matches!(op, WriteBlockOp::Perform(..)));

        let op = schema.process_delete_block_request(&block::Id::init().next());
        assert!(matches!(op, DeleteBlockOp::Perform(DeleteBlockPerform { block_offset: 85, .. })));

        assert!(matches!(
            schema.blocks_index.get(&block::Id::init().next()),
            Some(&BlockEntry {
                offset: 85,
                header: storage::BlockHeader {
                    block_id: ref block_id_a,
                    block_size: 13,
                    ..
                },
                environs: Environs {
                    left: LeftEnvirons::Block { block_id: ref block_id_b, },
                    right: RightEnvirons::Space { space_key: SpaceKey { space_available: 14, serial: 3, }, },
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
                offset: 24,
                header: storage::BlockHeader {
                    ref block_id,
                    block_size: 13,
                    ..
                },
                environs: Environs {
                    left: LeftEnvirons::Start,
                    right: RightEnvirons::Space { space_key: SpaceKey { space_available: 75, serial: 5, }, },
                },
                ..
            }) if block_id == &block::Id::init().next().next()
        ));
        assert_eq!(schema.gaps_index.space_total(), 75);
    }

    #[test]
    fn process_delete_block_task_done_defrag() {
        let mut schema = init();
        assert_eq!(schema.gaps_index.space_total(), 136);

        let op = schema.process_write_block_request(&sample_hello_world(), Some(0));
        assert!(matches!(op, WriteBlockOp::Perform(..)));
        let op = schema.process_write_block_request(&sample_hello_world(), Some(0));
        assert!(matches!(op, WriteBlockOp::Perform(..)));

        let op = schema.process_delete_block_request(&block::Id::init());
        assert!(matches!(op, DeleteBlockOp::Perform(DeleteBlockPerform { block_offset: 24, .. })));

        let op = schema.process_delete_block_task_done(block::Id::init());
        assert!(matches!(op, DeleteBlockTaskDoneOp::Perform(DeleteBlockTaskDonePerform {
            defrag_op: DefragOp::Queue {
                free_space_offset: 24,
                space_key: SpaceKey { space_available: 61, serial: 4, },
            },
            ..
        })));

        // defrag delete
        assert_eq!(schema.gaps_index.space_total(), 75);

        let op = schema.process_delete_block_request(&block::Id::init().next());
        assert!(matches!(op, DeleteBlockOp::Perform(DeleteBlockPerform { block_offset: 85, .. })));

        let op = schema.process_delete_block_task_done_defrag(
            block::Id::init().next(),
            SpaceKey { space_available: 61, serial: 4, },
        );
        assert!(matches!(op, DeleteBlockTaskDoneDefragOp::Perform(DeleteBlockTaskDoneDefragPerform {
            block_offset: 24,
            ..
        })));

        assert!(matches!(
            schema.blocks_index.get(&block::Id::init().next()),
            Some(&BlockEntry {
                offset: 24,
                header: storage::BlockHeader {
                    block_id: ref block_id_a,
                    block_size: 13,
                    ..
                },
                environs: Environs {
                    left: LeftEnvirons::Start,
                    right: RightEnvirons::Space { space_key: SpaceKey { space_available: 75, serial: 5, }, },
                },
                ..
            }) if block_id_a == &block::Id::init().next()
        ));

        assert_eq!(schema.gaps_index.space_total(), 75);
    }
}
