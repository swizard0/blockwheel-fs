use super::{
    gaps,
    task,
    block,
    index,
    storage,
};

#[derive(Debug)]
pub struct Schema {
    next_block_id: block::Id,
    storage_layout: storage::Layout,
    blocks_index: index::Blocks,
    gaps: gaps::Index,
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
    pub tasks_head: &'a mut task::store::TasksHead,
}

#[derive(Clone, PartialEq, Debug)]
pub enum DefragOp {
    None,
    Queue { free_space_offset: u64, space_key: gaps::SpaceKey, },
}

#[derive(Clone, PartialEq, Debug)]
pub struct WriteBlockTaskOp {
    pub block_id: block::Id,
    pub block_offset: u64,
    pub commit_type: WriteBlockTaskCommitType,
}

#[derive(Clone, PartialEq, Debug)]
pub enum WriteBlockTaskCommitType {
    CommitOnly,
    CommitAndEof,
}

#[derive(Debug)]
pub enum ReadBlockOp<'a> {
    Perform(ReadBlockPerform<'a>),
    NotFound,
}

#[derive(Debug)]
pub struct ReadBlockPerform<'a> {
    pub block_offset: u64,
    pub block_header: &'a storage::BlockHeader,
    pub tasks_head: &'a mut task::store::TasksHead,
}

#[derive(Debug)]
pub enum DeleteBlockOp<'a> {
    Perform(DeleteBlockPerform<'a>),
    NotFound,
}

#[derive(Debug)]
pub struct DeleteBlockPerform<'a> {
    pub block_offset: u64,
    pub tasks_head: &'a mut task::store::TasksHead,
}

#[derive(Clone, PartialEq, Debug)]
pub enum DeleteBlockTaskDoneOp {
    Perform(DeleteBlockTaskDonePerform),
}

#[derive(Clone, PartialEq, Debug)]
pub struct DeleteBlockTaskDonePerform {
    pub defrag_op: DefragOp,
}

impl Schema {
    pub fn new(storage_layout: storage::Layout) -> Schema {
        Schema {
            next_block_id: block::Id::init(),
            storage_layout,
            blocks_index: index::Blocks::new(),
            gaps: gaps::Index::new(),
        }
    }

    pub fn initialize_empty(&mut self, size_bytes_total: usize) {
        let total_service_size = self.storage_layout.service_size_min();
        if size_bytes_total > total_service_size {
            let space_available = size_bytes_total - total_service_size;
            self.gaps.insert(
                space_available,
                gaps::GapBetween::StartAndEnd,
            );
        }
    }

    pub fn storage_layout(&self) -> &storage::Layout {
        &self.storage_layout
    }

    pub fn block_tasks_head(&mut self, block_id: &block::Id) -> Option<&mut task::store::TasksHead> {
        self.blocks_index.get_mut(block_id)
            .map(|block_entry| &mut block_entry.tasks_head)
    }

    pub fn process_write_block_request<'a>(&'a mut self, block_bytes: &block::Bytes) -> WriteBlockOp<'a> {
        let block_id = self.next_block_id.clone();
        self.next_block_id = self.next_block_id.next();

        let mut defrag_op = DefragOp::None;
        let mut commit_type = WriteBlockTaskCommitType::CommitOnly;

        let space_required = block_bytes.len()
            + self.storage_layout.data_size_block_min();

        let block_offset = match self.gaps.allocate(space_required, &self.blocks_index) {

            Ok(gaps::Allocated::Success { space_available, between: gaps::GapBetween::StartAndBlock { right_block, }, }) => {
                let block_offset = self.storage_layout.wheel_header_size as u64;
                let right_block_id = right_block.block_id.clone();

                let space_left = space_available - space_required;
                let (self_env, right_env) = if space_left > 0 {
                    let space_key = self.gaps.insert(
                        space_left,
                        gaps::GapBetween::TwoBlocks {
                            left_block: block_id.clone(),
                            right_block: right_block_id.clone(),
                        },
                    );
                    let free_space_offset = block_offset + space_required as u64;
                    defrag_op = DefragOp::Queue { free_space_offset, space_key, };
                    (
                        index::RightEnvirons::Space { space_key, },
                        index::LeftEnvirons::Space { space_key, },
                    )
                } else {
                    (
                        index::RightEnvirons::Block { block_id: right_block_id.clone(), },
                        index::LeftEnvirons::Block { block_id: block_id.clone(), },
                    )
                };
                self.blocks_index.insert(
                    block_id.clone(),
                    index::BlockEntry {
                        offset: block_offset,
                        header: storage::BlockHeader {
                            block_id: block_id.clone(),
                            block_size: block_bytes.len(),
                            ..Default::default()
                        },
                        environs: index::Environs {
                            left: index::LeftEnvirons::Start,
                            right: self_env,
                        },
                        tasks_head: Default::default(),
                    },
                );
                self.blocks_index.update_env_left(&right_block_id, right_env);

                block_offset
            },

            Ok(gaps::Allocated::Success { space_available, between: gaps::GapBetween::TwoBlocks { left_block, right_block, }, }) => {
                let block_offset = left_block.block_entry.offset
                    + self.storage_layout.data_size_block_min() as u64
                    + left_block.block_entry.header.block_size as u64;
                let left_block_id = left_block.block_id.clone();
                let right_block_id = right_block.block_id.clone();

                let space_left = space_available - space_required;
                let (self_env, left_env, right_env) = if space_left > 0 {
                    let space_key = self.gaps.insert(
                        space_left,
                        gaps::GapBetween::TwoBlocks {
                            left_block: block_id.clone(),
                            right_block: right_block_id.clone(),
                        },
                    );
                    let free_space_offset = block_offset + space_required as u64;
                    defrag_op = DefragOp::Queue { free_space_offset, space_key, };
                    (
                        index::RightEnvirons::Space { space_key, },
                        index::RightEnvirons::Block { block_id: block_id.clone(), },
                        index::LeftEnvirons::Space { space_key, },
                    )
                } else {
                    (
                        index::RightEnvirons::Block { block_id: right_block_id.clone(), },
                        index::RightEnvirons::Block { block_id: block_id.clone(), },
                        index::LeftEnvirons::Block { block_id: block_id.clone(), },
                    )
                };
                self.blocks_index.insert(
                    block_id.clone(),
                    index::BlockEntry {
                        offset: block_offset,
                        header: storage::BlockHeader {
                            block_id: block_id.clone(),
                            block_size: block_bytes.len(),
                            ..Default::default()
                        },
                        environs: index::Environs {
                            left: index::LeftEnvirons::Block { block_id: left_block_id.clone(), },
                            right: self_env,
                        },
                        tasks_head: Default::default(),
                    },
                );
                self.blocks_index.update_env_right(&left_block_id, left_env);
                self.blocks_index.update_env_left(&right_block_id, right_env);

                block_offset
            },

            Ok(gaps::Allocated::Success { space_available, between: gaps::GapBetween::BlockAndEnd { left_block, }, }) => {
                let block_offset = left_block.block_entry.offset
                    + self.storage_layout.data_size_block_min() as u64
                    + left_block.block_entry.header.block_size as u64;
                let left_block_id = left_block.block_id.clone();

                let space_left = space_available - space_required;
                let (self_env, left_env) = if space_left > 0 {
                    let space_key = self.gaps.insert(
                        space_left,
                        gaps::GapBetween::BlockAndEnd {
                            left_block: block_id.clone(),
                        },
                    );
                    (
                        index::RightEnvirons::Space { space_key, },
                        index::RightEnvirons::Block { block_id: block_id.clone(), },
                    )
                } else {
                    (
                        index::RightEnvirons::End,
                        index::RightEnvirons::Block { block_id: block_id.clone(), },
                    )
                };
                self.blocks_index.insert(
                    block_id.clone(),
                    index::BlockEntry {
                        offset: block_offset,
                        header: storage::BlockHeader {
                            block_id: block_id.clone(),
                            block_size: block_bytes.len(),
                            ..Default::default()
                        },
                        environs: index::Environs {
                            left: index::LeftEnvirons::Block { block_id: left_block_id.clone(), },
                            right: self_env,
                        },
                        tasks_head: Default::default(),
                    },
                );
                self.blocks_index.update_env_right(&left_block_id, left_env);

                commit_type = WriteBlockTaskCommitType::CommitAndEof;
                block_offset
            },

            Ok(gaps::Allocated::Success { space_available, between: gaps::GapBetween::StartAndEnd, }) => {
                let block_offset = self.storage_layout.wheel_header_size as u64;

                let space_left = space_available - space_required;
                let environs = if space_left > 0 {
                    let space_key = self.gaps.insert(
                        space_left,
                        gaps::GapBetween::BlockAndEnd {
                            left_block: block_id.clone(),
                        },
                    );
                    index::Environs {
                        left: index::LeftEnvirons::Start,
                        right: index::RightEnvirons::Space { space_key, },
                    }
                } else {
                    index::Environs {
                        left: index::LeftEnvirons::Start,
                        right: index::RightEnvirons::End,
                    }
                };
                self.blocks_index.insert(
                    block_id.clone(),
                    index::BlockEntry {
                        offset: block_offset,
                        header: storage::BlockHeader {
                            block_id: block_id.clone(),
                            block_size: block_bytes.len(),
                            ..Default::default()
                        },
                        environs,
                        tasks_head: Default::default(),
                    },
                );

                commit_type = WriteBlockTaskCommitType::CommitAndEof;
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
                    commit_type,
                },
            },
        )
    }

    pub fn process_read_block_request<'a>(&'a mut self, block_id: &block::Id) -> ReadBlockOp<'a> {
        match self.blocks_index.get_mut(block_id) {
            Some(block_entry) =>
                ReadBlockOp::Perform(ReadBlockPerform {
                    block_offset: block_entry.offset,
                    block_header: &block_entry.header,
                    tasks_head: &mut block_entry.tasks_head,
                }),
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

    pub fn process_delete_block_task_done(&mut self, removed_block_id: block::Id) -> DeleteBlockTaskDoneOp {
        let block_entry = self.blocks_index.remove(&removed_block_id).unwrap();
        let mut defrag_op = DefragOp::None;

        match block_entry.environs {

            index::Environs {
                left: index::LeftEnvirons::Start,
                right: index::RightEnvirons::End,
            } => {
                assert_eq!(self.gaps.space_total(), 0);
                let space_available = block_entry.header.block_size
                    + self.storage_layout.data_size_block_min();
                self.gaps.insert(space_available, gaps::GapBetween::StartAndEnd);
            },

            index::Environs {
                left: index::LeftEnvirons::Start,
                right: index::RightEnvirons::Space { space_key, },
            } =>
                match self.gaps.remove(&space_key) {
                    None | Some(gaps::GapBetween::StartAndEnd) | Some(gaps::GapBetween::StartAndBlock { .. }) =>
                        unreachable!(),
                    Some(gaps::GapBetween::TwoBlocks { left_block, right_block, }) => {
                        assert_eq!(left_block, removed_block_id);
                        let space_available = block_entry.header.block_size
                            + self.storage_layout.data_size_block_min()
                            + space_key.space_available();
                        let space_key = self.gaps.insert(
                            space_available,
                            gaps::GapBetween::StartAndBlock { right_block: right_block.clone(), },
                        );
                        self.blocks_index.update_env_left(&right_block, index::LeftEnvirons::Start);
                        assert_eq!(block_entry.offset, self.storage_layout.wheel_header_size as u64);
                        defrag_op = DefragOp::Queue { free_space_offset: block_entry.offset, space_key, };
                    },
                    Some(gaps::GapBetween::BlockAndEnd { left_block, }) => {
                        assert_eq!(left_block, removed_block_id);
                        let space_available = block_entry.header.block_size
                            + self.storage_layout.data_size_block_min()
                            + space_key.space_available();
                        self.gaps.insert(space_available, gaps::GapBetween::StartAndEnd);
                    },
                },

            index::Environs {
                left: index::LeftEnvirons::Start,
                right: index::RightEnvirons::Block { block_id, },
            } => {
                let space_available = block_entry.header.block_size
                    + self.storage_layout.data_size_block_min();
                let space_key = self.gaps.insert(space_available, gaps::GapBetween::StartAndBlock { right_block: block_id.clone(), });
                self.blocks_index.update_env_left(&block_id, index::LeftEnvirons::Space { space_key, });
                assert_eq!(block_entry.offset, self.storage_layout.wheel_header_size as u64);
                defrag_op = DefragOp::Queue { free_space_offset: block_entry.offset, space_key, };
            },

            index::Environs {
                left: index::LeftEnvirons::Space { space_key, },
                right: index::RightEnvirons::End,
            } =>
                match self.gaps.remove(&space_key) {
                    None | Some(gaps::GapBetween::StartAndEnd) | Some(gaps::GapBetween::BlockAndEnd { .. }) =>
                        unreachable!(),
                    Some(gaps::GapBetween::TwoBlocks { left_block, right_block, }) => {
                        assert_eq!(right_block, removed_block_id);
                        let space_available = block_entry.header.block_size
                            + self.storage_layout.data_size_block_min()
                            + space_key.space_available();
                        self.gaps.insert(
                            space_available,
                            gaps::GapBetween::BlockAndEnd { left_block: left_block.clone(), },
                        );
                        self.blocks_index.update_env_right(&left_block, index::RightEnvirons::End);
                    },
                    Some(gaps::GapBetween::StartAndBlock { right_block, }) => {
                        assert_eq!(right_block, removed_block_id);
                        let space_available = block_entry.header.block_size
                            + self.storage_layout.data_size_block_min()
                            + space_key.space_available();
                        self.gaps.insert(space_available, gaps::GapBetween::StartAndEnd);
                    },
                },

            index::Environs {
                left: index::LeftEnvirons::Space { space_key: space_key_left, },
                right: index::RightEnvirons::Space { space_key: space_key_right, },
            } =>
                match (self.gaps.remove(&space_key_left), self.gaps.remove(&space_key_right)) {
                    (None, _) | (_, None) |
                    (Some(gaps::GapBetween::StartAndEnd), _) | (_, Some(gaps::GapBetween::StartAndEnd)) |
                    (Some(gaps::GapBetween::BlockAndEnd { .. }), _) | (_, Some(gaps::GapBetween::StartAndBlock { .. })) =>
                        unreachable!(),
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
                        self.gaps.insert(space_available, gaps::GapBetween::StartAndEnd);
                    },
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
                        let space_key = self.gaps.insert(
                            space_available,
                            gaps::GapBetween::StartAndBlock { right_block: right_block_right.clone(), },
                        );
                        self.blocks_index.update_env_left(&right_block_right, index::LeftEnvirons::Space { space_key, });
                        let free_space_offset = self.storage_layout.wheel_header_size as u64;
                        defrag_op = DefragOp::Queue { free_space_offset, space_key, };
                    },
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
                        let space_key = self.gaps.insert(
                            space_available,
                            gaps::GapBetween::BlockAndEnd { left_block: left_block_left.clone(), },
                        );
                        self.blocks_index.update_env_right(&left_block_left, index::RightEnvirons::Space { space_key, });
                    },
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
                        let space_key = self.gaps.insert(
                            space_available,
                            gaps::GapBetween::TwoBlocks {
                                left_block: left_block_left.clone(),
                                right_block: right_block_right.clone(),
                            },
                        );
                        self.blocks_index.update_env_right(&left_block_left, index::RightEnvirons::Space { space_key, });
                        self.blocks_index.update_env_left(&right_block_right, index::LeftEnvirons::Space { space_key, });
                        let block_entry_left = self.blocks_index.get(&left_block_left).unwrap();
                        let free_space_offset = block_entry_left.offset
                            + self.storage_layout.data_size_block_min() as u64
                            + block_entry_left.header.block_size as u64;
                        defrag_op = DefragOp::Queue { free_space_offset, space_key, };
                    },
                },

            index::Environs {
                left: index::LeftEnvirons::Space { space_key, },
                right: index::RightEnvirons::Block { block_id, },
            } =>
                match self.gaps.remove(&space_key) {
                    None | Some(gaps::GapBetween::StartAndEnd) | Some(gaps::GapBetween::BlockAndEnd { .. }) =>
                        unreachable!(),
                    Some(gaps::GapBetween::TwoBlocks { left_block, right_block, }) => {
                        assert_eq!(right_block, removed_block_id);
                        let space_available = block_entry.header.block_size
                            + self.storage_layout.data_size_block_min()
                            + space_key.space_available();
                        let space_key = self.gaps.insert(
                            space_available,
                            gaps::GapBetween::TwoBlocks {
                                left_block: left_block.clone(),
                                right_block: block_id.clone(),
                            },
                        );
                        self.blocks_index.update_env_right(&left_block, index::RightEnvirons::Space { space_key, });
                        self.blocks_index.update_env_left(&block_id, index::LeftEnvirons::Space { space_key, });
                        let block_entry_left = self.blocks_index.get(&left_block).unwrap();
                        let free_space_offset = block_entry_left.offset
                            + self.storage_layout.data_size_block_min() as u64
                            + block_entry_left.header.block_size as u64;
                        defrag_op = DefragOp::Queue { free_space_offset, space_key, };
                    },
                    Some(gaps::GapBetween::StartAndBlock { right_block, }) => {
                        assert_eq!(right_block, removed_block_id);
                        let space_available = block_entry.header.block_size
                            + self.storage_layout.data_size_block_min()
                            + space_key.space_available();
                        let space_key = self.gaps.insert(
                            space_available,
                            gaps::GapBetween::StartAndBlock { right_block: block_id.clone(), },
                        );
                        self.blocks_index.update_env_left(&block_id, index::LeftEnvirons::Space { space_key, });
                        let free_space_offset = self.storage_layout.wheel_header_size as u64;
                        defrag_op = DefragOp::Queue { free_space_offset, space_key, };
                    },
                },

            index::Environs {
                left: index::LeftEnvirons::Block { block_id, },
                right: index::RightEnvirons::End,
            } => {
                let space_available = block_entry.header.block_size
                    + self.storage_layout.data_size_block_min();
                let space_key = self.gaps.insert(
                    space_available,
                    gaps::GapBetween::BlockAndEnd { left_block: block_id.clone(), },
                );
                self.blocks_index.update_env_right(&block_id, index::RightEnvirons::Space { space_key, });
            },

            index::Environs {
                left: index::LeftEnvirons::Block { block_id, },
                right: index::RightEnvirons::Space { space_key, },
            } =>
                match self.gaps.remove(&space_key) {
                    None | Some(gaps::GapBetween::StartAndEnd) | Some(gaps::GapBetween::StartAndBlock { .. }) =>
                        unreachable!(),
                    Some(gaps::GapBetween::TwoBlocks { left_block, right_block, }) => {
                        assert_eq!(left_block, removed_block_id);
                        let space_available = block_entry.header.block_size
                            + self.storage_layout.data_size_block_min()
                            + space_key.space_available();
                        let space_key = self.gaps.insert(
                            space_available,
                            gaps::GapBetween::TwoBlocks {
                                left_block: block_id.clone(),
                                right_block: right_block.clone(),
                            },
                        );
                        self.blocks_index.update_env_right(&block_id, index::RightEnvirons::Space { space_key, });
                        self.blocks_index.update_env_left(&right_block, index::LeftEnvirons::Space { space_key, });
                        let block_entry_left = self.blocks_index.get(&block_id).unwrap();
                        let free_space_offset = block_entry_left.offset
                            + self.storage_layout.data_size_block_min() as u64
                            + block_entry_left.header.block_size as u64;
                        defrag_op = DefragOp::Queue { free_space_offset, space_key, };
                    },
                    Some(gaps::GapBetween::BlockAndEnd { left_block, }) => {
                        assert_eq!(left_block, removed_block_id);
                        let space_available = block_entry.header.block_size
                            + self.storage_layout.data_size_block_min()
                            + space_key.space_available();
                        let space_key = self.gaps.insert(
                            space_available,
                            gaps::GapBetween::BlockAndEnd { left_block: block_id.clone(), },
                        );
                        self.blocks_index.update_env_right(&block_id, index::RightEnvirons::Space { space_key, });
                    },
                },

            index::Environs {
                left: index::LeftEnvirons::Block { block_id: block_id_left, },
                right: index::RightEnvirons::Block { block_id: block_id_right, },
            } => {
                let space_available = block_entry.header.block_size
                    + self.storage_layout.data_size_block_min();
                let space_key = self.gaps.insert(
                    space_available,
                    gaps::GapBetween::TwoBlocks {
                        left_block: block_id_left.clone(),
                        right_block: block_id_right.clone(),
                    },
                );
                self.blocks_index.update_env_right(&block_id_left, index::RightEnvirons::Space { space_key, });
                self.blocks_index.update_env_left(&block_id_right, index::LeftEnvirons::Space { space_key, });
                defrag_op = DefragOp::Queue { free_space_offset: block_entry.offset, space_key, };
            },
        }

        DeleteBlockTaskDoneOp::Perform(DeleteBlockTaskDonePerform { defrag_op, })
    }
}

#[cfg(test)]
mod tests {
    // use super::{
    //     gaps,
    //     block,
    //     index,
    //     storage,
    //     Schema,
    //     WriteBlockOp,
    //     WriteBlockPerform,
    //     DefragOp,
    //     WriteBlockTaskOp,
    //     WriteBlockTaskCommitType,
    //     ReadBlockOp,
    //     ReadBlockPerform,
    //     DeleteBlockOp,
    //     DeleteBlockPerform,
    //     TombstoneWrittenOp,
    //     TombstoneWrittenPerform,
    // };

    // fn init() -> Schema {
    //     let storage_layout = storage::Layout {
    //         wheel_header_size: 24,
    //         block_header_size: 24,
    //         commit_tag_size: 16,
    //         eof_tag_size: 8,
    //     };
    //     let mut schema = Schema::new(storage_layout);
    //     schema.initialize_empty(144);
    //     schema
    // }

    // fn sample_hello_world() -> block::Bytes {
    //     let mut block_bytes_mut = block::BytesMut::new();
    //     block_bytes_mut.extend("hello, world!".as_bytes().iter().cloned());
    //     block_bytes_mut.freeze()
    // }

    // #[test]
    // fn process_write_block_request() {
    //     let mut schema = init();
    //     assert_eq!(schema.gaps.space_total(), 112);

    //     let op = schema.process_write_block_request(&sample_hello_world());
    //     assert_eq!(op, WriteBlockOp::Perform(
    //         WriteBlockPerform {
    //             defrag_op: DefragOp::None,
    //             task_op: WriteBlockTaskOp {
    //                 block_id: block::Id::init(),
    //                 block_offset: 24,
    //                 commit_type: WriteBlockTaskCommitType::CommitAndEof,
    //             },
    //         },
    //     ));

    //     assert_eq!(schema.next_block_id, block::Id::init().next());
    //     assert_eq!(
    //         schema.blocks_index.get(&block::Id::init()),
    //         Some(&index::BlockEntry {
    //             offset: 24,
    //             header: storage::BlockHeader {
    //                 block_id: block::Id::init(),
    //                 block_size: 13,
    //                 ..Default::default()
    //             },
    //             tombstone: false,
    //             environs: index::Environs {
    //                 left: index::LeftEnvirons::Start,
    //                 right: index::RightEnvirons::Space { space_key: gaps::SpaceKey { space_available: 59, serial: 2, }, },
    //             },
    //         }),
    //     );
    //     assert_eq!(schema.blocks_index.get(&block::Id::init().next()), None);
    //     assert_eq!(schema.gaps.space_total(), 59);

    //     let op = schema.process_write_block_request(&sample_hello_world());
    //     assert_eq!(op, WriteBlockOp::Perform(
    //         WriteBlockPerform {
    //             defrag_op: DefragOp::None,
    //             task_op: WriteBlockTaskOp {
    //                 block_id: block::Id::init().next(),
    //                 block_offset: 77,
    //                 commit_type: WriteBlockTaskCommitType::CommitAndEof,
    //             },
    //         },
    //     ));

    //     assert_eq!(schema.next_block_id, block::Id::init().next().next());
    //     assert_eq!(
    //         schema.blocks_index.get(&block::Id::init()),
    //         Some(&index::BlockEntry {
    //             offset: 24,
    //             header: storage::BlockHeader {
    //                 block_id: block::Id::init(),
    //                 block_size: 13,
    //                 ..Default::default()
    //             },
    //             tombstone: false,
    //             environs: index::Environs {
    //                 left: index::LeftEnvirons::Start,
    //                 right: index::RightEnvirons::Block { block_id: block::Id::init().next(), },
    //             }
    //         }),
    //     );
    //     assert_eq!(
    //         schema.blocks_index.get(&block::Id::init().next()),
    //         Some(&index::BlockEntry {
    //             offset: 77,
    //             header: storage::BlockHeader {
    //                 block_id: block::Id::init().next(),
    //                 block_size: 13,
    //                 ..Default::default()
    //             },
    //             tombstone: false,
    //             environs: index::Environs {
    //                 left: index::LeftEnvirons::Block { block_id: block::Id::init(), },
    //                 right: index::RightEnvirons::Space { space_key: gaps::SpaceKey { space_available: 6, serial: 3, }, },
    //             },
    //         }),
    //     );
    //     assert_eq!(schema.blocks_index.get(&block::Id::init().next().next()), None);
    //     assert_eq!(schema.gaps.space_total(), 6);

    //     let op = schema.process_write_block_request(&sample_hello_world());
    //     assert_eq!(op, WriteBlockOp::ReplyNoSpaceLeft);
    // }

    // #[test]
    // fn process_write_read_block_requests() {
    //     let mut schema = init();
    //     assert_eq!(schema.gaps.space_total(), 112);

    //     let op = schema.process_read_block_request(&block::Id::init());
    //     assert_eq!(op, ReadBlockOp::NotFound);

    //     let op = schema.process_write_block_request(&sample_hello_world());
    //     assert_eq!(op, WriteBlockOp::Perform(
    //         WriteBlockPerform {
    //             defrag_op: DefragOp::None,
    //             task_op: WriteBlockTaskOp {
    //                 block_id: block::Id::init(),
    //                 block_offset: 24,
    //                 commit_type: WriteBlockTaskCommitType::CommitAndEof,
    //             },
    //         },
    //     ));

    //     assert_eq!(schema.next_block_id, block::Id::init().next());
    //     assert_eq!(
    //         schema.blocks_index.get(&block::Id::init()),
    //         Some(&index::BlockEntry {
    //             offset: 24,
    //             header: storage::BlockHeader {
    //                 block_id: block::Id::init(),
    //                 block_size: 13,
    //                 ..Default::default()
    //             },
    //             tombstone: false,
    //             environs: index::Environs {
    //                 left: index::LeftEnvirons::Start,
    //                 right: index::RightEnvirons::Space { space_key: gaps::SpaceKey { space_available: 59, serial: 2, }, },
    //             },
    //         }),
    //     );
    //     assert_eq!(schema.blocks_index.get(&block::Id::init().next()), None);
    //     assert_eq!(schema.gaps.space_total(), 59);

    //     let op = schema.process_read_block_request(&block::Id::init());
    //     assert_eq!(op, ReadBlockOp::Perform(
    //         ReadBlockPerform {
    //             block_offset: 24,
    //             block_header: storage::BlockHeader {
    //                 magic: 1941340961166119119,
    //                 block_id: block::Id::init(),
    //                 block_size: 13,
    //             },
    //         },
    //     ));
    // }

    // #[test]
    // fn process_delete_block_request() {
    //     let mut schema = init();
    //     assert_eq!(schema.gaps.space_total(), 112);

    //     schema.process_write_block_request(&sample_hello_world());
    //     schema.process_write_block_request(&sample_hello_world());

    //     let op = schema.process_delete_block_request(&block::Id::init());
    //     assert_eq!(op, DeleteBlockOp::Perform(DeleteBlockPerform { block_offset: 24 }));

    //     assert_eq!(
    //         schema.blocks_index.get(&block::Id::init()),
    //         Some(&index::BlockEntry {
    //             offset: 24,
    //             header: storage::BlockHeader {
    //                 block_id: block::Id::init(),
    //                 block_size: 13,
    //                 ..Default::default()
    //             },
    //             tombstone: true,
    //             environs: index::Environs {
    //                 left: index::LeftEnvirons::Start,
    //                 right: index::RightEnvirons::Block { block_id: block::Id::init().next(), },
    //             },
    //         }),
    //     );

    //     let op = schema.process_tombstone_written(block::Id::init());
    //     assert_eq!(op, TombstoneWrittenOp::Perform(TombstoneWrittenPerform {
    //         defrag_op: DefragOp::Queue {
    //             free_space_offset: 24,
    //             space_key: gaps::SpaceKey { space_available: 53, serial: 4, },
    //         },
    //     }));

    //     assert_eq!(schema.blocks_index.get(&block::Id::init()), None);
    //     assert_eq!(
    //         schema.blocks_index.get(&block::Id::init().next()),
    //         Some(&index::BlockEntry {
    //             offset: 77,
    //             header: storage::BlockHeader {
    //                 block_id: block::Id::init().next(),
    //                 block_size: 13,
    //                 ..Default::default()
    //             },
    //             tombstone: false,
    //             environs: index::Environs {
    //                 left: index::LeftEnvirons::Space { space_key: gaps::SpaceKey { space_available: 53, serial: 4, }, },
    //                 right: index::RightEnvirons::Space { space_key: gaps::SpaceKey { space_available: 6, serial: 3, }, },
    //             },
    //         }),
    //     );
    //     assert_eq!(schema.gaps.space_total(), 59);

    //     schema.process_write_block_request(&sample_hello_world());

    //     let op = schema.process_delete_block_request(&block::Id::init().next());
    //     assert_eq!(op, DeleteBlockOp::Perform(DeleteBlockPerform { block_offset: 77, }));

    //     assert_eq!(
    //         schema.blocks_index.get(&block::Id::init().next()),
    //         Some(&index::BlockEntry {
    //             offset: 77,
    //             header: storage::BlockHeader {
    //                 block_id: block::Id::init().next(),
    //                 block_size: 13,
    //                 ..Default::default()
    //             },
    //             tombstone: true,
    //             environs: index::Environs {
    //                 left: index::LeftEnvirons::Block { block_id: block::Id::init().next().next(), },
    //                 right: index::RightEnvirons::Space { space_key: gaps::SpaceKey { space_available: 6, serial: 3, }, },
    //             },
    //         }),
    //     );

    //     let op = schema.process_tombstone_written(block::Id::init().next());
    //     assert_eq!(op, TombstoneWrittenOp::Perform(TombstoneWrittenPerform { defrag_op: DefragOp::None, }));

    //     assert_eq!(schema.blocks_index.get(&block::Id::init().next()), None);
    //     assert_eq!(
    //         schema.blocks_index.get(&block::Id::init().next().next()),
    //         Some(&index::BlockEntry {
    //             offset: 24,
    //             header: storage::BlockHeader {
    //                 block_id: block::Id::init().next().next(),
    //                 block_size: 13,
    //                 ..Default::default()
    //             },
    //             tombstone: false,
    //             environs: index::Environs {
    //                 left: index::LeftEnvirons::Start,
    //                 right: index::RightEnvirons::Space { space_key: gaps::SpaceKey { space_available: 59, serial: 5, }, },
    //             },
    //         }),
    //     );
    //     assert_eq!(schema.gaps.space_total(), 59);
    // }

}
