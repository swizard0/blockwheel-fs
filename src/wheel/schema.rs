use super::{
    gaps,
    task,
    proto,
    block,
    index,
    defrag,
    storage,
};

#[derive(Debug)]
pub struct Schema {
    next_block_id: block::Id,
    storage_layout: storage::Layout,
    blocks_index: index::Blocks,
    gaps: gaps::Index,
    defrag_pending_queue: defrag::PendingQueue,
    defrag_task_queue: defrag::TaskQueue,
}

impl Schema {
    pub fn new(storage_layout: storage::Layout) -> Schema {
        Schema {
            next_block_id: block::Id::init(),
            storage_layout,
            blocks_index: index::Blocks::new(),
            gaps: gaps::Index::new(),
            defrag_pending_queue: defrag::PendingQueue::new(),
            defrag_task_queue: defrag::TaskQueue::new(),
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

    pub fn process_write_block_request(
        &mut self,
        proto::RequestWriteBlock { block_bytes, reply_tx, }: proto::RequestWriteBlock,
        tasks_queue: &mut task::Queue,
    )
    {
        let block_id = self.next_block_id.clone();
        self.next_block_id = self.next_block_id.next();
        let mut task_write_block = task::WriteBlock {
            block_id,
            block_bytes,
            reply_tx,
            commit_type: task::CommitType::CommitOnly,
        };

        let space_required = task_write_block.block_bytes.len()
            + self.storage_layout.data_size_block_min();

        match self.gaps.allocate(space_required, &self.blocks_index) {
            Ok(gaps::Allocated::Success { space_available, between: gaps::GapBetween::StartAndBlock { right_block, }, }) => {
                let block_offset = self.storage_layout.wheel_header_size as u64;
                let right_block_id = right_block.block_id.clone();

                let space_left = space_available - space_required;
                let (self_env, right_env) = if space_left > 0 {
                    let space_key = self.gaps.insert(
                        space_left,
                        gaps::GapBetween::TwoBlocks {
                            left_block: task_write_block.block_id.clone(),
                            right_block: right_block_id.clone(),
                        },
                    );
                    let free_space_offset = block_offset + space_required as u64;
                    self.defrag_task_queue.push(free_space_offset, space_key);
                    (
                        index::RightEnvirons::Space { space_key, },
                        index::LeftEnvirons::Space { space_key, },
                    )
                } else {
                    (
                        index::RightEnvirons::Block { block_id: right_block_id.clone(), },
                        index::LeftEnvirons::Block { block_id: task_write_block.block_id.clone(), },
                    )
                };
                self.blocks_index.insert(
                    task_write_block.block_id.clone(),
                    index::BlockEntry {
                        offset: block_offset,
                        header: storage::BlockHeader {
                            block_id: task_write_block.block_id.clone(),
                            block_size: task_write_block.block_bytes.len(),
                            ..Default::default()
                        },
                        tombstone: false,
                        environs: index::Environs {
                            left: index::LeftEnvirons::Start,
                            right: self_env,
                        },
                    },
                );
                self.blocks_index.update_env_left(&right_block_id, right_env);

                tasks_queue.push(block_offset, task::TaskKind::WriteBlock(task_write_block));
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
                            left_block: task_write_block.block_id.clone(),
                            right_block: right_block_id.clone(),
                        },
                    );
                    let free_space_offset = block_offset + space_required as u64;
                    self.defrag_task_queue.push(free_space_offset, space_key);
                    (
                        index::RightEnvirons::Space { space_key, },
                        index::RightEnvirons::Block { block_id: task_write_block.block_id.clone(), },
                        index::LeftEnvirons::Space { space_key, },
                    )
                } else {
                    (
                        index::RightEnvirons::Block { block_id: right_block_id.clone(), },
                        index::RightEnvirons::Block { block_id: task_write_block.block_id.clone(), },
                        index::LeftEnvirons::Block { block_id: task_write_block.block_id.clone(), },
                    )
                };
                self.blocks_index.insert(
                    task_write_block.block_id.clone(),
                    index::BlockEntry {
                        offset: block_offset,
                        header: storage::BlockHeader {
                            block_id: task_write_block.block_id.clone(),
                            block_size: task_write_block.block_bytes.len(),
                            ..Default::default()
                        },
                        tombstone: false,
                        environs: index::Environs {
                            left: index::LeftEnvirons::Block { block_id: left_block_id.clone(), },
                            right: self_env,
                        },
                    },
                );
                self.blocks_index.update_env_right(&left_block_id, left_env);
                self.blocks_index.update_env_left(&right_block_id, right_env);

                tasks_queue.push(block_offset, task::TaskKind::WriteBlock(task_write_block));
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
                            left_block: task_write_block.block_id.clone(),
                        },
                    );
                    (
                        index::RightEnvirons::Space { space_key, },
                        index::RightEnvirons::Block { block_id: task_write_block.block_id.clone(), },
                    )
                } else {
                    (
                        index::RightEnvirons::End,
                        index::RightEnvirons::Block { block_id: task_write_block.block_id.clone(), },
                    )
                };
                self.blocks_index.insert(
                    task_write_block.block_id.clone(),
                    index::BlockEntry {
                        offset: block_offset,
                        header: storage::BlockHeader {
                            block_id: task_write_block.block_id.clone(),
                            block_size: task_write_block.block_bytes.len(),
                            ..Default::default()
                        },
                        tombstone: false,
                        environs: index::Environs {
                            left: index::LeftEnvirons::Block { block_id: left_block_id.clone(), },
                            right: self_env,
                        },
                    },
                );
                self.blocks_index.update_env_right(&left_block_id, left_env);

                task_write_block.commit_type = task::CommitType::CommitAndEof;
                tasks_queue.push(block_offset, task::TaskKind::WriteBlock(task_write_block));
            },
            Ok(gaps::Allocated::Success { space_available, between: gaps::GapBetween::StartAndEnd, }) => {
                let block_offset = self.storage_layout.wheel_header_size as u64;

                let space_left = space_available - space_required;
                let environs = if space_left > 0 {
                    let space_key = self.gaps.insert(
                        space_left,
                        gaps::GapBetween::BlockAndEnd {
                            left_block: task_write_block.block_id.clone(),
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
                    task_write_block.block_id.clone(),
                    index::BlockEntry {
                        offset: block_offset,
                        header: storage::BlockHeader {
                            block_id: task_write_block.block_id.clone(),
                            block_size: task_write_block.block_bytes.len(),
                            ..Default::default()
                        },
                        tombstone: false,
                        environs,
                    },
                );

                task_write_block.commit_type = task::CommitType::CommitAndEof;
                tasks_queue.push(block_offset, task::TaskKind::WriteBlock(task_write_block));
            },
            Ok(gaps::Allocated::PendingDefragmentation) => {
                log::debug!(
                    "cannot directly allocate {} bytes in process_write_block_request: moving to pending defrag queue",
                    task_write_block.block_bytes.len(),
                );
                self.defrag_pending_queue.push(task_write_block);
            },
            Err(gaps::Error::NoSpaceLeft) => {
                if let Err(_send_error) = task_write_block.reply_tx.send(Err(proto::RequestWriteBlockError::NoSpaceLeft)) {
                    log::warn!("process_write_block_request: reply channel has been closed");
                }
            }
        }
    }

    pub fn process_read_block_request(
        &mut self,
        proto::RequestReadBlock { block_id, reply_tx, }: proto::RequestReadBlock,
        block_bytes: block::BytesMut,
        tasks_queue: &mut task::Queue,
    )
    {
        match self.blocks_index.get(&block_id) {
            Some(block_entry) if !block_entry.tombstone => {
                tasks_queue.push(
                    block_entry.offset,
                    task::TaskKind::ReadBlock(task::ReadBlock {
                        block_header: block_entry.header.clone(),
                        block_bytes,
                        reply_tx,
                    }),
                );
            },
            Some(..) | None => {
                if let Err(_send_error) = reply_tx.send(Err(proto::RequestReadBlockError::NotFound)) {
                    log::warn!("process_read_block_request: reply channel has been closed");
                }
            }
        }
    }

    pub fn process_delete_block_request(
        &mut self,
        proto::RequestDeleteBlock { block_id, reply_tx, }: proto::RequestDeleteBlock,
        tasks_queue: &mut task::Queue,
    )
    {
        match self.blocks_index.get_mut(&block_id) {
            Some(block_entry) if !block_entry.tombstone => {
                block_entry.tombstone = true;
                tasks_queue.push(
                    block_entry.offset,
                    task::TaskKind::MarkTombstone(task::MarkTombstone {
                        block_id,
                        reply_tx,
                    }),
                );
            },
            Some(..) | None => {
                if let Err(_send_error) = reply_tx.send(Err(proto::RequestDeleteBlockError::NotFound)) {
                    log::warn!("process_delete_block_request: reply channel has been closed");
                }
            }
        }
    }

    pub fn process_tombstone_written(&mut self, removed_block_id: block::Id) {
        let block_entry = self.blocks_index.remove(&removed_block_id).unwrap();
        assert!(block_entry.tombstone);

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
                        self.defrag_task_queue.push(block_entry.offset, space_key);
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
                self.defrag_task_queue.push(block_entry.offset, space_key);
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
                        let defrag_offset = self.storage_layout.wheel_header_size as u64;
                        self.defrag_task_queue.push(defrag_offset, space_key);
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
                        let defrag_offset = block_entry_left.offset
                            + self.storage_layout.data_size_block_min() as u64
                            + block_entry_left.header.block_size as u64;
                        self.defrag_task_queue.push(defrag_offset, space_key);
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
                        let defrag_offset = block_entry_left.offset
                            + self.storage_layout.data_size_block_min() as u64
                            + block_entry_left.header.block_size as u64;
                        self.defrag_task_queue.push(defrag_offset, space_key);
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
                        let defrag_offset = self.storage_layout.wheel_header_size as u64;
                        self.defrag_task_queue.push(defrag_offset, space_key);
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
                        let defrag_offset = block_entry_left.offset
                            + self.storage_layout.data_size_block_min() as u64
                            + block_entry_left.header.block_size as u64;
                        self.defrag_task_queue.push(defrag_offset, space_key);
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
                self.defrag_task_queue.push(block_entry.offset, space_key);
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use futures::channel::oneshot;

    use super::{
        gaps,
        task,
        proto,
        block,
        index,
        storage,
        Schema,
    };

    fn init() -> (Schema, task::Queue) {
        let storage_layout = storage::Layout {
            wheel_header_size: 24,
            block_header_size: 24,
            commit_tag_size: 16,
            eof_tag_size: 8,
        };
        let mut schema = Schema::new(storage_layout);
        schema.initialize_empty(144);

        (schema, task::Queue::new())
    }

    fn sample_hello_world() -> block::Bytes {
        let mut block_bytes_mut = block::BytesMut::new();
        block_bytes_mut.extend("hello, world!".as_bytes().iter().cloned());
        block_bytes_mut.freeze()
    }

    #[test]
    fn process_write_block_request() {
        let (mut schema, mut tasks_queue) = init();
        assert_eq!(schema.gaps.space_total(), 112);

        let (reply_tx, mut reply_rx) = oneshot::channel();
        schema.process_write_block_request(
            proto::RequestWriteBlock {
                block_bytes: sample_hello_world(),
                reply_tx,
            },
            &mut tasks_queue,
        );

        assert_eq!(schema.next_block_id, block::Id::init().next());
        assert_eq!(
            schema.blocks_index.get(&block::Id::init()),
            Some(&index::BlockEntry {
                offset: 24,
                header: storage::BlockHeader {
                    block_id: block::Id::init(),
                    block_size: 13,
                    ..Default::default()
                },
                tombstone: false,
                environs: index::Environs {
                    left: index::LeftEnvirons::Start,
                    right: index::RightEnvirons::Space { space_key: gaps::SpaceKey { space_available: 59, serial: 2, }, },
                },
            }),
        );
        assert_eq!(schema.blocks_index.get(&block::Id::init().next()), None);
        assert_eq!(schema.gaps.space_total(), 59);
        assert_eq!(reply_rx.try_recv(), Ok(None));

        let (reply_tx, mut reply_rx) = oneshot::channel();
        schema.process_write_block_request(
            proto::RequestWriteBlock {
                block_bytes: sample_hello_world(),
                reply_tx,
            },
            &mut tasks_queue,
        );

        assert_eq!(schema.next_block_id, block::Id::init().next().next());
        assert_eq!(
            schema.blocks_index.get(&block::Id::init()),
            Some(&index::BlockEntry {
                offset: 24,
                header: storage::BlockHeader {
                    block_id: block::Id::init(),
                    block_size: 13,
                    ..Default::default()
                },
                tombstone: false,
                environs: index::Environs {
                    left: index::LeftEnvirons::Start,
                    right: index::RightEnvirons::Block { block_id: block::Id::init().next(), },
                }
            }),
        );
        assert_eq!(
            schema.blocks_index.get(&block::Id::init().next()),
            Some(&index::BlockEntry {
                offset: 77,
                header: storage::BlockHeader {
                    block_id: block::Id::init().next(),
                    block_size: 13,
                    ..Default::default()
                },
                tombstone: false,
                environs: index::Environs {
                    left: index::LeftEnvirons::Block { block_id: block::Id::init(), },
                    right: index::RightEnvirons::Space { space_key: gaps::SpaceKey { space_available: 6, serial: 3, }, },
                },
            }),
        );
        assert_eq!(schema.blocks_index.get(&block::Id::init().next().next()), None);
        assert_eq!(schema.gaps.space_total(), 6);
        assert_eq!(reply_rx.try_recv(), Ok(None));

        let (reply_tx, mut reply_rx) = oneshot::channel();
        schema.process_write_block_request(
            proto::RequestWriteBlock {
                block_bytes: sample_hello_world(),
                reply_tx,
            },
            &mut tasks_queue,
        );
        assert_eq!(reply_rx.try_recv(), Ok(Some(Err(proto::RequestWriteBlockError::NoSpaceLeft))));
    }

    #[test]
    fn process_write_read_block_requests() {
        let (mut schema, mut tasks_queue) = init();
        assert_eq!(schema.gaps.space_total(), 112);

        let (reply_tx, mut reply_rx) = oneshot::channel();
        schema.process_read_block_request(
            proto::RequestReadBlock {
                block_id: block::Id::init(),
                reply_tx,
            },
            block::BytesMut::new(),
            &mut tasks_queue,
        );
        assert_eq!(reply_rx.try_recv(), Ok(Some(Err(proto::RequestReadBlockError::NotFound))));

        let (reply_tx, mut reply_rx) = oneshot::channel();
        schema.process_write_block_request(
            proto::RequestWriteBlock {
                block_bytes: sample_hello_world(),
                reply_tx,
            },
            &mut tasks_queue,
        );

        assert_eq!(schema.next_block_id, block::Id::init().next());
        assert_eq!(
            schema.blocks_index.get(&block::Id::init()),
            Some(&index::BlockEntry {
                offset: 24,
                header: storage::BlockHeader {
                    block_id: block::Id::init(),
                    block_size: 13,
                    ..Default::default()
                },
                tombstone: false,
                environs: index::Environs {
                    left: index::LeftEnvirons::Start,
                    right: index::RightEnvirons::Space { space_key: gaps::SpaceKey { space_available: 59, serial: 2, }, },
                },
            }),
        );
        assert_eq!(schema.blocks_index.get(&block::Id::init().next()), None);
        assert_eq!(schema.gaps.space_total(), 59);
        assert_eq!(reply_rx.try_recv(), Ok(None));

        let (reply_tx, mut reply_rx) = oneshot::channel();
        schema.process_read_block_request(
            proto::RequestReadBlock {
                block_id: block::Id::init(),
                reply_tx,
            },
            block::BytesMut::new(),
            &mut tasks_queue,
        );
        assert_eq!(reply_rx.try_recv(), Ok(None));
    }

    #[test]
    fn process_delete_block_request() {
        let (mut schema, mut tasks_queue) = init();
        assert_eq!(schema.gaps.space_total(), 112);

        let (reply_tx, _reply_rx) = oneshot::channel();
        schema.process_write_block_request(
            proto::RequestWriteBlock {
                block_bytes: sample_hello_world(),
                reply_tx,
            },
            &mut tasks_queue,
        );
        let (reply_tx, _reply_rx) = oneshot::channel();
        schema.process_write_block_request(
            proto::RequestWriteBlock {
                block_bytes: sample_hello_world(),
                reply_tx,
            },
            &mut tasks_queue,
        );

        let (reply_tx, mut reply_rx) = oneshot::channel();
        schema.process_delete_block_request(
            proto::RequestDeleteBlock { block_id: block::Id::init(), reply_tx, },
            &mut tasks_queue,
        );

        assert_eq!(
            schema.blocks_index.get(&block::Id::init()),
            Some(&index::BlockEntry {
                offset: 24,
                header: storage::BlockHeader {
                    block_id: block::Id::init(),
                    block_size: 13,
                    ..Default::default()
                },
                tombstone: true,
                environs: index::Environs {
                    left: index::LeftEnvirons::Start,
                    right: index::RightEnvirons::Block { block_id: block::Id::init().next(), },
                },
            }),
        );
        assert_eq!(reply_rx.try_recv(), Ok(None));

        schema.process_tombstone_written(block::Id::init());

        assert_eq!(schema.blocks_index.get(&block::Id::init()), None);
        assert_eq!(
            schema.blocks_index.get(&block::Id::init().next()),
            Some(&index::BlockEntry {
                offset: 77,
                header: storage::BlockHeader {
                    block_id: block::Id::init().next(),
                    block_size: 13,
                    ..Default::default()
                },
                tombstone: false,
                environs: index::Environs {
                    left: index::LeftEnvirons::Space { space_key: gaps::SpaceKey { space_available: 53, serial: 4, }, },
                    right: index::RightEnvirons::Space { space_key: gaps::SpaceKey { space_available: 6, serial: 3, }, },
                },
            }),
        );
        assert_eq!(schema.gaps.space_total(), 59);
        assert_eq!(
            schema.defrag_task_queue.pop(),
            Some((24, gaps::SpaceKey { space_available: 53, serial: 4, })),
        );

        let (reply_tx, _reply_rx) = oneshot::channel();
        schema.process_write_block_request(
            proto::RequestWriteBlock {
                block_bytes: sample_hello_world(),
                reply_tx,
            },
            &mut tasks_queue,
        );

        let (reply_tx, mut reply_rx) = oneshot::channel();
        schema.process_delete_block_request(
            proto::RequestDeleteBlock { block_id: block::Id::init().next(), reply_tx, },
            &mut tasks_queue,
        );

        assert_eq!(
            schema.blocks_index.get(&block::Id::init().next()),
            Some(&index::BlockEntry {
                offset: 77,
                header: storage::BlockHeader {
                    block_id: block::Id::init().next(),
                    block_size: 13,
                    ..Default::default()
                },
                tombstone: true,
                environs: index::Environs {
                    left: index::LeftEnvirons::Block { block_id: block::Id::init().next().next(), },
                    right: index::RightEnvirons::Space { space_key: gaps::SpaceKey { space_available: 6, serial: 3, }, },
                },
            }),
        );
        assert_eq!(reply_rx.try_recv(), Ok(None));

        schema.process_tombstone_written(block::Id::init().next());

        assert_eq!(schema.blocks_index.get(&block::Id::init().next()), None);
        assert_eq!(
            schema.blocks_index.get(&block::Id::init().next().next()),
            Some(&index::BlockEntry {
                offset: 24,
                header: storage::BlockHeader {
                    block_id: block::Id::init().next().next(),
                    block_size: 13,
                    ..Default::default()
                },
                tombstone: false,
                environs: index::Environs {
                    left: index::LeftEnvirons::Start,
                    right: index::RightEnvirons::Space { space_key: gaps::SpaceKey { space_available: 59, serial: 5, }, },
                },
            }),
        );
        assert_eq!(schema.gaps.space_total(), 59);
        assert_eq!(schema.defrag_task_queue.pop(), None);
    }

}
