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

    pub fn initialize_empty(
        &mut self,
        eof_block_start_offset: u64,
        eof_block_header: storage::BlockHeader,
        size_bytes_total: u64,
    )
    {
        self.blocks_index.insert(
            self.next_block_id.clone(),
            index::BlockEntry {
                offset: eof_block_start_offset,
                header: eof_block_header,
            },
        );

        let total_service_size = self.storage_layout.data_size_service_min()
            + self.storage_layout.data_size_block_min();
        if size_bytes_total > total_service_size as u64 {
            let space_available = size_bytes_total - total_service_size as u64;
            self.gaps.insert(
                space_available as usize,
                gaps::GapBetween::BlockAndEnd {
                    left_block: self.next_block_id.clone(),
                },
            );
        }

        self.next_block_id = self.next_block_id.next();
    }

    pub fn process_write_block_request(
        &mut self,
        proto::RequestWriteBlock { block_bytes, reply_tx, }: proto::RequestWriteBlock,
        tasks_queue: &mut task::Queue,
    )
    {
        let block_id = self.next_block_id.clone();
        self.next_block_id = self.next_block_id.next();
        let task_write_block = task::WriteBlock { block_id, block_bytes, reply_tx, };

        let space_required = task_write_block.block_bytes.len();

        match self.gaps.allocate(space_required, &self.blocks_index) {
            Ok(gaps::Allocated::Success { space_available, between: gaps::GapBetween::StartAndBlock { right_block, }, }) => {
                let block_offset = self.storage_layout.wheel_header_size as u64;
                let right_block_id = right_block.block_id.clone();
                self.blocks_index.insert(
                    task_write_block.block_id.clone(),
                    index::BlockEntry {
                        offset: block_offset,
                        header: storage::BlockHeader::Regular(storage::BlockHeaderRegular {
                            block_id: task_write_block.block_id.clone(),
                            block_size: space_required,
                        }),
                    },
                );

                let space_left = space_available - space_required;
                if space_left >= self.storage_layout.data_size_block_min() {
                    let space_left_available = space_left - self.storage_layout.data_size_block_min();
                    self.gaps.insert(
                        space_left_available,
                        gaps::GapBetween::TwoBlocks {
                            left_block: task_write_block.block_id.clone(),
                            right_block: right_block_id.clone(),
                        },
                    );
                }
                if space_left > 0 {
                    let free_space_offset = block_offset
                        + self.storage_layout.data_size_block_min() as u64
                        + space_required as u64;
                    self.defrag_task_queue.push(free_space_offset, defrag::DefragPosition::TwoBlocks {
                        left_block_id: task_write_block.block_id.clone(),
                        right_block_id: right_block_id.clone(),
                    });
                }

                tasks_queue.push(block_offset, task::TaskKind::WriteBlock(task_write_block));
            },
            Ok(gaps::Allocated::Success { space_available, between: gaps::GapBetween::TwoBlocks { left_block, right_block, }, }) => {

                unimplemented!()
            },
            Ok(gaps::Allocated::Success { space_available, between: gaps::GapBetween::BlockAndEnd { left_block, }, }) => {

                unimplemented!()
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
}
