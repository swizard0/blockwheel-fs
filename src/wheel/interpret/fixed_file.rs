use std::io;

use futures::{
    channel::mpsc,
    StreamExt,
};

use tokio::{
    fs,
    io::{
        AsyncReadExt,
        AsyncWriteExt,
    },
};

use crate::wheel::{
    core::task,
    block,
    storage,
};

use super::Request;

#[derive(Debug)]
pub enum Error {
    WheelFileInitialSeek(io::Error),
    WheelFileSeek {
        offset: u64,
        cursor: u64,
        error: io::Error,
    },
    BlockHeaderSerialize(bincode::Error),
    CommitTagSerialize(bincode::Error),
    TombstoneTagSerialize(bincode::Error),
    BlockWrite(io::Error),
    BlockFlush(io::Error),
    BlockRead(io::Error),
    BlockHeaderDeserialize(bincode::Error),
    CommitTagDeserialize(bincode::Error),
    CorruptedData(CorruptedDataError),
}

#[derive(Debug)]
pub enum CorruptedDataError {
    BlockIdMismatch {
        offset: u64,
        block_id_expected: block::Id,
        block_id_actual: block::Id,
    },
    BlockSizeMismatch {
        offset: u64,
        block_id: block::Id,
        block_size_expected: usize,
        block_size_actual: usize,
    },
    CommitTagBlockIdMismatch {
        offset: u64,
        block_id_expected: block::Id,
        block_id_actual: block::Id,
    },
    CommitTagCrcMismatch {
        offset: u64,
        crc_expected: u64,
        crc_actual: u64,
    },
}

pub async fn busyloop(
    mut interpret_rx: mpsc::Receiver<Request>,
    mut wheel_file: fs::File,
    mut work_block: Vec<u8>,
    storage_layout: storage::Layout,
)
    -> Result<(), Error>
{
    let mut cursor = 0;
    wheel_file.seek(io::SeekFrom::Start(cursor)).await
        .map_err(Error::WheelFileInitialSeek)?;

    while let Some(Request { offset, task, reply_tx, }) = interpret_rx.next().await {
        if cursor != offset {
            wheel_file.seek(io::SeekFrom::Start(offset)).await
                .map_err(|error| Error::WheelFileSeek { offset, cursor, error, })?;
            cursor = offset;
        }

        match task.kind {
            task::TaskKind::WriteBlock(write_block) => {
                let block_header = storage::BlockHeader {
                    block_id: task.block_id.clone(),
                    block_size: write_block.block_bytes.len(),
                    ..Default::default()
                };
                bincode::serialize_into(&mut work_block, &block_header)
                    .map_err(Error::BlockHeaderSerialize)?;
                work_block.extend(write_block.block_bytes.iter());
                let commit_tag = storage::CommitTag {
                    block_id: task.block_id.clone(),
                    crc: write_block.block_bytes.crc(),
                    ..Default::default()
                };
                bincode::serialize_into(&mut work_block, &commit_tag)
                    .map_err(Error::CommitTagSerialize)?;

                wheel_file.write_all(&work_block).await
                    .map_err(Error::BlockWrite)?;
                wheel_file.flush().await
                    .map_err(Error::BlockFlush)?;

                cursor += work_block.len() as u64;
                work_block.clear();

                let task_done = task::Done {
                    current_offset: cursor,
                    task: task::TaskDone {
                        block_id: task.block_id,
                        kind: task::TaskDoneKind::WriteBlock(task::TaskDoneWriteBlock {
                            context: write_block.context,
                        }),
                    },
                };
                if let Err(_send_error) = reply_tx.send(task_done) {
                    break;
                }
            },

            task::TaskKind::ReadBlock(mut read_block) => {
                let total_chunk_size = storage_layout.data_size_block_min()
                    + read_block.block_header.block_size;
                work_block.extend((0 .. total_chunk_size).map(|_| 0));
                wheel_file.read_exact(&mut work_block).await
                    .map_err(Error::BlockRead)?;

                let block_buffer_start = storage_layout.block_header_size;
                let block_buffer_end = work_block.len() - storage_layout.commit_tag_size;

                let block_header: storage::BlockHeader = bincode::deserialize_from(&work_block[.. block_buffer_start])
                    .map_err(Error::BlockHeaderDeserialize)?;
                if block_header.block_id != read_block.block_header.block_id {
                    return Err(Error::CorruptedData(CorruptedDataError::BlockIdMismatch {
                        offset,
                        block_id_expected: read_block.block_header.block_id,
                        block_id_actual: block_header.block_id,
                    }));
                }
                if block_header.block_size != read_block.block_header.block_size {
                    return Err(Error::CorruptedData(CorruptedDataError::BlockSizeMismatch {
                        offset,
                        block_id: block_header.block_id,
                        block_size_expected: read_block.block_header.block_size,
                        block_size_actual: block_header.block_size,
                    }));
                }

                read_block.block_bytes.extend(work_block[block_buffer_start .. block_buffer_end].iter());

                let commit_tag: storage::CommitTag = bincode::deserialize_from(&work_block[block_buffer_end ..])
                    .map_err(Error::CommitTagDeserialize)?;
                if commit_tag.block_id != read_block.block_header.block_id {
                    return Err(Error::CorruptedData(CorruptedDataError::CommitTagBlockIdMismatch {
                        offset,
                        block_id_expected: read_block.block_header.block_id,
                        block_id_actual: commit_tag.block_id,
                    }));
                }
                if commit_tag.crc != read_block.block_bytes.crc() {
                    return Err(Error::CorruptedData(CorruptedDataError::CommitTagCrcMismatch {
                        offset,
                        crc_expected: read_block.block_bytes.crc(),
                        crc_actual: commit_tag.crc,
                    }));
                }

                cursor += work_block.len() as u64;
                work_block.clear();

                let task_done = task::Done {
                    current_offset: cursor,
                    task: task::TaskDone {
                        block_id: read_block.block_header.block_id,
                        kind: task::TaskDoneKind::ReadBlock(task::TaskDoneReadBlock {
                            block_bytes: read_block.block_bytes,
                            context: read_block.context,
                        }),
                    },
                };
                if let Err(_send_error) = reply_tx.send(task_done) {
                    break;
                }
            },

            task::TaskKind::DeleteBlock(delete_block) => {
                let tombstone_tag = storage::TombstoneTag::default();
                bincode::serialize_into(&mut work_block, &tombstone_tag)
                    .map_err(Error::TombstoneTagSerialize)?;

                wheel_file.write_all(&work_block).await
                    .map_err(Error::BlockWrite)?;
                wheel_file.flush().await
                    .map_err(Error::BlockFlush)?;

                cursor += work_block.len() as u64;
                work_block.clear();

                let task_done = task::Done {
                    current_offset: cursor,
                    task: task::TaskDone {
                        block_id: task.block_id,
                        kind: task::TaskDoneKind::DeleteBlock(task::TaskDoneDeleteBlock {
                            context: delete_block.context,
                        }),
                    },
                };
                if let Err(_send_error) = reply_tx.send(task_done) {
                    break;
                }
            },
        }
    }

    log::debug!("master channel closed in interpret_loop, shutting down");
    Ok(())
}
