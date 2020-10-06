use std::{
    io,
    path::{
        Path,
        PathBuf,
    },
};

use futures::{
    channel::mpsc,
    Future,
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
    core::{
        task,
        schema,
    },
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

#[derive(Debug)]
pub enum WheelCreateError {
    FileCreate {
        wheel_filename: PathBuf,
        error: io::Error,
    },
    StorageLayoutCalculate(storage::LayoutError),

    InitWheelSizeIsTooSmall {
        provided: usize,
        required_min: usize,
    },
    HeaderSerialize(bincode::Error),
    HeaderTagWrite(io::Error),
    ZeroChunkWrite(io::Error),
    Flush(io::Error),
}

#[derive(Debug)]
pub enum WheelOpenError {
    FileWrongType,
    FileNotFound,
    FileMetadata {
        wheel_filename: PathBuf,
        error: io::Error,
    },
    FileOpen {
        wheel_filename: PathBuf,
        error: io::Error,
    },
}

#[derive(Clone, Debug)]
pub struct CreateParams<P> {
    pub wheel_filename: P,
    pub work_block_size_bytes: usize,
    pub init_wheel_size_bytes: usize,
}

#[derive(Clone, Debug)]
pub struct OpenParams<P> {
    pub wheel_filename: P,
    pub work_block_size_bytes: usize,
}

pub struct GenServer {
    wheel_file: fs::File,
    work_block: Vec<u8>,
    schema: schema::Schema,
    request_tx: mpsc::Sender<Request>,
    request_rx: mpsc::Receiver<Request>,
}

impl GenServer {
    pub async fn create<P>(params: CreateParams<P>) -> Result<Self, WheelCreateError> where P: AsRef<Path> {
        log::debug!("creating new wheel file [ {:?} ]", params.wheel_filename.as_ref());

        let mut wheel_file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(params.wheel_filename.as_ref())
            .await
            .map_err(|error| WheelCreateError::FileCreate {
                wheel_filename: params.wheel_filename.as_ref().to_owned(),
                error,
            })?;

        let mut work_block: Vec<u8> = Vec::with_capacity(params.work_block_size_bytes);

        let storage_layout = storage::Layout::calculate(&mut work_block)
            .map_err(WheelCreateError::StorageLayoutCalculate)?;

        let wheel_header = storage::WheelHeader {
            size_bytes: params.init_wheel_size_bytes,
            ..storage::WheelHeader::default()
        };
        bincode::serialize_into(&mut work_block, &wheel_header)
            .map_err(WheelCreateError::HeaderSerialize)?;

        let mut cursor = work_block.len();
        let min_wheel_file_size = storage_layout.wheel_header_size;
        assert_eq!(cursor, min_wheel_file_size);
        wheel_file.write_all(&work_block).await
            .map_err(WheelCreateError::HeaderTagWrite)?;

        let size_bytes_total = params.init_wheel_size_bytes;
        if size_bytes_total < min_wheel_file_size {
            return Err(WheelCreateError::InitWheelSizeIsTooSmall {
                provided: size_bytes_total,
                required_min: min_wheel_file_size,
            });
        }

        work_block.clear();
        work_block.extend((0 .. params.work_block_size_bytes).map(|_| 0));

        while cursor < size_bytes_total {
            let bytes_remain = size_bytes_total - cursor;
            let write_amount = if bytes_remain < params.work_block_size_bytes {
                bytes_remain
            } else {
                params.work_block_size_bytes
            };
            wheel_file.write_all(&work_block[.. write_amount]).await
                .map_err(WheelCreateError::ZeroChunkWrite)?;
            cursor += write_amount;
        }
        wheel_file.flush().await
            .map_err(WheelCreateError::Flush)?;

        let mut schema = schema::Schema::new(storage_layout);
        schema.initialize_empty(size_bytes_total);

        log::debug!("initialized wheel schema: {:?}", schema);

        let (request_tx, request_rx) = mpsc::channel(0);

        Ok(GenServer {
            wheel_file,
            work_block,
            schema,
            request_tx,
            request_rx,
        })
    }

    pub async fn open<P>(params: OpenParams<P>) -> Result<Self, WheelOpenError> where P: AsRef<Path> {
        log::debug!("opening existing wheel file [ {:?} ]", params.wheel_filename.as_ref());

        match fs::metadata(&params.wheel_filename).await {
            Ok(ref metadata) if metadata.file_type().is_file() =>
                (),
            Ok(..) =>
                return Err(WheelOpenError::FileWrongType),
            Err(ref error) if error.kind() == io::ErrorKind::NotFound =>
                return Err(WheelOpenError::FileNotFound),
            Err(error) =>
                return Err(WheelOpenError::FileMetadata {
                    wheel_filename: params.wheel_filename.as_ref().to_owned(),
                    error,
                }),
        }

        unimplemented!()
    }

    pub fn run(self) -> (Pid, impl Future<Output = Result<(), Error>>) {
        let storage_layout = self.schema.storage_layout().clone();
        (
            Pid {
                request_tx: self.request_tx,
                schema: self.schema,
            },
            busyloop(
                self.request_rx,
                self.wheel_file,
                self.work_block,
                storage_layout,
            ),
        )
    }
}

pub struct Pid {
    pub request_tx: mpsc::Sender<Request>,
    pub schema: schema::Schema,
}

async fn busyloop(
    mut request_rx: mpsc::Receiver<Request>,
    mut wheel_file: fs::File,
    mut work_block: Vec<u8>,
    storage_layout: storage::Layout,
)
    -> Result<(), Error>
{
    let mut cursor = 0;
    wheel_file.seek(io::SeekFrom::Start(cursor)).await
        .map_err(Error::WheelFileInitialSeek)?;

    while let Some(Request { offset, task, reply_tx, }) = request_rx.next().await {
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
