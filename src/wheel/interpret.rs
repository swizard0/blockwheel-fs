use futures::{
    channel::{
        mpsc,
        oneshot,
    },
    SinkExt,
};

use alloc_pool::bytes::{
    Bytes,
    BytesMut,
};

use crate::{
    InterpretStats,
    context::Context,
    wheel::{
        block,
        storage,
        core::task,
    },
};

pub mod ram;
pub mod fixed_file;

struct Request<C> where C: Context {
    offset: u64,
    task: task::Task<C>,
    reply_tx: oneshot::Sender<DoneTask<C>>,
}

pub struct DoneTask<C> where C: Context {
    pub task_done: task::Done<C>,
    pub stats: InterpretStats,
}

pub type RequestTask<C> = task::Task<C>;
pub type RequestReplyRx<C> = oneshot::Receiver<DoneTask<C>>;

pub struct Synced;

enum Command<C> where C: Context {
    Request(Request<C>),
    DeviceSync { reply_tx: oneshot::Sender<Synced>, },
}

#[derive(Debug)]
pub enum CreateError {
    FixedFile(fixed_file::WheelCreateError),
}

#[derive(Debug)]
pub enum OpenError {
    FixedFile(fixed_file::WheelOpenError),
}

#[derive(Debug)]
pub enum RunError {
    FixedFile(fixed_file::Error),
}

#[derive(Clone)]
pub struct Pid<C> where C: Context {
    request_tx: mpsc::Sender<Command<C>>,
}

impl<C> Pid<C> where C: Context {
    pub async fn push_request(&mut self, offset: u64, task: RequestTask<C>) -> Result<RequestReplyRx<C>, ero::NoProcError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.request_tx
            .send(Command::Request(Request { offset, task, reply_tx, }))
            .await
            .map_err(|_send_error| ero::NoProcError)?;
        Ok(reply_rx)
    }

    pub async fn device_sync(&mut self) -> Result<Synced, ero::NoProcError> {
        loop {
            let (reply_tx, reply_rx) = oneshot::channel();
            self.request_tx.send(Command::DeviceSync { reply_tx, }).await
                .map_err(|_send_error| ero::NoProcError)?;
            match reply_rx.await {
                Ok(Synced) =>
                    return Ok(Synced),
                Err(oneshot::Canceled) =>
                    (),
            }
        }
    }
}

#[derive(Debug)]
pub enum BlockProcessJobError {
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

pub type BlockProcessJobOutput = Result<BlockProcessJobDone, BlockProcessJobError>;

pub struct BlockProcessJobDone {
    block_id: block::Id,
    block_bytes: Bytes,
    block_crc: u64,
}

pub struct BlockProcessJobArgs {
    offset: u64,
    storage_layout: storage::Layout,
    block_header: storage::BlockHeader,
    block_bytes: BytesMut,
}

pub fn block_process_job(
    BlockProcessJobArgs {
        offset,
        storage_layout,
        block_header,
        block_bytes,
    }: BlockProcessJobArgs,
)
    -> BlockProcessJobOutput
{
    let block_buffer_start = storage_layout.block_header_size;
    let block_buffer_end = block_bytes.len() - storage_layout.commit_tag_size;

    let storage_block_header: storage::BlockHeader = bincode::deserialize_from(
        &block_bytes[.. block_buffer_start],
    ).map_err(BlockProcessJobError::BlockHeaderDeserialize)?;
    if storage_block_header.block_id != block_header.block_id {
        return Err(BlockProcessJobError::CorruptedData(CorruptedDataError::BlockIdMismatch {
            offset,
            block_id_expected: block_header.block_id,
            block_id_actual: storage_block_header.block_id,
        }));
    }

    if storage_block_header.block_size != block_header.block_size {
        return Err(BlockProcessJobError::CorruptedData(CorruptedDataError::BlockSizeMismatch {
            offset,
            block_id: block_header.block_id,
            block_size_expected: block_header.block_size,
            block_size_actual: storage_block_header.block_size,
        }));
    }
    let commit_tag: storage::CommitTag = bincode::deserialize_from(
        &block_bytes[block_buffer_end ..],
    ).map_err(BlockProcessJobError::CommitTagDeserialize)?;
    if commit_tag.block_id != block_header.block_id {
        return Err(BlockProcessJobError::CorruptedData(CorruptedDataError::CommitTagBlockIdMismatch {
            offset,
            block_id_expected: block_header.block_id,
            block_id_actual: commit_tag.block_id,
        }));
    }
    let block_bytes = block_bytes.freeze_range(block_buffer_start .. block_buffer_end);
    let block_id = block_header.block_id;

    let crc_expected = block::crc(&block_bytes);
    if commit_tag.crc != crc_expected {
        return Err(BlockProcessJobError::CorruptedData(CorruptedDataError::CommitTagCrcMismatch {
            offset,
            crc_expected,
            crc_actual: commit_tag.crc,
        }));
    }

    Ok(BlockProcessJobDone { block_id, block_bytes, block_crc: commit_tag.crc, })
}
