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
    BytesPool,
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
    task: RequestTask<C>,
    reply_tx: oneshot::Sender<DoneTask<C>>,
}

pub enum RequestTask<C> where C: Context {
    WriteBlock(RequestTaskWriteBlock<C>),
    ReadBlock(task::ReadBlock<C>),
    DeleteBlock(RequestTaskDeleteBlock<C>),
}

pub struct RequestTaskWriteBlock<C> {
    pub write_block_bytes: Bytes,
    pub context: task::WriteBlockContext<C>,
}

pub struct RequestTaskDeleteBlock<C> {
    pub delete_block_bytes: Bytes,
    pub context: task::DeleteBlockContext<C>,
}

pub struct DoneTask<C> where C: Context {
    pub current_offset: u64,
    pub block_id: block::Id,
    pub task_done: RequestDoneTask<C>,
    pub stats: InterpretStats,
}

pub enum RequestDoneTask<C> where C: Context {
    WriteBlock(task::TaskDoneWriteBlock<C::WriteBlock>),
    ReadBlock(RequestDoneTaskReadBlock<C>),
    DeleteBlock(task::TaskDoneDeleteBlock<C::DeleteBlock>),
}

pub struct RequestDoneTaskReadBlock<C> where C: Context {
    pub block_process_job_args: BlockProcessJobArgs,
    pub context: task::ReadBlockContext<C>,
}

pub type RequestReplyRx<C> = oneshot::Receiver<DoneTask<C>>;

pub struct Synced;

enum Command<C> where C: Context {
    Request(Request<C>),
    DeviceSync { reply_tx: oneshot::Sender<Synced>, },
}

#[derive(Debug)]
pub enum CreateError {
    FixedFile(fixed_file::WheelCreateError),
    Ram(ram::WheelCreateError),
}

#[derive(Debug)]
pub enum OpenError {
    FixedFile(fixed_file::WheelOpenError),
}

#[derive(Debug)]
pub enum RunError {
    FixedFile(fixed_file::Error),
    Ram(ram::Error),
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
pub enum BlockPrepareWriteJobError {
    BlockHeaderSerialize(bincode::Error),
    CommitTagSerialize(bincode::Error),
    TerminatorTagSerialize(bincode::Error),
}

pub struct BlockPrepareWriteJobDone {
    write_block_bytes: Bytes,
}

pub struct BlockPrepareWriteJobArgs {
    block_id: block::Id,
    block_bytes: Bytes,
    block_crc: Option<u64>,
    commit: task::CommitKind,
    blocks_pool: BytesPool,
}

pub type BlockPrepareWriteJobOutput = Result<BlockPrepareWriteJobDone, BlockPrepareWriteJobError>;

pub fn block_prepare_write_job(
    BlockPrepareWriteJobArgs {
        block_id,
        block_bytes,
        block_crc,
        commit,
        blocks_pool,
    }: BlockPrepareWriteJobArgs,
)
    -> BlockPrepareWriteJobOutput
{
    let mut write_block_bytes = blocks_pool.lend();

    let block_header = storage::BlockHeader {
        block_id: block_id.clone(),
        block_size: block_bytes.len(),
        ..Default::default()
    };
    bincode::serialize_into(&mut write_block_bytes, &block_header)
        .map_err(BlockPrepareWriteJobError::BlockHeaderSerialize)?;
    write_block_bytes.extend_from_slice(&block_bytes);

    let crc = if let Some(value) = block_crc {
        value
    } else {
        block::crc(&block_bytes)
    };

    let commit_tag = storage::CommitTag {
        block_id: block_id.clone(),
        crc,
        ..Default::default()
    };
    bincode::serialize_into(&mut write_block_bytes, &commit_tag)
        .map_err(BlockPrepareWriteJobError::CommitTagSerialize)?;
    match commit {
        task::CommitKind::CommitOnly =>
            (),
        task::CommitKind::CommitAndTerminate => {
            bincode::serialize_into(&mut write_block_bytes, &storage::TerminatorTag::default())
                .map_err(BlockPrepareWriteJobError::TerminatorTagSerialize)?;
        },
    }

    Ok(BlockPrepareWriteJobDone {
        write_block_bytes: write_block_bytes.freeze(),
    })
}

#[derive(Debug)]
pub enum BlockPrepareDeleteJobError {
    TombstoneTagSerialize(bincode::Error),
    TerminatorTagSerialize(bincode::Error),
}

pub struct BlockPrepareDeleteJobDone {
    delete_block_bytes: Bytes,
}

pub struct BlockPrepareDeleteJobArgs {
    block_id: block::Id,
    commit: task::CommitKind,
    blocks_pool: BytesPool,
}

pub type BlockPrepareDeleteJobOutput = Result<BlockPrepareDeleteJobDone, BlockPrepareDeleteJobError>;

pub fn block_prepare_delete_job(
    BlockPrepareDeleteJobArgs {
        block_id,
        commit,
        blocks_pool,
    }: BlockPrepareDeleteJobArgs,
)
    -> BlockPrepareDeleteJobOutput
{
    let mut delete_block_bytes = blocks_pool.lend();

    let tombstone_tag = storage::TombstoneTag::default();
    bincode::serialize_into(&mut delete_block_bytes, &tombstone_tag)
        .map_err(BlockPrepareDeleteJobError::TombstoneTagSerialize)?;
    match commit {
        task::CommitKind::CommitOnly =>
            (),
        task::CommitKind::CommitAndTerminate => {
            bincode::serialize_into(&mut delete_block_bytes, &storage::TerminatorTag::default())
                .map_err(BlockPrepareDeleteJobError::TerminatorTagSerialize)?;
        },
    }

    Ok(BlockPrepareDeleteJobDone {
        delete_block_bytes: delete_block_bytes.freeze(),
    })
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
