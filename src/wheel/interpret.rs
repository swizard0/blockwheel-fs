use std::{
    sync::{
        Arc,
        Mutex,
        Condvar,
    },
};

use futures::{
    channel::{
        oneshot,
    },
};

use bincode::Options;

use alloc_pool::{
    bytes::{
        Bytes,
        BytesMut,
        BytesPool,
    },
};

use crate::{
    InterpretStats,
    context::{
        Context,
    },
    wheel::{
        block,
        storage,
        core::{
            task,
        },
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

#[derive(Debug)]
pub enum TaskJoinError {
    FixedFile(fixed_file::TaskJoinError),
    Ram(ram::TaskJoinError),
}

#[derive(Clone)]
pub struct Pid<C> where C: Context {
    inner: Arc<PidInner<C>>,
}

struct PidInner<C> where C: Context {
    commands_queue: Mutex<Vec<Command<C>>>,
    condvar: Condvar,
}

impl<C> PidInner<C> where C: Context {
    fn new() -> Self {
        Self {
            commands_queue: Mutex::new(Vec::with_capacity(2)),
            condvar: Condvar::new(),
        }
    }

    fn schedule(&self, command: Command<C>) {
        let mut commands_queue = self.commands_queue.lock().unwrap();
        commands_queue.push(command);
        self.condvar.notify_one();
    }

    fn acquire(&self) -> Command<C> {
        let mut commands_queue = self.commands_queue.lock().unwrap();
        loop {
            if let Some(task) = commands_queue.pop() {
                return task;
            }
            commands_queue = self.condvar.wait(commands_queue).unwrap();
        }
    }
}

impl<C> Pid<C> where C: Context {
    pub fn push_request(&self, offset: u64, task: task::Task<C>) -> Result<RequestReplyRx<C>, ero::NoProcError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        if Arc::strong_count(&self.inner) > 1 {
            self.inner.schedule(Command::Request(Request { offset, task, reply_tx, }));
            Ok(reply_rx)
        } else {
            Err(ero::NoProcError)
        }
    }

    pub async fn device_sync(&mut self) -> Result<Synced, ero::NoProcError> {
        loop {
            let (reply_tx, reply_rx) = oneshot::channel();
            if Arc::strong_count(&self.inner) > 1 {
                self.inner.schedule(Command::DeviceSync { reply_tx, });
                match reply_rx.await {
                    Ok(Synced) =>
                        return Ok(Synced),
                    Err(oneshot::Canceled) =>
                        (),
                }
            } else {
                return Err(ero::NoProcError);
            }
        }
    }
}

#[derive(Debug)]
pub enum AppendTerminatorError {
    TerminatorTagSerialize(bincode::Error),
}

pub fn block_append_terminator(block_bytes: &mut BytesMut) -> Result<(), AppendTerminatorError> {
    storage::bincode_options()
        .serialize_into(&mut ***block_bytes, &storage::TerminatorTag::default())
        .map_err(AppendTerminatorError::TerminatorTagSerialize)
}

#[derive(Debug)]
pub enum BlockPrepareWriteJobError {
    BlockHeaderSerialize(bincode::Error),
    CommitTagSerialize(bincode::Error),
}

pub struct BlockPrepareWriteJobDone {
    pub write_block_bytes: task::WriteBlockBytes,
}

pub struct BlockPrepareWriteJobArgs {
    pub block_id: block::Id,
    pub block_bytes: Bytes,
    pub blocks_pool: BytesPool,
}

pub type BlockPrepareWriteJobOutput = Result<BlockPrepareWriteJobDone, BlockPrepareWriteJobError>;

pub fn block_prepare_write_job(
    BlockPrepareWriteJobArgs {
        block_id,
        block_bytes,
        blocks_pool,
    }: BlockPrepareWriteJobArgs,
)
    -> BlockPrepareWriteJobOutput
{
    let block_header = storage::BlockHeader {
        block_id: block_id.clone(),
        block_size: block_bytes.len(),
        ..Default::default()
    };
    let mut block_header_bytes = blocks_pool.lend();
    storage::bincode_options()
        .serialize_into(&mut **block_header_bytes, &block_header)
        .map_err(BlockPrepareWriteJobError::BlockHeaderSerialize)?;

    let commit_tag = storage::CommitTag {
        block_id: block_id.clone(),
        crc: block::crc(&block_bytes),
        ..Default::default()
    };
    let mut commit_tag_bytes = blocks_pool.lend();
    storage::bincode_options()
        .serialize_into(&mut **commit_tag_bytes, &commit_tag)
        .map_err(BlockPrepareWriteJobError::CommitTagSerialize)?;

    Ok(BlockPrepareWriteJobDone {
        write_block_bytes: task::WriteBlockBytes::Composite(
            task::WriteBlockBytesComposite {
                block_header: block_header_bytes.freeze(),
                block_bytes,
                commit_tag: commit_tag_bytes.freeze(),
            },
        ),
    })
}

#[derive(Debug)]
pub enum BlockPrepareDeleteJobError {
    TombstoneTagSerialize(bincode::Error),
}

pub struct BlockPrepareDeleteJobDone {
    pub delete_block_bytes: BytesMut,
}

pub struct BlockPrepareDeleteJobArgs {
    pub blocks_pool: BytesPool,
}

pub type BlockPrepareDeleteJobOutput = Result<BlockPrepareDeleteJobDone, BlockPrepareDeleteJobError>;

pub fn block_prepare_delete_job(
    BlockPrepareDeleteJobArgs {
        blocks_pool,
    }: BlockPrepareDeleteJobArgs,
)
    -> BlockPrepareDeleteJobOutput
{
    let mut delete_block_bytes = blocks_pool.lend();

    let tombstone_tag = storage::TombstoneTag::default();
    storage::bincode_options()
        .serialize_into(&mut **delete_block_bytes, &tombstone_tag)
        .map_err(BlockPrepareDeleteJobError::TombstoneTagSerialize)?;

    Ok(BlockPrepareDeleteJobDone { delete_block_bytes, })
}

#[derive(Debug)]
pub enum BlockProcessReadJobError {
    BlockHeaderDeserialize(bincode::Error),
    CommitTagDeserialize(bincode::Error),
    CorruptedData(CorruptedDataError),
}

#[derive(Debug)]
pub enum CorruptedDataError {
    BlockIdMismatch {
        block_id_expected: block::Id,
        block_id_actual: block::Id,
    },
    BlockSizeMismatch {
        block_id: block::Id,
        block_size_expected: usize,
        block_size_actual: usize,
    },
    CommitTagBlockIdMismatch {
        block_id_expected: block::Id,
        block_id_actual: block::Id,
    },
    CommitTagCrcMismatch {
        crc_expected: u64,
        crc_actual: u64,
    },
}

pub type BlockProcessReadJobOutput = Result<BlockProcessReadJobDone, BlockProcessReadJobError>;

pub struct BlockProcessReadJobDone {
    pub block_id: block::Id,
    pub block_bytes: Bytes,
}

pub struct BlockProcessReadJobArgs {
    pub storage_layout: storage::Layout,
    pub block_header: storage::BlockHeader,
    pub block_bytes: Bytes,
}

pub fn block_process_read_job(
    BlockProcessReadJobArgs {
        storage_layout,
        block_header,
        block_bytes,
    }: BlockProcessReadJobArgs,
)
    -> BlockProcessReadJobOutput
{
    let block_buffer_start = storage_layout.block_header_size;
    let block_buffer_end = block_bytes.len() - storage_layout.commit_tag_size;

    let storage_block_header: storage::BlockHeader = storage::bincode_options()
        .deserialize_from(&block_bytes[.. block_buffer_start])
        .map_err(BlockProcessReadJobError::BlockHeaderDeserialize)?;
    if storage_block_header.block_id != block_header.block_id {
        return Err(BlockProcessReadJobError::CorruptedData(CorruptedDataError::BlockIdMismatch {
            block_id_expected: block_header.block_id,
            block_id_actual: storage_block_header.block_id,
        }));
    }

    if storage_block_header.block_size != block_header.block_size {
        return Err(BlockProcessReadJobError::CorruptedData(CorruptedDataError::BlockSizeMismatch {
            block_id: block_header.block_id,
            block_size_expected: block_header.block_size,
            block_size_actual: storage_block_header.block_size,
        }));
    }
    let commit_tag: storage::CommitTag = storage::bincode_options()
        .deserialize_from(&block_bytes[block_buffer_end ..])
        .map_err(BlockProcessReadJobError::CommitTagDeserialize)?;
    if commit_tag.block_id != block_header.block_id {
        return Err(BlockProcessReadJobError::CorruptedData(CorruptedDataError::CommitTagBlockIdMismatch {
            block_id_expected: block_header.block_id,
            block_id_actual: commit_tag.block_id,
        }));
    }
    let block_bytes = block_bytes.subrange(block_buffer_start .. block_buffer_end);
    let block_id = block_header.block_id;

    let crc_expected = block::crc(&block_bytes);
    if commit_tag.crc != crc_expected {
        return Err(BlockProcessReadJobError::CorruptedData(CorruptedDataError::CommitTagCrcMismatch {
            crc_expected,
            crc_actual: commit_tag.crc,
        }));
    }

    Ok(BlockProcessReadJobDone { block_id, block_bytes, })
}
