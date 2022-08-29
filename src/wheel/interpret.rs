use std::{
    sync::{
        Arc,
        Mutex,
        Condvar,
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
    job,
    context::{
        Context,
    },
    wheel::{
        block,
        storage,
        performer_sklave,
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
}

enum Command<C> where C: Context {
    Request(Request<C>),
    DeviceSync { flush_context: C::Flush, },
    Terminate,
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
    pub fn push_request(&self, offset: u64, task: task::Task<C>) -> Result<(), ero::NoProcError> {
        if Arc::strong_count(&self.inner) > 1 {
            self.inner.schedule(Command::Request(Request { offset, task, }));
            Ok(())
        } else {
            Err(ero::NoProcError)
        }
    }

    pub fn device_sync(&self, flush_context: C::Flush) -> Result<(), ero::NoProcError> {
        if Arc::strong_count(&self.inner) > 1 {
            self.inner.schedule(Command::DeviceSync { flush_context, });
            Ok(())
        } else {
            return Err(ero::NoProcError);
        }
    }
}

impl<C> Drop for Pid<C> where C: Context {
    fn drop(&mut self) {
        if Arc::strong_count(&self.inner) > 1 {
            self.inner.schedule(Command::Terminate);
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

pub struct BlockPrepareWriteJobArgs {
    pub block_id: block::Id,
    pub block_bytes: Bytes,
    pub blocks_pool: BytesPool,
    pub context: performer_sklave::WriteBlockContext,
    pub meister: performer_sklave::Meister,
}

pub struct BlockPrepareWriteJobDone {
    pub block_id: block::Id,
    pub write_block_bytes: task::WriteBlockBytes,
    pub context: performer_sklave::WriteBlockContext,
}

#[derive(Debug)]
pub enum BlockPrepareWriteJobError {
    BlockHeaderSerialize(bincode::Error),
    CommitTagSerialize(bincode::Error),
}

pub type BlockPrepareWriteJobOutput = Result<BlockPrepareWriteJobDone, BlockPrepareWriteJobError>;

pub fn block_prepare_write_job<P>(
    BlockPrepareWriteJobArgs {
        block_id,
        block_bytes,
        blocks_pool,
        context,
        meister,
    }: BlockPrepareWriteJobArgs,
    thread_pool: &P,
)
where P: edeltraud::ThreadPool<job::Job>
{
    let output = run_block_prepare_write_job(block_id.clone(), block_bytes, blocks_pool)
        .map(|RunBlockPrepareWriteJobDone { write_block_bytes, }| {
            BlockPrepareWriteJobDone {
                block_id,
                write_block_bytes,
                context,
            }
        });
    let order = performer_sklave::Order::PreparedWriteBlockDone(
        performer_sklave::OrderPreparedWriteBlockDone { output, },
    );
    if let Err(error) = meister.order(order, thread_pool) {
        log::warn!("arbeitssklave error during block_prepare_write_job respond: {error:?}");
    }
}

struct RunBlockPrepareWriteJobDone {
    write_block_bytes: task::WriteBlockBytes,
}

fn run_block_prepare_write_job(
    block_id: block::Id,
    block_bytes: Bytes,
    blocks_pool: BytesPool,
)
    -> Result<RunBlockPrepareWriteJobDone, BlockPrepareWriteJobError>
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
        block_id: block_id,
        crc: block::crc(&block_bytes),
        ..Default::default()
    };
    let mut commit_tag_bytes = blocks_pool.lend();
    storage::bincode_options()
        .serialize_into(&mut **commit_tag_bytes, &commit_tag)
        .map_err(BlockPrepareWriteJobError::CommitTagSerialize)?;

    Ok(RunBlockPrepareWriteJobDone {
        write_block_bytes: task::WriteBlockBytes::Composite(
            task::WriteBlockBytesComposite {
                block_header: block_header_bytes.freeze(),
                block_bytes,
                commit_tag: commit_tag_bytes.freeze(),
            },
        ),
    })
}

pub struct BlockPrepareDeleteJobArgs {
    pub block_id: block::Id,
    pub blocks_pool: BytesPool,
    pub context: performer_sklave::DeleteBlockContext,
    pub meister: performer_sklave::Meister,
}

pub struct BlockPrepareDeleteJobDone {
    pub block_id: block::Id,
    pub delete_block_bytes: BytesMut,
    pub context: performer_sklave::DeleteBlockContext,
}

#[derive(Debug)]
pub enum BlockPrepareDeleteJobError {
    TombstoneTagSerialize(bincode::Error),
}

pub type BlockPrepareDeleteJobOutput = Result<BlockPrepareDeleteJobDone, BlockPrepareDeleteJobError>;

pub fn block_prepare_delete_job<P>(
    BlockPrepareDeleteJobArgs {
        block_id,
        blocks_pool,
        context,
        meister,
    }: BlockPrepareDeleteJobArgs,
    thread_pool: &P,
)
where P: edeltraud::ThreadPool<job::Job>
{
    let output = run_block_prepare_delete_job(blocks_pool)
        .map(|RunBlockPrepareDeleteJobDone { delete_block_bytes, }| {
            BlockPrepareDeleteJobDone {
                block_id,
                delete_block_bytes,
                context,
            }
        });
    let order = performer_sklave::Order::PreparedDeleteBlockDone(
        performer_sklave::OrderPreparedDeleteBlockDone { output, },
    );
    if let Err(error) = meister.order(order, thread_pool) {
        log::warn!("arbeitssklave error during block_prepare_delete_job respond: {error:?}");
    }
}

struct RunBlockPrepareDeleteJobDone {
    delete_block_bytes: BytesMut,
}

fn run_block_prepare_delete_job(
    blocks_pool: BytesPool,
)
    -> Result<RunBlockPrepareDeleteJobDone, BlockPrepareDeleteJobError>
{
    let mut delete_block_bytes = blocks_pool.lend();

    let tombstone_tag = storage::TombstoneTag::default();
    storage::bincode_options()
        .serialize_into(&mut **delete_block_bytes, &tombstone_tag)
        .map_err(BlockPrepareDeleteJobError::TombstoneTagSerialize)?;

    Ok(RunBlockPrepareDeleteJobDone { delete_block_bytes, })
}

pub struct BlockProcessReadJobArgs {
    pub storage_layout: storage::Layout,
    pub block_header: storage::BlockHeader,
    pub block_bytes: Bytes,
    pub pending_contexts: task::queue::PendingReadContextBag,
    pub meister: performer_sklave::Meister,
}

pub struct BlockProcessReadJobDone {
    pub block_id: block::Id,
    pub block_bytes: Bytes,
    pub pending_contexts: task::queue::PendingReadContextBag,
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

pub fn block_process_read_job<P>(
    BlockProcessReadJobArgs {
        storage_layout,
        block_header,
        block_bytes,
        pending_contexts,
        meister,
    }: BlockProcessReadJobArgs,
    thread_pool: &P,
)
where P: edeltraud::ThreadPool<job::Job>
{
    let output = run_block_process_read_job(storage_layout, block_header, block_bytes)
        .map(|RunBlockProcessReadJobDone { block_id, block_bytes, }| {
            BlockProcessReadJobDone {
                block_id,
                block_bytes,
                pending_contexts,
            }
        });
    let order = performer_sklave::Order::ProcessReadBlockDone(
        performer_sklave::OrderProcessReadBlockDone { output, },
    );
    if let Err(error) = meister.order(order, thread_pool) {
        log::warn!("arbeitssklave error during block_process_read_job respond: {error:?}");
    }
}

struct RunBlockProcessReadJobDone {
    block_id: block::Id,
    block_bytes: Bytes,
}

fn run_block_process_read_job(
    storage_layout: storage::Layout,
    block_header: storage::BlockHeader,
    block_bytes: Bytes,
)
    -> Result<RunBlockProcessReadJobDone, BlockProcessReadJobError>
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

    Ok(RunBlockProcessReadJobDone { block_id, block_bytes, })
}
