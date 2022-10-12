use bincode::{
    Options,
};

use alloc_pool::{
    bytes::{
        Bytes,
        BytesMut,
        BytesPool,
    },
};

use arbeitssklave::{
    ewig,
};

use crate::{
    job,
    block,
    context,
    storage,
    blockwheel_context::{
        Context,
    },
    wheel::{
        lru,
        performer_sklave,
        core::{
            task,
            performer,
        },
    },
    Params,
    AccessPolicy,
    InterpreterParams,
};

pub mod ram;
pub mod fixed_file;

pub struct Request<A> where A: AccessPolicy {
    offset: u64,
    task: task::Task<Context<A>>,
}

pub enum Order<A> where A: AccessPolicy {
    Request(Request<A>),
    DeviceSync { flush_context: <Context<A> as context::Context>::Flush, },
}

#[derive(Debug)]
pub enum Error {
    Ewig(ewig::Error),
    FixedFile(fixed_file::Error),
    Ram(ram::Error),
    PerformerBuilderInit(performer::BuilderError),
}

impl From<ewig::Error> for Error {
    fn from(error: ewig::Error) -> Error {
        Error::Ewig(error)
    }
}

impl From<fixed_file::Error> for Error {
    fn from(error: fixed_file::Error) -> Error {
        Error::FixedFile(error)
    }
}

impl From<ram::Error> for Error {
    fn from(error: ram::Error) -> Error {
        Error::Ram(error)
    }
}

pub struct Interpreter<A> where A: AccessPolicy {
    interpreter_meister: ewig::Meister<Order<A>, Error>
}

impl<A> Interpreter<A> where A: AccessPolicy {
    pub fn starten<P>(
        params: Params,
        performer_sklave_meister: performer_sklave::Meister<A>,
        blocks_pool: BytesPool,
        thread_pool: &P
    )
        -> Result<Self, Error>
    where P: edeltraud::ThreadPool<job::Job<A>> + Clone + Send + 'static,
    {
        let performer_builder =
            performer::PerformerBuilderInit::new(
                lru::Cache::new(params.lru_cache_size_bytes),
                if params.defrag_parallel_tasks_limit == 0 {
                    None
                } else {
                    Some(performer::DefragConfig::new(params.defrag_parallel_tasks_limit))
                },
                params.work_block_size_bytes,
            )
            .map_err(Error::PerformerBuilderInit)?;

        let thread_pool_clone = thread_pool.clone();
        let interpreter_frei = ewig::Freie::new();
        match params.interpreter {
            InterpreterParams::FixedFile(interpreter_params) => {
                let interpreter_meister = interpreter_frei
                    .versklaven_als(
                        "blockwheel_fs::wheel::interpret::fixed_file".to_string(),
                        move |sklave| {
                            fixed_file::bootstrap(
                                sklave,
                                interpreter_params,
                                performer_sklave_meister,
                                performer_builder,
                                blocks_pool,
                                thread_pool_clone,
                            )
                        },
                    )?;
                Ok(Interpreter { interpreter_meister, })
            },
            InterpreterParams::Ram(interpreter_params) => {
                let interpreter_meister = interpreter_frei
                    .versklaven_als(
                        "blockwheel_fs::wheel::interpret::ram".to_string(),
                        move |sklave| {
                            ram::bootstrap(
                                sklave,
                                interpreter_params,
                                performer_sklave_meister,
                                performer_builder,
                                blocks_pool,
                                thread_pool_clone,
                            )
                        },
                    )?;
                Ok(Interpreter { interpreter_meister, })
            },
        }
    }

    pub fn push_task(&self, offset: u64, task: task::Task<Context<A>>) -> Result<(), Error> {
        self.interpreter_meister
            .befehl(Order::Request(Request { offset, task, }))
    }

    pub fn device_sync(&self, flush_context: <Context<A> as context::Context>::Flush) -> Result<(), Error> {
        self.interpreter_meister
            .befehl(Order::DeviceSync { flush_context, })
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

pub struct BlockPrepareWriteJobArgs<A> where A: AccessPolicy {
    pub block_id: block::Id,
    pub block_bytes: Bytes,
    pub blocks_pool: BytesPool,
    pub context: performer_sklave::WriteBlockContext<A>,
    pub meister: performer_sklave::Meister<A>,
}

pub struct BlockPrepareWriteJobDone<A> where A: AccessPolicy {
    pub block_id: block::Id,
    pub write_block_bytes: task::WriteBlockBytes,
    pub context: performer_sklave::WriteBlockContext<A>,
}

#[derive(Debug)]
pub enum BlockPrepareWriteJobError {
    BlockHeaderSerialize(bincode::Error),
    CommitTagSerialize(bincode::Error),
}

pub type BlockPrepareWriteJobOutput<A> = Result<BlockPrepareWriteJobDone<A>, BlockPrepareWriteJobError>;

pub fn block_prepare_write_job<A, P>(
    BlockPrepareWriteJobArgs {
        block_id,
        block_bytes,
        blocks_pool,
        context,
        meister,
    }: BlockPrepareWriteJobArgs<A>,
    thread_pool: &P,
)
where A: AccessPolicy,
      P: edeltraud::ThreadPool<job::Job<A>>,
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
    if let Err(error) = meister.befehl(order, thread_pool) {
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
        block_id,
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

pub struct BlockPrepareDeleteJobArgs<A> where A: AccessPolicy {
    pub block_id: block::Id,
    pub blocks_pool: BytesPool,
    pub context: performer_sklave::DeleteBlockContext<A>,
    pub meister: performer_sklave::Meister<A>,
}

pub struct BlockPrepareDeleteJobDone<A> where A: AccessPolicy {
    pub block_id: block::Id,
    pub delete_block_bytes: BytesMut,
    pub context: performer_sklave::DeleteBlockContext<A>,
}

#[derive(Debug)]
pub enum BlockPrepareDeleteJobError {
    TombstoneTagSerialize(bincode::Error),
}

pub type BlockPrepareDeleteJobOutput<A> = Result<BlockPrepareDeleteJobDone<A>, BlockPrepareDeleteJobError>;

pub fn block_prepare_delete_job<A, P>(
    BlockPrepareDeleteJobArgs {
        block_id,
        blocks_pool,
        context,
        meister,
    }: BlockPrepareDeleteJobArgs<A>,
    thread_pool: &P,
)
where A: AccessPolicy,
      P: edeltraud::ThreadPool<job::Job<A>>,
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
    if let Err(error) = meister.befehl(order, thread_pool) {
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

pub struct BlockProcessReadJobArgs<A> where A: AccessPolicy {
    pub storage_layout: storage::Layout,
    pub block_header: storage::BlockHeader,
    pub block_bytes: Bytes,
    pub pending_contexts: task::queue::PendingReadContextBag,
    pub meister: performer_sklave::Meister<A>,
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

pub fn block_process_read_job<A, P>(
    BlockProcessReadJobArgs {
        storage_layout,
        block_header,
        block_bytes,
        pending_contexts,
        meister,
    }: BlockProcessReadJobArgs<A>,
    thread_pool: &P,
)
where A: AccessPolicy,
      P: edeltraud::ThreadPool<job::Job<A>>,
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
    if let Err(error) = meister.befehl(order, thread_pool) {
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
