use alloc_pool::{
    bytes::{
        Bytes,
        BytesMut,
        BytesPool,
    },
};

use alloc_pool_pack::{
    Source,
    SourceSlice,
    ReadFromSource,
    WriteToBytesMut,
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
    EchoPolicy,
    InterpreterParams,
};

pub mod ram;
pub mod fixed_file;

pub struct Request<E> where E: EchoPolicy {
    offset: u64,
    task: task::Task<Context<E>>,
}

pub enum Order<E> where E: EchoPolicy {
    Request(Request<E>),
    DeviceSync { flush_context: <Context<E> as context::Context>::Flush, },
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

pub struct Interpreter<E> where E: EchoPolicy {
    interpreter_meister: ewig::Meister<Order<E>, Error>
}

impl<E> Interpreter<E> where E: EchoPolicy {
    pub fn starten<P>(
        params: Params,
        performer_sklave_meister: performer_sklave::Meister<E>,
        blocks_pool: BytesPool,
        thread_pool: &P
    )
        -> Result<Self, Error>
    where P: edeltraud::ThreadPool<job::Job<E>> + Clone + Send + 'static,
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
                &blocks_pool,
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

    pub fn push_task(&self, offset: u64, task: task::Task<Context<E>>) -> Result<(), Error> {
        self.interpreter_meister
            .befehl(Order::Request(Request { offset, task, }))
    }

    pub fn device_sync(&self, flush_context: <Context<E> as context::Context>::Flush) -> Result<(), Error> {
        self.interpreter_meister
            .befehl(Order::DeviceSync { flush_context, })
    }
}

pub fn block_append_terminator(block_bytes: &mut BytesMut) {
    storage::TerminatorTag::default()
        .write_to_bytes_mut(block_bytes);
}

pub struct BlockPrepareWriteJobArgs<E> where E: EchoPolicy {
    pub block_id: block::Id,
    pub block_bytes: Bytes,
    pub blocks_pool: BytesPool,
    pub context: performer_sklave::WriteBlockContext<E>,
    pub meister: performer_sklave::Meister<E>,
}

pub struct BlockPrepareWriteJobDone<E> where E: EchoPolicy {
    pub block_id: block::Id,
    pub write_block_bytes: task::WriteBlockBytes,
    pub context: performer_sklave::WriteBlockContext<E>,
}

#[derive(Debug)]
pub enum BlockPrepareWriteJobError {
    BlockSizeInto(std::num::TryFromIntError),
}

pub type BlockPrepareWriteJobOutput<E> = Result<BlockPrepareWriteJobDone<E>, BlockPrepareWriteJobError>;

pub fn block_prepare_write_job<E, P>(
    BlockPrepareWriteJobArgs {
        block_id,
        block_bytes,
        blocks_pool,
        context,
        meister,
    }: BlockPrepareWriteJobArgs<E>,
    thread_pool: &P,
)
where E: EchoPolicy,
      P: edeltraud::ThreadPool<job::Job<E>>,
{
    let output =
        run_block_prepare_write_job(
            block_id.clone(),
            block_bytes,
            blocks_pool,
        )
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
        block_size: block_bytes.len().try_into()
            .map_err(BlockPrepareWriteJobError::BlockSizeInto)?,
        ..Default::default()
    };
    let block_header_bytes = alloc_pool_pack::write(&blocks_pool, &block_header);

    let commit_tag = storage::CommitTag {
        block_id,
        crc: block::crc(&block_bytes),
        ..Default::default()
    };
    let commit_tag_bytes = alloc_pool_pack::write(&blocks_pool, &commit_tag);

    Ok(RunBlockPrepareWriteJobDone {
        write_block_bytes: task::WriteBlockBytes::Composite(
            task::WriteBlockBytesComposite {
                block_header: block_header_bytes,
                block_bytes,
                commit_tag: commit_tag_bytes,
            },
        ),
    })
}

pub struct BlockPrepareDeleteJobArgs<E> where E: EchoPolicy {
    pub block_id: block::Id,
    pub blocks_pool: BytesPool,
    pub context: performer_sklave::DeleteBlockContext<E>,
    pub meister: performer_sklave::Meister<E>,
}

pub struct BlockPrepareDeleteJobDone<E> where E: EchoPolicy {
    pub block_id: block::Id,
    pub delete_block_bytes: BytesMut,
    pub context: performer_sklave::DeleteBlockContext<E>,
}

#[derive(Debug)]
pub enum BlockPrepareDeleteJobError {
}

pub type BlockPrepareDeleteJobOutput<E> = Result<BlockPrepareDeleteJobDone<E>, BlockPrepareDeleteJobError>;

pub fn block_prepare_delete_job<E, P>(
    BlockPrepareDeleteJobArgs {
        block_id,
        blocks_pool,
        context,
        meister,
    }: BlockPrepareDeleteJobArgs<E>,
    thread_pool: &P,
)
where E: EchoPolicy,
      P: edeltraud::ThreadPool<job::Job<E>>,
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
    storage::TombstoneTag::default()
        .write_to_bytes_mut(&mut delete_block_bytes);

    Ok(RunBlockPrepareDeleteJobDone { delete_block_bytes, })
}

pub struct BlockProcessReadJobArgs<E> where E: EchoPolicy {
    pub storage_layout: storage::Layout,
    pub block_header: storage::BlockHeader,
    pub block_bytes: Bytes,
    pub pending_contexts: task::queue::PendingReadContextBag,
    pub meister: performer_sklave::Meister<E>,
}

pub struct BlockProcessReadJobDone {
    pub block_id: block::Id,
    pub block_bytes: Bytes,
    pub pending_contexts: task::queue::PendingReadContextBag,
}

#[derive(Debug)]
pub enum BlockProcessReadJobError {
    BlockHeaderDeserialize(storage::ReadBlockHeaderError),
    CommitTagDeserialize(storage::ReadCommitTagError),
    CorruptedData(CorruptedDataError),
}

#[derive(Debug)]
#[allow(clippy::enum_variant_names)]
pub enum CorruptedDataError {
    BlockIdMismatch {
        block_id_expected: block::Id,
        block_id_actual: block::Id,
    },
    BlockSizeMismatch {
        block_id: block::Id,
        block_size_expected: u64,
        block_size_actual: u64,
    },
    CommitTagBlockIdMismatch {
        block_id_expected: block::Id,
        block_id_actual: block::Id,
    },
    CommitTagCrcMismatch {
        crc_expected: u64,
        crc_actual: u64,
    },
    TrailingGarbageAfterCommitTagFor {
        block_id: block::Id,
    },
}

pub type BlockProcessReadJobOutput = Result<BlockProcessReadJobDone, BlockProcessReadJobError>;

pub fn block_process_read_job<E, P>(
    BlockProcessReadJobArgs {
        storage_layout,
        block_header,
        block_bytes,
        pending_contexts,
        meister,
    }: BlockProcessReadJobArgs<E>,
    thread_pool: &P,
)
where E: EchoPolicy,
      P: edeltraud::ThreadPool<job::Job<E>>,
{
    let output =
        run_block_process_read_job(
            storage_layout,
            block_header,
            block_bytes,
        )
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
    let mut source = SourceSlice::from(&*block_bytes);
    let storage_block_header = storage::BlockHeader::read_from_source(&mut source)
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

    source.advance(storage_block_header.block_size as usize);
    let commit_tag = storage::CommitTag::read_from_source(&mut source)
        .map_err(BlockProcessReadJobError::CommitTagDeserialize)?;
    if commit_tag.block_id != block_header.block_id {
        return Err(BlockProcessReadJobError::CorruptedData(CorruptedDataError::CommitTagBlockIdMismatch {
            block_id_expected: block_header.block_id,
            block_id_actual: commit_tag.block_id,
        }));
    }
    if !source.slice().is_empty() {
        return Err(BlockProcessReadJobError::CorruptedData(CorruptedDataError::TrailingGarbageAfterCommitTagFor {
            block_id: commit_tag.block_id,
        }));
    }

    let block_bytes = block_bytes.into_subrange(
        storage_layout.block_header_size .. storage_layout.block_header_size + storage_block_header.block_size as usize,
    );
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
