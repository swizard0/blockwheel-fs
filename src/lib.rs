#![forbid(unsafe_code)]

use std::{
    path::{
        PathBuf,
    },
};

use alloc_pool::{
    bytes::{
        Bytes,
        BytesPool,
    }
};

use arbeitssklave::{
    komm,
};

pub mod job;
pub mod block;
pub mod stress;

mod wheel;
mod proto;
mod storage;
mod context;
mod blockwheel_context;

#[cfg(test)]
mod tests;

#[derive(Clone, Debug)]
pub struct Params {
    pub interpreter: InterpreterParams,
    pub work_block_size_bytes: usize,
    pub lru_cache_size_bytes: usize,
    pub defrag_parallel_tasks_limit: usize,
}

#[derive(Clone, Debug)]
pub enum InterpreterParams {
    FixedFile(FixedFileInterpreterParams),
    Ram(RamInterpreterParams),
}

#[derive(Clone, Debug)]
pub struct FixedFileInterpreterParams {
    pub wheel_filename: PathBuf,
    pub init_wheel_size_bytes: usize,
}

#[derive(Clone, Debug)]
pub struct RamInterpreterParams {
    pub init_wheel_size_bytes: usize,
}

impl Default for Params {
    fn default() -> Params {
        Params {
            interpreter: Default::default(),
            work_block_size_bytes: 8 * 1024 * 1024,
            lru_cache_size_bytes: 16 * 1024 * 1024,
            defrag_parallel_tasks_limit: 1,
        }
    }
}

impl Default for InterpreterParams {
    fn default() -> InterpreterParams {
        InterpreterParams::FixedFile(Default::default())
    }
}

impl Default for FixedFileInterpreterParams {
    fn default() -> FixedFileInterpreterParams {
        FixedFileInterpreterParams {
            wheel_filename: "wheel".to_string().into(),
            init_wheel_size_bytes: 64 * 1024 * 1024,
        }
    }
}

impl Default for RamInterpreterParams {
    fn default() -> RamInterpreterParams {
        RamInterpreterParams {
            init_wheel_size_bytes: 64 * 1024 * 1024,
        }
    }
}

#[derive(Debug)]
pub enum Error {
    Interpreter(wheel::interpret::Error),
    Arbeitssklave(arbeitssklave::Error),
}

pub trait EchoPolicy
where Self: Send + 'static,
      Self::Info: komm::Echo<Info> + Send + 'static,
      Self::Flush: komm::Echo<Flushed> + Send + 'static,
      Self::WriteBlock: komm::Echo<Result<block::Id, RequestWriteBlockError>> + Send + 'static,
      Self::ReadBlock: komm::Echo<Result<Bytes, RequestReadBlockError>> + Send + 'static,
      Self::DeleteBlock: komm::Echo<Result<Deleted, RequestDeleteBlockError>> + Send + 'static,
      Self::IterBlocksInit: komm::Echo<IterBlocks> + Send + 'static,
      Self::IterBlocksNext: komm::Echo<IterBlocksItem> + Send + 'static,
{
    type Info;
    type Flush;
    type WriteBlock;
    type ReadBlock;
    type DeleteBlock;
    type IterBlocksInit;
    type IterBlocksNext;
}

pub struct Freie<E> where E: EchoPolicy {
    performer_sklave_freie: arbeitssklave::Freie<wheel::performer_sklave::Welt<E>, wheel::performer_sklave::Order<E>>,
}

impl<E> Default for Freie<E> where E: EchoPolicy {
    fn default() -> Self {
        Self::new()
    }
}

impl<E> Freie<E> where E: EchoPolicy {
    pub fn new() -> Self {
        Self {
            performer_sklave_freie: arbeitssklave::Freie::new(),
        }
    }

    pub fn versklaven<P>(
        self,
        params: Params,
        blocks_pool: BytesPool,
        thread_pool: &P,
    )
        -> Result<Meister<E>, Error>
    where P: edeltraud::ThreadPool<job::Job<E>> + Clone + Send + 'static,
    {
        let performer_sklave_meister =
            self.performer_sklave_freie.meister();

        let interpreter =
            wheel::interpret::Interpreter::starten(
                params,
                performer_sklave_meister,
                blocks_pool.clone(),
                thread_pool,
            )
            .map_err(Error::Interpreter)?;

        let performer_sklave_meister = self
            .performer_sklave_freie
            .versklaven(
                wheel::performer_sklave::Welt {
                    env: wheel::performer_sklave::Env {
                        interpreter,
                        blocks_pool,
                        incoming_orders: Vec::new(),
                        delayed_orders: Vec::new(),
                    },
                    kont: wheel::performer_sklave::Kont::Initialize,
                },
                thread_pool,
            )
            .map_err(Error::Arbeitssklave)?;

        Ok(Meister {
            performer_sklave_meister,
        })
    }
}

pub struct Meister<E> where E: EchoPolicy {
    performer_sklave_meister: arbeitssklave::Meister<wheel::performer_sklave::Welt<E>, wheel::performer_sklave::Order<E>>,
}

impl<E> Clone for Meister<E> where E: EchoPolicy {
    fn clone(&self) -> Self {
        Meister {
            performer_sklave_meister: self.performer_sklave_meister.clone(),
        }
    }
}

impl<E> Meister<E> where E: EchoPolicy {
    pub fn info<P>(
        &self,
        echo: <blockwheel_context::Context<E> as context::Context>::Info,
        thread_pool: &P,
    )
        -> Result<(), arbeitssklave::Error>
    where P: edeltraud::ThreadPool<job::Job<E>>
    {
        self.order(proto::Request::Info(proto::RequestInfo { context: echo, }), thread_pool)
    }

    pub fn flush<P>(
        &self,
        echo: <blockwheel_context::Context<E> as context::Context>::Flush,
        thread_pool: &P,
    )
        -> Result<(), arbeitssklave::Error>
    where P: edeltraud::ThreadPool<job::Job<E>>
    {
        self.order(proto::Request::Flush(proto::RequestFlush { context: echo, }), thread_pool)
    }

    pub fn write_block<P>(
        &self,
        block_bytes: Bytes,
        echo: <blockwheel_context::Context<E> as context::Context>::WriteBlock,
        thread_pool: &P,
    )
        -> Result<(), arbeitssklave::Error>
    where P: edeltraud::ThreadPool<job::Job<E>>
    {
        self.order(
            proto::Request::WriteBlock(
                proto::RequestWriteBlock {
                    block_bytes,
                    context: echo,
                },
            ),
            thread_pool,
        )
    }

    pub fn read_block<P>(
        &self,
        block_id: block::Id,
        echo: <blockwheel_context::Context<E> as context::Context>::ReadBlock,
        thread_pool: &P,
    )
        -> Result<(), arbeitssklave::Error>
    where P: edeltraud::ThreadPool<job::Job<E>>
    {
        self.order(
            proto::Request::ReadBlock(
                proto::RequestReadBlock {
                    block_id,
                    context: echo,
                },
            ),
            thread_pool,
        )
    }

    pub fn delete_block<P>(
        &self,
        block_id: block::Id,
        echo: <blockwheel_context::Context<E> as context::Context>::DeleteBlock,
        thread_pool: &P,
    )
        -> Result<(), arbeitssklave::Error>
    where P: edeltraud::ThreadPool<job::Job<E>>
    {
        self.order(
            proto::Request::DeleteBlock(
                proto::RequestDeleteBlock {
                    block_id,
                    context: echo,
                },
            ),
            thread_pool,
        )
    }

    pub fn iter_blocks_init<P>(
        &self,
        echo: <blockwheel_context::Context<E> as context::Context>::IterBlocksInit,
        thread_pool: &P,
    )
        -> Result<(), arbeitssklave::Error>
    where P: edeltraud::ThreadPool<job::Job<E>>
    {
        self.order(
            proto::Request::IterBlocksInit(
                proto::RequestIterBlocksInit {
                    context: echo,
                },
            ),
            thread_pool,
        )
    }

    pub fn iter_blocks_next<P>(
        &self,
        iterator_next: IterBlocksIterator,
        echo: <blockwheel_context::Context<E> as context::Context>::IterBlocksNext,
        thread_pool: &P,
    )
        -> Result<(), arbeitssklave::Error>
    where P: edeltraud::ThreadPool<job::Job<E>>
    {
        self.order(
            proto::Request::IterBlocksNext(
                proto::RequestIterBlocksNext {
                    iterator_next,
                    context: echo,
                },
            ),
            thread_pool,
        )
    }

    fn order<P>(
        &self,
        request: proto::Request<blockwheel_context::Context<E>>,
        thread_pool: &P,
    )
        -> Result<(), arbeitssklave::Error>
    where P: edeltraud::ThreadPool<job::Job<E>>
    {
        self.performer_sklave_meister
            .befehl(
                wheel::performer_sklave::Order::Request(request),
                thread_pool,
            )
    }
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum RequestWriteBlockError {
    NoSpaceLeft,
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum RequestReadBlockError {
    NotFound,
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum RequestDeleteBlockError {
    NotFound,
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Default, Debug)]
pub struct Info {
    pub blocks_count: usize,
    pub wheel_size_bytes: usize,
    pub service_bytes_used: usize,
    pub data_bytes_used: usize,
    pub defrag_write_pending_bytes: usize,
    pub bytes_free: usize,
    pub read_block_cache_hits: usize,
    pub read_block_cache_misses: usize,
    pub interpret_stats: InterpretStats,
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Default, Debug)]
pub struct InterpretStats {
    pub count_total: usize,
    pub count_no_seek: usize,
    pub count_seek_forward: usize,
    pub count_seek_backward: usize,
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct Deleted;

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct Flushed;

#[derive(PartialEq, Eq, Debug)]
pub struct IterBlocks {
    pub blocks_total_count: usize,
    pub blocks_total_size: usize,
    pub iterator_next: IterBlocksIterator,
}

#[derive(PartialEq, Eq, Debug)]
pub enum IterBlocksItem {
    Block {
        block_id: block::Id,
        block_bytes: Bytes,
        iterator_next: IterBlocksIterator,
    },
    NoMoreBlocks,
}

#[derive(PartialEq, Eq, Debug)]
pub struct IterBlocksIterator {
    block_id_from: block::Id,
}
