#![forbid(unsafe_code)]

use std::{
    path::{
        PathBuf,
    },
};

use alloc_pool::bytes::{
    Bytes,
    BytesPool,
};

use arbeitssklave::{
    komm,
};

pub mod job;
pub mod block;
// pub mod stress;

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

pub trait AccessPolicy: Send + 'static
where Self::Order: From<komm::UmschlagAbbrechen<Self::Info>>,
      Self::Order: From<komm::Umschlag<Info, Self::Info>>,
      Self::Order: From<komm::UmschlagAbbrechen<Self::Flush>>,
      Self::Order: From<komm::Umschlag<Flushed, Self::Flush>>,
      Self::Order: From<komm::UmschlagAbbrechen<Self::WriteBlock>>,
      Self::Order: From<komm::Umschlag<Result<block::Id, RequestWriteBlockError>, Self::WriteBlock>>,
      Self::Order: From<komm::UmschlagAbbrechen<Self::ReadBlock>>,
      Self::Order: From<komm::Umschlag<Result<Bytes, RequestReadBlockError>, Self::ReadBlock>>,
      Self::Order: From<komm::UmschlagAbbrechen<Self::DeleteBlock>>,
      Self::Order: From<komm::Umschlag<Result<Deleted, RequestDeleteBlockError>, Self::DeleteBlock>>,
      Self::Order: From<komm::UmschlagAbbrechen<Self::IterBlocksInit>>,
      Self::Order: From<komm::Umschlag<IterBlocks, Self::IterBlocksInit>>,
      Self::Order: From<komm::UmschlagAbbrechen<Self::IterBlocksNext>>,
      Self::Order: From<komm::Umschlag<IterBlocksItem, Self::IterBlocksNext>>,
      Self::Order: Send + 'static,
      Self::Info: Send + 'static,
      Self::Flush: Send + 'static,
      Self::WriteBlock: Send + 'static,
      Self::ReadBlock: Send + 'static,
      Self::DeleteBlock: Send + 'static,
      Self::IterBlocksInit: Send + 'static,
      Self::IterBlocksNext: Send + 'static,
{
    type Order;
    type Info;
    type Flush;
    type WriteBlock;
    type ReadBlock;
    type DeleteBlock;
    type IterBlocksInit;
    type IterBlocksNext;
}

pub struct Freie<A> where A: AccessPolicy {
    performer_sklave_freie: arbeitssklave::Freie<wheel::performer_sklave::Welt<A>, wheel::performer_sklave::Order<A>>,
}

impl<A> Freie<A> where A: AccessPolicy {
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
        -> Result<Meister<A>, Error>
    where P: edeltraud::ThreadPool<job::Job<A>> + Clone + Send + 'static,
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

        let performer_sklave_meister = arbeitssklave::Freie::new()
            .versklaven(
                wheel::performer_sklave::Welt {
                    env: wheel::performer_sklave::Env {
                        interpreter,
                        blocks_pool,
                    },
                    kont: wheel::performer_sklave::Kont::Initialize,
                },
                thread_pool,
            )
            .map_err(Error::Arbeitssklave)?;

        Ok(Meister { performer_sklave_meister, })
    }
}

pub struct Meister<A> where A: AccessPolicy {
    performer_sklave_meister: arbeitssklave::Meister<wheel::performer_sklave::Welt<A>, wheel::performer_sklave::Order<A>>,
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

#[derive(Debug)]
pub struct IterBlocks {
    pub blocks_total_count: usize,
    pub blocks_total_size: usize,
    pub iterator_next: IterBlocksIterator,
}

#[derive(Debug)]
pub enum IterBlocksItem {
    Block {
        block_id: block::Id,
        block_bytes: Bytes,
        iterator_next: IterBlocksIterator,
    },
    NoMoreBlocks,
}

#[derive(Debug)]
pub struct IterBlocksIterator {
    block_id_from: block::Id,
}

// impl Pid {
//     pub async fn info(&mut self) -> Result<Info, ero::NoProcError> {
//         loop {
//             let (reply_tx, reply_rx) = oneshot::channel();
//             self.request_tx.send(proto::Request::Info(proto::RequestInfo { context: reply_tx, })).await
//                 .map_err(|_send_error| ero::NoProcError)?;
//             match reply_rx.await {
//                 Ok(info) =>
//                     return Ok(info),
//                 Err(oneshot::Canceled) =>
//                     (),
//             }
//         }
//     }

//     pub async fn flush(&mut self) -> Result<Flushed, ero::NoProcError> {
//         loop {
//             let (reply_tx, reply_rx) = oneshot::channel();
//             self.request_tx.send(proto::Request::Flush(proto::RequestFlush { context: reply_tx, })).await
//                 .map_err(|_send_error| ero::NoProcError)?;
//             match reply_rx.await {
//                 Ok(Flushed) =>
//                     return Ok(Flushed),
//                 Err(oneshot::Canceled) =>
//                     (),
//             }
//         }
//     }

//     pub async fn write_block(&mut self, block_bytes: Bytes) -> Result<block::Id, WriteBlockError> {
//         loop {
//             let (reply_tx, reply_rx) = oneshot::channel();
//             self.request_tx
//                 .send(proto::Request::WriteBlock(proto::RequestWriteBlock {
//                     block_bytes: block_bytes.clone(),
//                     context: reply_tx,
//                 }))
//                 .await
//                 .map_err(|_send_error| WriteBlockError::GenServer(ero::NoProcError))?;

//             match reply_rx.await {
//                 Ok(Ok(block_id)) =>
//                     return Ok(block_id),
//                 Ok(Err(blockwheel_context::RequestWriteBlockError::NoSpaceLeft)) =>
//                     return Err(WriteBlockError::NoSpaceLeft),
//                 Err(oneshot::Canceled) =>
//                     (),
//             }
//         }
//     }

//     pub async fn read_block(&mut self, block_id: block::Id) -> Result<Bytes, ReadBlockError> {
//         loop {
//             let (reply_tx, reply_rx) = oneshot::channel();
//             self.request_tx
//                 .send(proto::Request::ReadBlock(proto::RequestReadBlock {
//                     block_id: block_id.clone(),
//                     context: reply_tx,
//                 }))
//                 .await
//                 .map_err(|_send_error| ReadBlockError::GenServer(ero::NoProcError))?;

//             match reply_rx.await {
//                 Ok(Ok(block_bytes)) =>
//                     return Ok(block_bytes),
//                 Ok(Err(blockwheel_context::RequestReadBlockError::NotFound)) =>
//                     return Err(ReadBlockError::NotFound),
//                 Err(oneshot::Canceled) =>
//                     (),
//             }
//         }
//     }

//     pub async fn delete_block(&mut self, block_id: block::Id) -> Result<Deleted, DeleteBlockError> {
//         loop {
//             let (reply_tx, reply_rx) = oneshot::channel();
//             self.request_tx
//                 .send(proto::Request::DeleteBlock(proto::RequestDeleteBlock {
//                     block_id: block_id.clone(),
//                     context: reply_tx,
//                 }))
//                 .await
//                 .map_err(|_send_error| DeleteBlockError::GenServer(ero::NoProcError))?;

//             match reply_rx.await {
//                 Ok(Ok(Deleted)) =>
//                     return Ok(Deleted),
//                 Ok(Err(blockwheel_context::RequestDeleteBlockError::NotFound)) =>
//                     return Err(DeleteBlockError::NotFound),
//                 Err(oneshot::Canceled) =>
//                     (),
//             }
//         }
//     }

//     pub async fn iter_blocks(&mut self) -> Result<IterBlocks, IterBlocksError> {
//         loop {
//             let (reply_tx, reply_rx) = oneshot::channel();
//             self.request_tx
//                 .send(proto::Request::IterBlocks(proto::RequestIterBlocks {
//                     context: blockwheel_context::IterBlocksContext {
//                         reply_tx,
//                         maybe_iter_task_tx: None,
//                     },
//                 }))
//                 .await
//                 .map_err(|_send_error| IterBlocksError::GenServer(ero::NoProcError))?;

//             match reply_rx.await {
//                 Ok(iter_blocks) =>
//                     return Ok(iter_blocks),
//                 Err(oneshot::Canceled) =>
//                     (),
//             }
//         }
//     }
// }
