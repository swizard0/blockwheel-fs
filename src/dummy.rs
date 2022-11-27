use std::{
    sync::{
        Arc,
        Mutex,
    },
    collections::{
        BTreeMap,
    },
};

use alloc_pool::{
    bytes::{
        Bytes,
    },
};
use arbeitssklave::komm::Echo;

use crate::{
    block,
    context,
    blockwheel_context,
    Info,
    Error,
    Params,
    Flushed,
    Deleted,
    EchoPolicy,
    IterBlocks,
    IterBlocksItem,
    InterpretStats,
    IterBlocksIterator,
    RequestReadBlockError,
    RequestWriteBlockError,
    RequestDeleteBlockError,
};

pub struct Meister {
    storage: Arc<Mutex<Storage>>,
    params: Params,
}

struct Storage {
    next_block_id: block::Id,
    total_blocks_size: usize,
    map: BTreeMap<block::Id, Bytes>,
}

impl Meister {
    pub(super) fn new(params: Params) -> Meister {
        Meister {
            storage: Arc::new(Mutex::new(Storage {
                next_block_id: block::Id::init(),
                total_blocks_size: 0,
                map: BTreeMap::new(),
            })),
            params,
        }
    }
}

impl Clone for Meister {
    fn clone(&self) -> Self {
        Meister {
            storage: self.storage.clone(),
            params: self.params.clone(),
        }
    }
}

impl Meister {

    pub(super) fn info<E>(
        &self,
        echo: <blockwheel_context::Context<E> as context::Context>::Info,
    )
        -> Result<(), Error>
    where E: EchoPolicy,
    {
        let (blocks_count, wheel_size_bytes) = {
            let storage_lock = self.storage.lock()
                .map_err(|_| Error::StorageMutexPoisoned)?;
            (storage_lock.map.len(), storage_lock.total_blocks_size)
        };

        let info = Info {
            blocks_count,
            wheel_size_bytes,
            service_bytes_used: 0,
            data_bytes_used: wheel_size_bytes,
            defrag_write_pending_bytes: 0,
            bytes_free: self.params.interpreter.init_wheel_size_bytes() - wheel_size_bytes,
            read_block_cache_hits: 0,
            read_block_cache_misses: 0,
            interpret_stats: InterpretStats::default(),
        };
        echo.commit_echo(info)
            .map_err(Error::RequestInfoCommit)
    }

    pub(super) fn flush<E>(
        &self,
        echo: <blockwheel_context::Context<E> as context::Context>::Flush,
    )
        -> Result<(), Error>
    where E: EchoPolicy,
    {
        echo.commit_echo(Flushed)
            .map_err(Error::RequestFlushCommit)
    }

    pub(super) fn write_block<E>(
        &self,
        block_bytes: Bytes,
        echo: <blockwheel_context::Context<E> as context::Context>::WriteBlock,
    )
        -> Result<(), Error>
    where E: EchoPolicy,
    {
        let write_result = {
            let block_bytes_len = block_bytes.len();
            let mut storage_lock = self.storage.lock()
                .map_err(|_| Error::StorageMutexPoisoned)?;
            if storage_lock.total_blocks_size + block_bytes_len < self.params.interpreter.init_wheel_size_bytes() {
                let block_id = storage_lock.next_block_id.clone();
                storage_lock.next_block_id = block_id.next();
                storage_lock.map.insert(block_id.clone(), block_bytes);
                storage_lock.total_blocks_size += block_bytes_len;
                Ok(block_id)
            } else {
                Err(RequestWriteBlockError::NoSpaceLeft)
            }
        };
        echo.commit_echo(write_result)
            .map_err(Error::RequestWriteBlockCommit)
    }

    pub(super) fn read_block<E>(
        &self,
        block_id: block::Id,
        echo: <blockwheel_context::Context<E> as context::Context>::ReadBlock,
    )
        -> Result<(), Error>
    where E: EchoPolicy,
    {
        let read_result = {
            let storage_lock = self.storage.lock()
                .map_err(|_| Error::StorageMutexPoisoned)?;
            match storage_lock.map.get(&block_id) {
                Some(block_bytes) =>
                    Ok(block_bytes.clone()),
                None =>
                    Err(RequestReadBlockError::NotFound),
            }
        };
        echo.commit_echo(read_result)
            .map_err(Error::RequestReadBlockCommit)
    }

    pub(super) fn delete_block<E>(
        &self,
        block_id: block::Id,
        echo: <blockwheel_context::Context<E> as context::Context>::DeleteBlock,
    )
        -> Result<(), Error>
    where E: EchoPolicy,
    {
        let delete_result = {
            let mut storage_lock = self.storage.lock()
                .map_err(|_| Error::StorageMutexPoisoned)?;
            match storage_lock.map.remove(&block_id) {
                Some(block_bytes) => {
                    storage_lock.total_blocks_size -= block_bytes.len();
                    Ok(Deleted)
                },
                None =>
                    Err(RequestDeleteBlockError::NotFound),
            }
        };
        echo.commit_echo(delete_result)
            .map_err(Error::RequestDeleteBlockCommit)
    }

    pub fn iter_blocks_init<E>(
        &self,
        echo: <blockwheel_context::Context<E> as context::Context>::IterBlocksInit,
    )
        -> Result<(), Error>
    where E: EchoPolicy,
    {
        let (blocks_total_count, blocks_total_size) = {
            let storage_lock = self.storage.lock()
                .map_err(|_| Error::StorageMutexPoisoned)?;
            (storage_lock.map.len(), storage_lock.total_blocks_size)
        };

        let iter_blocks = IterBlocks {
            blocks_total_count,
            blocks_total_size,
            iterator_next: IterBlocksIterator {
                block_id_from: block::Id::init(),
            },
        };

        echo.commit_echo(iter_blocks)
            .map_err(Error::RequestIterBlocksInitCommit)
    }

    pub fn iter_blocks_next<E>(
        &self,
        iterator_next: IterBlocksIterator,
        echo: <blockwheel_context::Context<E> as context::Context>::IterBlocksNext,
    )
        -> Result<(), Error>
    where E: EchoPolicy,
    {
        let iter_result = {
            let storage_lock = self.storage.lock()
                .map_err(|_| Error::StorageMutexPoisoned)?;
            match storage_lock.map.range(iterator_next.block_id_from ..).next() {
                Some((block_id, block_bytes)) =>
                    IterBlocksItem::Block {
                        block_id: block_id.clone(),
                        block_bytes: block_bytes.clone(),
                        iterator_next: IterBlocksIterator {
                            block_id_from: block_id.next(),
                        },
                    },
                None =>
                    IterBlocksItem::NoMoreBlocks,
            }
        };
        echo.commit_echo(iter_result)
            .map_err(Error::RequestIterBlocksNextCommit)
    }

}
