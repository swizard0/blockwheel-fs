use std::{
    io,
    thread,
    sync::{
        Arc,
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
        BytesPool,
    },
};

use crate::{
    context::{
        Context,
    },
    wheel::{
        storage,
        performer_sklave,
        core::{
            task,
            performer,
        },
        interpret::{
            Pid,
            Command,
            Request,
            PidInner,
            AppendTerminatorError,
            block_append_terminator,
        },
    },
    InterpretStats,
};

#[derive(Debug)]
pub enum Error {
    AppendTerminator(AppendTerminatorError),
    ThreadSpawn(io::Error),
    Arbeitssklave(arbeitssklave::Error),
}

#[derive(Debug)]
pub enum WheelCreateError {
    InitWheelSizeIsTooSmall {
        provided: usize,
        required_min: usize,
    },
    HeaderSerialize(bincode::Error),
    TerminatorTagSerialize(bincode::Error),
}

#[derive(Debug)]
pub enum TaskJoinError {
    Create(tokio::task::JoinError),
}

pub struct WheelData<C> where C: Context {
    pub sync_gen_server_init: SyncGenServerInit,
    pub performer: performer::Performer<C>,
}

#[derive(Clone, Debug)]
pub struct CreateParams {
    pub init_wheel_size_bytes: usize,
}

pub struct SyncGenServerInit {
    memory: Vec<u8>,
    storage_layout: storage::Layout,
}

pub struct SyncGenServer<C> where C: Context {
    memory: Vec<u8>,
    pid_inner: Arc<PidInner<C>>,
    storage_layout: storage::Layout,
}

impl SyncGenServerInit {
    pub fn create<C>(
        params: CreateParams,
        performer_builder: performer::PerformerBuilderInit<C>,
    )
        -> Result<WheelData<C>, WheelCreateError>
    where C: Context,
    {
        log::debug!("creating new ram file of {:?} bytes", params.init_wheel_size_bytes);

        let mut memory = Vec::with_capacity(params.init_wheel_size_bytes);

        let wheel_header = storage::WheelHeader {
            size_bytes: params.init_wheel_size_bytes as u64,
            ..storage::WheelHeader::default()
        };
        storage::bincode_options()
            .serialize_into(&mut memory, &wheel_header)
            .map_err(WheelCreateError::HeaderSerialize)?;

        let terminator_tag = storage::TerminatorTag::default();
        storage::bincode_options()
            .serialize_into(&mut memory, &terminator_tag)
            .map_err(WheelCreateError::TerminatorTagSerialize)?;

        let min_wheel_file_size = performer_builder.storage_layout().wheel_header_size
            + performer_builder.storage_layout().terminator_tag_size;
        assert_eq!(memory.len(), min_wheel_file_size);
        let size_bytes_total = params.init_wheel_size_bytes;
        if size_bytes_total < min_wheel_file_size {
            return Err(WheelCreateError::InitWheelSizeIsTooSmall {
                provided: size_bytes_total,
                required_min: min_wheel_file_size,
            });
        }

        memory.resize(size_bytes_total, 0);

        log::debug!("ram file create success");
        let storage_layout = performer_builder.storage_layout().clone();

        let (performer_builder, _work_block) = performer_builder.start_fill();

        Ok(WheelData {
            sync_gen_server_init: SyncGenServerInit {
                memory,
                storage_layout,
            },
            performer: performer_builder
                .finish(params.init_wheel_size_bytes),
        })
    }

    pub fn finish<C>(self) -> SyncGenServer<C> where C: Context {
        let pid_inner = Arc::new(PidInner::new());

        SyncGenServer {
            memory: self.memory,
            storage_layout: self.storage_layout,
            pid_inner,
        }
    }
}

impl<C> SyncGenServer<C> where C: Context {
    pub fn pid(&self) -> Pid<C> {
        Pid {
            inner: self.pid_inner.clone(),
        }
    }

    pub fn run<F, E, P, J, W>(
        self,
        meister: arbeitssklave::Meister<W, performer_sklave::Order<C>>,
        thread_pool: P,
        blocks_pool: BytesPool,
        error_tx: oneshot::Sender<E>,
        error_map: F,
    )
        -> Result<(), Error>
    where F: FnOnce(Error) -> E + Send + 'static,
          E: Send + 'static,
          C: 'static,
          C::Info: Send,
          C::WriteBlock: Send,
          C::ReadBlock: Send,
          C::DeleteBlock: Send,
          C::IterBlocks: Send,
          C::IterBlocksStream: Send,
          C::Flush: Send,
          P: edeltraud::ThreadPool<J> + Send + 'static,
          J: edeltraud::Job<Output = ()> + From<arbeitssklave::SklaveJob<W, performer_sklave::Order<C>>>,
          W: Send + 'static,
    {
        thread::Builder::new()
            .name("wheel::interpret::ram".to_string())
            .spawn(move || {
                let result = busyloop(
                    self.pid_inner,
                    self.memory,
                    self.storage_layout,
                    meister,
                    thread_pool,
                    blocks_pool,
                );
                if let Err(error) = result {
                    log::error!("wheel::interpret::ram terminated with {:?}", error);
                    error_tx.send(error_map(error)).ok();
                }
            })
            .map_err(Error::ThreadSpawn)?;
        Ok(())
    }
}

fn busyloop<C, P, J, W>(
    pid_inner: Arc<PidInner<C>>,
    memory: Vec<u8>,
    storage_layout: storage::Layout,
    meister: arbeitssklave::Meister<W, performer_sklave::Order<C>>,
    thread_pool: P,
    blocks_pool: BytesPool,
)
    -> Result<(), Error>
where C: Context,
      P: edeltraud::ThreadPool<J>,
      J: edeltraud::Job<Output = ()> + From<arbeitssklave::SklaveJob<W, performer_sklave::Order<C>>>,
{
    let mut stats = InterpretStats::default();

    let mut terminator_block_bytes = blocks_pool.lend();
    block_append_terminator(&mut terminator_block_bytes)
        .map_err(Error::AppendTerminator)?;

    let mut cursor = io::Cursor::new(memory);
    cursor.set_position(storage_layout.wheel_header_size as u64);

    loop {
        enum Event<C> { Command(C), }
        let event = if Arc::strong_count(&pid_inner) > 1 {
            Event::Command(Some(pid_inner.acquire()))
        } else {
            Event::Command(None)
        };

        match event {

            Event::Command(None) =>
                break,

            Event::Command(Some(Command::Request(Request { offset, task, }))) => {
                stats.count_total += 1;

                if cursor.position() != offset {
                    if cursor.position() < offset {
                        stats.count_seek_forward += 1;
                    } else if cursor.position() > offset {
                        stats.count_seek_backward += 1;
                    }
                    cursor.set_position(offset);
                } else {
                    stats.count_no_seek += 1;
                }

                match task.kind {
                    task::TaskKind::WriteBlock(write_block) => {
                        let start = cursor.position() as usize;
                        let slice = cursor.get_mut();
                        let written = match write_block.write_block_bytes {
                            task::WriteBlockBytes::Chunk(write_block_bytes) => {
                                slice[start .. start + write_block_bytes.len()]
                                    .copy_from_slice(&write_block_bytes);
                                write_block_bytes.len()
                            },
                            task::WriteBlockBytes::Composite(task::WriteBlockBytesComposite { block_header, block_bytes, commit_tag, }) => {
                                let mut offset = start;
                                slice[offset .. offset + block_header.len()]
                                    .copy_from_slice(&block_header);
                                offset += block_header.len();
                                slice[offset .. offset + block_bytes.len()]
                                    .copy_from_slice(&block_bytes);
                                offset += block_bytes.len();
                                slice[offset .. offset + commit_tag.len()]
                                    .copy_from_slice(&commit_tag);
                                offset += commit_tag.len();
                                offset - start
                            },
                        };

                        match write_block.commit {
                            task::Commit::None =>
                                (),
                            task::Commit::WithTerminator => {
                                slice[start + written .. start + written + terminator_block_bytes.len()]
                                    .copy_from_slice(&terminator_block_bytes);
                                // note: do not count terminator length in cursor in order to overwrite it during next write
                            }
                        }

                        cursor.set_position(start as u64 + written as u64);

                        let order = performer_sklave::Order::TaskDoneStats(
                            performer_sklave::OrderTaskDoneStats {
                                task_done: task::Done {
                                    current_offset: cursor.position(),
                                    task: task::TaskDone {
                                        block_id: task.block_id,
                                        kind: task::TaskDoneKind::WriteBlock(task::TaskDoneWriteBlock {
                                            context: write_block.context,
                                        }),
                                    },
                                },
                                stats,
                            },
                        );
                        match meister.order(order, &thread_pool) {
                            Ok(()) =>
                               (),
                            Err(arbeitssklave::Error::Terminated) =>
                                break,
                            Err(error) =>
                                return Err(Error::Arbeitssklave(error)),
                        }
                    },

                    task::TaskKind::ReadBlock(task::ReadBlock { block_header, context, }) => {
                        let total_chunk_size = storage_layout.data_size_block_min()
                            + block_header.block_size;
                        let mut block_bytes = blocks_pool.lend();
                        block_bytes.resize(total_chunk_size, 0);
                        let start = cursor.position() as usize;
                        let slice = cursor.get_ref();
                        block_bytes.copy_from_slice(&slice[start .. start + total_chunk_size]);
                        cursor.set_position(start as u64 + block_bytes.len() as u64);

                        let order = performer_sklave::Order::TaskDoneStats(
                            performer_sklave::OrderTaskDoneStats {
                                task_done: task::Done {
                                    current_offset: cursor.position(),
                                    task: task::TaskDone {
                                        block_id: block_header.block_id,
                                        kind: task::TaskDoneKind::ReadBlock(task::TaskDoneReadBlock {
                                            block_bytes,
                                            context,
                                        }),
                                    },
                                },
                                stats,
                            },
                        );
                        match meister.order(order, &thread_pool) {
                            Ok(()) =>
                               (),
                            Err(arbeitssklave::Error::Terminated) =>
                                break,
                            Err(error) =>
                                return Err(Error::Arbeitssklave(error)),
                        }
                    },

                    task::TaskKind::DeleteBlock(delete_block) => {
                        let start = cursor.position() as usize;
                        let slice = cursor.get_mut();
                        slice[start .. start + delete_block.delete_block_bytes.len()]
                            .copy_from_slice(&delete_block.delete_block_bytes);
                        let written = delete_block.delete_block_bytes.len();

                        match delete_block.commit {
                            task::Commit::None =>
                                (),
                            task::Commit::WithTerminator => {
                                slice[start + written .. start + written + terminator_block_bytes.len()]
                                    .copy_from_slice(&terminator_block_bytes);
                                // note: do not count terminator length in cursor in order to overwrite it during next write
                            }
                        }

                        cursor.set_position(start as u64 + written as u64);

                        let order = performer_sklave::Order::TaskDoneStats(
                            performer_sklave::OrderTaskDoneStats {
                                task_done: task::Done {
                                    current_offset: cursor.position(),
                                    task: task::TaskDone {
                                        block_id: task.block_id,
                                        kind: task::TaskDoneKind::DeleteBlock(task::TaskDoneDeleteBlock {
                                            context: delete_block.context,
                                        }),
                                    },
                                },
                                stats,
                            },
                        );
                        match meister.order(order, &thread_pool) {
                            Ok(()) =>
                               (),
                            Err(arbeitssklave::Error::Terminated) =>
                                break,
                            Err(error) =>
                                return Err(Error::Arbeitssklave(error)),
                        }
                    },
                }
            },

            Event::Command(Some(Command::DeviceSync { flush_context, })) => {
                let order = performer_sklave::Order::DeviceSyncDone(
                    performer_sklave::OrderDeviceSyncDone {
                        flush_context,
                    },
                );
                match meister.order(order, &thread_pool) {
                    Ok(()) =>
                        (),
                    Err(arbeitssklave::Error::Terminated) =>
                        break,
                    Err(error) =>
                        return Err(Error::Arbeitssklave(error)),
                }
            },

        }
    }

    log::debug!("master channel closed in interpret_loop, shutting down");
    Ok(())
}
