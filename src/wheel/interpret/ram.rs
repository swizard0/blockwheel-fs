use std::io;

use futures::{
    channel::{
        mpsc,
    },
    StreamExt,
};

use alloc_pool::bytes::{
    BytesPool,
};

use crate::{
    context::Context,
    wheel::{
        storage,
        core::{
            task,
            performer,
        },
        interpret::{
            Pid,
            Synced,
            Command,
            Request,
            DoneTask,
            AppendTerminatorError,
            block_append_terminator,
        },
    },
    InterpretStats,
};

#[derive(Debug)]
pub enum Error {
    AppendTerminator(AppendTerminatorError),
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

pub struct WheelData<C> where C: Context {
    pub gen_server: GenServer<C>,
    pub performer: performer::Performer<C>,
}

#[derive(Clone, Debug)]
pub struct CreateParams {
    pub init_wheel_size_bytes: usize,
}

pub struct GenServer<C> where C: Context {
    memory: Vec<u8>,
    request_tx: mpsc::Sender<Command<C>>,
    request_rx: mpsc::Receiver<Command<C>>,
    storage_layout: storage::Layout,
}

impl<C> GenServer<C> where C: Context {
    pub async fn create(
        params: CreateParams,
        performer_builder: performer::PerformerBuilderInit<C>,
    )
        -> Result<WheelData<C>, WheelCreateError>
    {
        log::debug!("creating new ram file of {:?} bytes", params.init_wheel_size_bytes);

        let mut memory = Vec::with_capacity(params.init_wheel_size_bytes);

        let wheel_header = storage::WheelHeader {
            size_bytes: params.init_wheel_size_bytes as u64,
            ..storage::WheelHeader::default()
        };
        bincode::serialize_into(&mut memory, &wheel_header)
            .map_err(WheelCreateError::HeaderSerialize)?;

        let terminator_tag = storage::TerminatorTag::default();
        bincode::serialize_into(&mut memory, &terminator_tag)
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

        let (request_tx, request_rx) = mpsc::channel(0);

        let (performer_builder, _work_block) = performer_builder.start_fill();

        Ok(WheelData {
            gen_server: GenServer {
                memory,
                request_tx,
                request_rx,
                storage_layout,
            },
            performer: performer_builder
                .finish(params.init_wheel_size_bytes),
        })
    }

    pub fn pid(&self) -> Pid<C> {
        Pid {
            request_tx: self.request_tx.clone(),
        }
    }

    pub async fn run(self, blocks_pool: BytesPool) -> Result<(), Error> {
        busyloop(
            self.request_rx,
            self.memory,
            self.storage_layout,
            blocks_pool,
        ).await
    }
}

async fn busyloop<C>(
    request_rx: mpsc::Receiver<Command<C>>,
    memory: Vec<u8>,
    storage_layout: storage::Layout,
    blocks_pool: BytesPool,
)
    -> Result<(), Error>
where C: Context,
{
    let mut stats = InterpretStats::default();

    let mut fused_request_rx = request_rx.fuse();

    let mut terminator_block_bytes = blocks_pool.lend();
    block_append_terminator(&mut terminator_block_bytes)
        .map_err(Error::AppendTerminator)?;

    let mut cursor = io::Cursor::new(memory);
    cursor.set_position(storage_layout.wheel_header_size as u64);

    loop {
        enum Event<C> { Command(C), }

        let event = Event::Command(fused_request_rx.next().await);

        match event {

            Event::Command(None) =>
                break,

            Event::Command(Some(Command::Request(Request { offset, task, reply_tx, }))) => {
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
                        slice[start .. start + write_block.write_block_bytes.len()]
                            .copy_from_slice(&write_block.write_block_bytes);
                        let written = write_block.write_block_bytes.len();

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

                        let task_done = task::Done {
                            current_offset: cursor.position(),
                            task: task::TaskDone {
                                block_id: task.block_id,
                                kind: task::TaskDoneKind::WriteBlock(task::TaskDoneWriteBlock {
                                    context: write_block.context,
                                }),
                            },
                        };
                        if let Err(_send_error) = reply_tx.send(DoneTask { task_done, stats, }) {
                            break;
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

                        let task_done = task::Done {
                            current_offset: cursor.position(),
                            task: task::TaskDone {
                                block_id: block_header.block_id,
                                kind: task::TaskDoneKind::ReadBlock(task::TaskDoneReadBlock {
                                    block_bytes,
                                    context,
                                }),
                            },
                        };
                        if let Err(_send_error) = reply_tx.send(DoneTask { task_done, stats, }) {
                            break;
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

                        let task_done = task::Done {
                            current_offset: cursor.position(),
                            task: task::TaskDone {
                                block_id: task.block_id,
                                kind: task::TaskDoneKind::DeleteBlock(task::TaskDoneDeleteBlock {
                                    context: delete_block.context,
                                }),
                            },
                        };
                        if let Err(_send_error) = reply_tx.send(DoneTask { task_done, stats, }) {
                            break;
                        }
                    },
                }
            },

            Event::Command(Some(Command::DeviceSync { reply_tx, })) =>
                if let Err(_send_error) = reply_tx.send(Synced) {
                    break;
                },

        }
    }

    log::debug!("master channel closed in interpret_loop, shutting down");
    Ok(())
}
