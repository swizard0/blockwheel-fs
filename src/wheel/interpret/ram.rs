use std::io;

use futures::{
    stream::{
        FuturesUnordered,
    },
    channel::{
        mpsc,
    },
    select,
    StreamExt,
};

use edeltraud::{
    Edeltraud,
};

use crate::{
    job,
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
            BlockProcessJobArgs,
            BlockProcessJobDone,
            BlockProcessJobError,
        },
    },
    InterpretStats,
};

#[derive(Debug)]
pub enum Error {
    BlockHeaderSerialize(bincode::Error),
    CommitTagSerialize(bincode::Error),
    TombstoneTagSerialize(bincode::Error),
    WheelPeerLost,
    ThreadPoolGone,
    BlockProcessJob(BlockProcessJobError),
}

#[derive(Debug)]
pub enum WheelCreateError {
    InitWheelSizeIsTooSmall {
        provided: usize,
        required_min: usize,
    },
    HeaderSerialize(bincode::Error),
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
        let min_wheel_file_size = performer_builder.storage_layout().wheel_header_size;
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

    pub async fn run<J>(self, thread_pool: Edeltraud<J>) -> Result<(), Error>
    where C: Send, J: edeltraud::Job + From<job::Job>,
          J::Output: From<job::JobOutput>,
          job::JobOutput: From<J::Output>,
    {
        busyloop(
            self.request_rx,
            self.memory,
            self.storage_layout,
            thread_pool,
        ).await
    }
}

async fn busyloop<C, J>(
    request_rx: mpsc::Receiver<Command<C>>,
    memory: Vec<u8>,
    storage_layout: storage::Layout,
    thread_pool: Edeltraud<J>,
)
    -> Result<(), Error>
where C: Context + Send,
      J: edeltraud::Job + From<job::Job>,
      J::Output: From<job::JobOutput>,
      job::JobOutput: From<J::Output>,
{
    let mut stats = InterpretStats::default();
    let mut tasks = FuturesUnordered::new();
    let mut tasks_count = 0;

    let mut fused_request_rx = request_rx.fuse();

    let mut cursor = io::Cursor::new(memory);
    cursor.set_position(storage_layout.wheel_header_size as u64);

    loop {
        enum Event<C, T> { Command(C), Task(T), }

        let event = match tasks_count {
            0 =>
                Event::Command(fused_request_rx.next().await),
            _ =>
                select! {
                    result = fused_request_rx.next() =>
                        Event::Command(result),
                    result = tasks.next() => match result {
                        None =>
                            unreachable!(),
                        Some(task) => {
                            tasks_count -= 1;
                            Event::Task(task)
                        },
                    },
                },
        };

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
                        let block_header = storage::BlockHeader {
                            block_id: task.block_id.clone(),
                            block_size: write_block.block_bytes.len(),
                            ..Default::default()
                        };

                        bincode::serialize_into(&mut cursor, &block_header)
                            .map_err(Error::BlockHeaderSerialize)?;
                        let start = cursor.position() as usize;
                        let slice = cursor.get_mut();
                        slice[start ..].copy_from_slice(&write_block.block_bytes);
                        cursor.set_position(start as u64 + write_block.block_bytes.len() as u64);
                        let commit_tag = storage::CommitTag {
                            block_id: task.block_id.clone(),
                            crc: write_block.block_crc.unwrap(), // must be already calculated
                            ..Default::default()
                        };
                        bincode::serialize_into(&mut cursor, &commit_tag)
                            .map_err(Error::CommitTagSerialize)?;

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

                    task::TaskKind::ReadBlock(task::ReadBlock { block_header, mut block_bytes, context, }) => {
                        let total_chunk_size = storage_layout.data_size_block_min()
                            + block_header.block_size;
                        block_bytes.resize(total_chunk_size, 0);
                        let start = cursor.position() as usize;
                        let slice = cursor.get_ref();
                        block_bytes.copy_from_slice(&slice[start .. start + total_chunk_size]);
                        cursor.set_position(start as u64 + block_bytes.len() as u64);

                        let storage_layout = storage_layout.clone();
                        let block_process_task = thread_pool.spawn(job::Job::BlockProcess(BlockProcessJobArgs {
                            offset,
                            storage_layout: storage_layout.clone(),
                            block_header,
                            block_bytes,
                        }));

                        // moving block process to separate task, unlock main loop
                        let current_offset = cursor.position();
                        tasks.push(async move {
                            let job_output = block_process_task.await
                                .map_err(|edeltraud::SpawnError::ThreadPoolGone| Error::ThreadPoolGone)?;
                            let job_output: job::JobOutput = job_output.into();
                            let job::BlockProcessDone(block_process_result) = job_output.into();
                            let BlockProcessJobDone { block_id, block_bytes, block_crc, } = block_process_result
                                .map_err(Error::BlockProcessJob)?;

                            let task_done = task::Done {
                                current_offset,
                                task: task::TaskDone {
                                    block_id,
                                    kind: task::TaskDoneKind::ReadBlock(task::TaskDoneReadBlock {
                                        block_bytes,
                                        block_crc,
                                        context,
                                    }),
                                },
                            };

                            reply_tx.send(DoneTask { task_done, stats, })
                                .map_err(|_send_error| Error::WheelPeerLost)
                        });
                        tasks_count += 1;
                    },

                    task::TaskKind::DeleteBlock(delete_block) => {
                        let tombstone_tag = storage::TombstoneTag::default();
                        bincode::serialize_into(&mut cursor, &tombstone_tag)
                            .map_err(Error::TombstoneTagSerialize)?;

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

            Event::Task(Err(Error::WheelPeerLost)) =>
                break,

            Event::Task(task_result) => {
                let () = task_result?;
            },

        }
    }

    log::debug!("master channel closed in interpret_loop, shutting down");
    Ok(())
}
