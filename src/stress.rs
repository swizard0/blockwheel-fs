use futures::{
    select,
    pin_mut,
    channel::{
        mpsc,
    },
    Future,
    SinkExt,
    StreamExt,
};

use ero::{
    supervisor::{
        SupervisorPid,
        SupervisorGenServer,
    },
};

use alloc_pool::bytes::{
    Bytes,
    BytesPool,
};

use rand::Rng;

use crate::{
    job,
    block,
    Params,
    Flushed,
    Deleted,
    GenServer,
    IterBlocksItem,
    WriteBlockError,
    DeleteBlockError,
    ReadBlockError,
    IterBlocksError,
};

#[derive(Debug)]
pub enum Error {
    ThreadPool(edeltraud::BuildError),
    ThreadPoolGone,
    WheelGoneDuringInfo,
    WheelGoneDuringFlush,
    WriteBlock(WriteBlockError),
    DeleteBlock(DeleteBlockError),
    ReadBlock(ReadBlockError),
    ReadBlockCrcMismarch {
        block_id: block::Id,
        expected_crc: u64,
        provided_crc: u64,
    },
    IterBlocks(IterBlocksError),
    BlocksCountMismatch {
        blocks_count_iter: usize,
        blocks_count_info: usize,
    },
    BlocksSizeMismatch {
        blocks_size_iter: usize,
        blocks_size_info: usize,
    },
    IterBlocksRxDropped,
    IterBlocksUnexpectedBlockReceived {
        block_id: block::Id,
    },
}

#[derive(Clone, Copy, Default, Debug)]
pub struct Limits {
    pub actions: usize,
    pub active_tasks: usize,
    pub block_size_bytes: usize,
}

#[derive(Clone, Copy, Default, Debug)]
pub struct Counter {
    pub reads: usize,
    pub writes: usize,
    pub deletes: usize,
}

impl Counter {
    pub fn sum(&self) -> usize {
        self.reads + self.writes + self.deletes
    }

    pub fn clear(&mut self) {
        self.reads = 0;
        self.writes = 0;
        self.deletes = 0;
    }
}

#[derive(Clone, Debug)]
pub struct BlockTank {
    block_id: block::Id,
    block_bytes: Bytes,
}

pub async fn stress_loop(params: Params, blocks: &mut Vec<BlockTank>, counter: &mut Counter, limits: &Limits) -> Result<(), Error> {
    let supervisor_gen_server = SupervisorGenServer::new();
    let mut supervisor_pid = supervisor_gen_server.pid();
    tokio::spawn(supervisor_gen_server.run());

    let blocks_pool = BytesPool::new();
    let thread_pool: edeltraud::Edeltraud<Job> = edeltraud::Builder::new()
        .build()
        .map_err(Error::ThreadPool)?;

    let gen_server = GenServer::new();
    let mut pid = gen_server.pid();
    supervisor_pid.spawn_link_permanent(
        gen_server.run(supervisor_pid.clone(), thread_pool.clone(), blocks_pool.clone(), params),
    );

    let (done_tx, done_rx) = mpsc::channel(0);
    pin_mut!(done_rx);
    let mut active_tasks_counter = Counter::default();
    let mut actions_counter = 0;

    enum TaskDone {
        WriteBlock(BlockTank),
        WriteBlockNoSpace,
        DeleteBlock,
        ReadBlockSuccess,
        ReadBlockNotFound(block::Id),
    }

    fn spawn_task<T>(
        supervisor_pid: &mut SupervisorPid,
        mut done_tx: mpsc::Sender<Result<TaskDone, Error>>,
        task: T,
    )
    where T: Future<Output = Result<TaskDone, Error>> + Send + 'static
    {
        supervisor_pid.spawn_link_temporary(async move {
            let result = task.await;
            done_tx.send(result).await.ok();
        })
    }

    fn process(task_done: TaskDone, blocks: &mut Vec<BlockTank>, counter: &mut Counter, active_tasks_counter: &mut Counter) {
        match task_done {
            TaskDone::WriteBlock(block_tank) => {
                blocks.push(block_tank);
                counter.writes += 1;
                active_tasks_counter.writes -= 1;
            },
            TaskDone::WriteBlockNoSpace => {
                counter.writes += 1;
                active_tasks_counter.writes -= 1;
            },
            TaskDone::ReadBlockSuccess => {
                counter.reads += 1;
                active_tasks_counter.reads -= 1;
            }
            TaskDone::ReadBlockNotFound(block_id) => {
                counter.reads += 1;
                active_tasks_counter.reads -= 1;
                assert!(blocks.iter().find(|tank| tank.block_id == block_id).is_none());
            },
            TaskDone::DeleteBlock => {
                counter.deletes += 1;
                active_tasks_counter.deletes -= 1;
            },
        }
    }

    let info = pid.info().await
        .map_err(|ero::NoProcError| Error::WheelGoneDuringInfo)?;
    log::info!("start | info: {:?}", info);

    loop {
        if actions_counter >= limits.actions {
            std::mem::drop(done_tx);

            while active_tasks_counter.sum() > 0 {
                process(done_rx.next().await.unwrap()?, blocks, counter, &mut active_tasks_counter);
            }
            break;
        }
        let maybe_task_result = if (active_tasks_counter.sum() >= limits.active_tasks) || (blocks.is_empty() && active_tasks_counter.writes > 0) {
            Some(done_rx.next().await.unwrap())
        } else {
            select! {
                task_result = done_rx.next() =>
                    Some(task_result.unwrap()),
                default =>
                    None,
            }
        };
        match maybe_task_result {
            None =>
                (),
            Some(task_result) => {
                process(task_result?, blocks, counter, &mut active_tasks_counter);
                continue;
            }
        }
        // construct action and run task
        let info = pid.info().await
            .map_err(|ero::NoProcError| Error::WheelGoneDuringInfo)?;
        if blocks.is_empty() || rand::thread_rng().gen_range(0.0f32 .. 1.0) < 0.5 {
            // write or delete task
            let write_prob = if info.bytes_free * 2 >= info.wheel_size_bytes {
                1.0
            } else {
                (info.bytes_free * 2) as f64 / info.wheel_size_bytes as f64
            };
            let dice = rand::thread_rng().gen_range(0.0 .. 1.0);
            if blocks.is_empty() || dice < write_prob {
                // write task
                let mut blockwheel_pid = pid.clone();
                let blocks_pool = blocks_pool.clone();
                let block_size_bytes = limits.block_size_bytes;
                let thread_pool = thread_pool.clone();
                spawn_task(&mut supervisor_pid, done_tx.clone(), async move {
                    let job = Job::GenRandomBlock(GenRandomBlockJobArgs { blocks_pool, block_size_bytes, });
                    let job_output = thread_pool.spawn(job).await
                        .map_err(|edeltraud::SpawnError::ThreadPoolGone| Error::ThreadPoolGone)?;
                    let job_output: JobOutput = job_output.into();
                    let GenRandomBlockDone(GenRandomBlockJobOutput { block_bytes, }) = job_output.into();
                    match blockwheel_pid.write_block(block_bytes.clone()).await {
                        Ok(block_id) =>
                            Ok(TaskDone::WriteBlock(BlockTank { block_id, block_bytes, })),
                        Err(super::WriteBlockError::NoSpaceLeft) =>
                            Ok(TaskDone::WriteBlockNoSpace),
                        Err(error) =>
                            Err(Error::WriteBlock(error))
                    }
                });
                active_tasks_counter.writes += 1;
            } else {
                // delete task
                let block_index = rand::thread_rng().gen_range(0 .. blocks.len());
                let BlockTank { block_id, .. } = blocks.swap_remove(block_index);
                let mut blockwheel_pid = pid.clone();
                spawn_task(&mut supervisor_pid, done_tx.clone(), async move {
                    let Deleted = blockwheel_pid.delete_block(block_id.clone()).await
                        .map_err(Error::DeleteBlock)?;
                    Ok(TaskDone::DeleteBlock)
                });
                active_tasks_counter.deletes += 1;
            }
        } else {
            // read task
            let block_index = rand::thread_rng().gen_range(0 .. blocks.len());
            let BlockTank { block_id, block_bytes, } = blocks[block_index].clone();
            let mut blockwheel_pid = pid.clone();
            spawn_task(&mut supervisor_pid, done_tx.clone(), async move {
                match blockwheel_pid.read_block(block_id.clone()).await {
                    Ok(block_bytes_read) => {
                        let expected_crc = block::crc(&block_bytes);
                        let provided_crc = block::crc(&block_bytes_read);
                        if expected_crc != provided_crc {
                            Err(Error::ReadBlockCrcMismarch { block_id, expected_crc, provided_crc, })
                        } else {
                            Ok(TaskDone::ReadBlockSuccess)
                        }
                    },
                    Err(super::ReadBlockError::NotFound) =>
                        Ok(TaskDone::ReadBlockNotFound(block_id)),
                    Err(error) =>
                        Err(Error::ReadBlock(error)),
                }
            });
            active_tasks_counter.reads += 1;
        }
        actions_counter += 1;
    }

    assert!(done_rx.next().await.is_none());

    log::info!("finish | invoking flush");

    let Flushed = pid.flush().await
        .map_err(|ero::NoProcError| Error::WheelGoneDuringFlush)?;
    let info = pid.info().await
        .map_err(|ero::NoProcError| Error::WheelGoneDuringInfo)?;

    log::info!("finish | flushed: {:?}", info);

    // backwards check with iterator
    let mut iter_blocks = pid.iter_blocks().await
        .map_err(Error::IterBlocks)?;
    if iter_blocks.blocks_total_count != info.blocks_count {
        return Err(Error::BlocksCountMismatch {
            blocks_count_iter: iter_blocks.blocks_total_count,
            blocks_count_info: info.blocks_count,
        });
    }
    if iter_blocks.blocks_total_size != info.data_bytes_used {
        return Err(Error::BlocksSizeMismatch {
            blocks_size_iter: iter_blocks.blocks_total_size,
            blocks_size_info: info.data_bytes_used,
        });
    }
    let mut actual_count = 0;
    loop {
        match iter_blocks.blocks_rx.next().await {
            None =>
                return Err(Error::IterBlocksRxDropped),
            Some(IterBlocksItem::Block { block_id, block_bytes, }) =>
                match blocks.iter().find(|tank| tank.block_id == block_id) {
                    None =>
                        return Err(Error::IterBlocksUnexpectedBlockReceived { block_id, }),
                    Some(tank) => {
                        let expected_crc = block::crc(&tank.block_bytes);
                        let provided_crc = block::crc(&block_bytes);
                        if expected_crc != provided_crc {
                            return Err(Error::ReadBlockCrcMismarch { block_id, expected_crc, provided_crc, });
                        }
                        actual_count += 1;
                    },
                },
            Some(IterBlocksItem::NoMoreBlocks) =>
                break,
        }
    }
    assert_eq!(actual_count, iter_blocks.blocks_total_count);

    Ok::<_, Error>(())
}

struct GenRandomBlockJobArgs {
    blocks_pool: BytesPool,
    block_size_bytes: usize,
}

struct GenRandomBlockJobOutput {
    block_bytes: Bytes,
}

fn run_gen_random_block(GenRandomBlockJobArgs { blocks_pool, block_size_bytes, }: GenRandomBlockJobArgs) -> GenRandomBlockJobOutput {
    let amount = rand::thread_rng().gen_range(1 .. block_size_bytes);
    let mut block = blocks_pool.lend();
    block.extend((0 .. amount).map(|_| 0));
    rand::thread_rng().fill(&mut block[..]);
    let block_bytes = block.freeze();
    GenRandomBlockJobOutput { block_bytes, }
}

enum Job {
    BlockwheelFs(job::Job),
    GenRandomBlock(GenRandomBlockJobArgs),
}

enum JobOutput {
    BlockwheelFs(job::JobOutput),
    GenRandomBlock(GenRandomBlockDone),
}

impl edeltraud::Job for Job {
    type Output = JobOutput;

    fn run(self) -> Self::Output {
        match self {
            Job::BlockwheelFs(job) =>
                JobOutput::BlockwheelFs(job.run()),
            Job::GenRandomBlock(args) =>
                JobOutput::GenRandomBlock(GenRandomBlockDone(run_gen_random_block(args))),
        }
    }
}

struct GenRandomBlockDone(GenRandomBlockJobOutput);

impl From<JobOutput> for GenRandomBlockDone {
    fn from(output: JobOutput) -> Self {
        match output {
            JobOutput::GenRandomBlock(done) =>
                done,
            _other =>
                panic!("expected JobOutput::GenRandomBlockDone but got other"),
        }
    }
}

impl From<job::Job> for Job {
    fn from(job: job::Job) -> Job {
        Job::BlockwheelFs(job)
    }
}

impl From<job::JobOutput> for JobOutput {
    fn from(output: job::JobOutput) -> JobOutput {
        JobOutput::BlockwheelFs(output)
    }
}

impl From<JobOutput> for job::JobOutput {
    fn from(output: JobOutput) -> job::JobOutput {
        match output {
            JobOutput::BlockwheelFs(done) =>
                done,
            _other =>
                panic!("expected JobOutput::BlockwheelFs but got other"),
        }
    }
}
