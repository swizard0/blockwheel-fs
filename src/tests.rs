use std::{
    fs,
    sync::Arc,
};

use futures::{
    Future,
    SinkExt,
    StreamExt,
    select,
    pin_mut,
    channel::mpsc,
};

use alloc_pool::bytes::{
    Bytes,
    BytesPool,
};

use ero::{
    supervisor::{
        SupervisorPid,
        SupervisorGenServer,
    },
};

use rand::Rng;

use super::{
    block,
    Params,
    GenServer,
    Flushed,
    Deleted,
    IterBlocksItem,
};

#[test]
fn stress() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let wheel_filename = "/tmp/blockwheel_stress";
    let work_block_size_bytes = 16 * 1024;
    let init_wheel_size_bytes = 1 * 1024 * 1024;

    let params = Params {
        wheel_filename: wheel_filename.into(),
        init_wheel_size_bytes,
        work_block_size_bytes,
        lru_cache_size_bytes: 0,
        defrag_parallel_tasks_limit: 8,
        ..Default::default()
    };

    let limits = Limits {
        active_tasks: 128,
        actions: 1024,
        block_size_bytes: work_block_size_bytes - 256,
    };

    let mut counter = Counter::default();
    let mut blocks = Vec::new();

    // first fill wheel from scratch
    fs::remove_file(wheel_filename).ok();
    runtime.block_on(stress_loop(params.clone(), &mut blocks, &mut counter, &limits)).unwrap();

    assert_eq!(counter.reads + counter.writes + counter.deletes, limits.actions);

    // next load existing wheel and repeat stress with blocks
    counter.clear();
    runtime.block_on(stress_loop(params.clone(), &mut blocks, &mut counter, &limits)).unwrap();

    assert_eq!(counter.reads + counter.writes + counter.deletes, limits.actions);

    fs::remove_file(wheel_filename).ok();
}

#[derive(Clone, Copy, Default, Debug)]
struct Limits {
    actions: usize,
    active_tasks: usize,
    block_size_bytes: usize,
}

#[derive(Clone, Copy, Default, Debug)]
struct Counter {
    reads: usize,
    writes: usize,
    deletes: usize,
}

impl Counter {
    fn sum(&self) -> usize {
        self.reads + self.writes + self.deletes
    }

    fn clear(&mut self) {
        self.reads = 0;
        self.writes = 0;
        self.deletes = 0;
    }
}

#[derive(Clone, Debug)]
struct BlockTank {
    block_id: block::Id,
    block_bytes: Bytes,
}

async fn stress_loop(params: Params, blocks: &mut Vec<BlockTank>, counter: &mut Counter, limits: &Limits) -> Result<(), Error> {
    let supervisor_gen_server = SupervisorGenServer::new();
    let mut supervisor_pid = supervisor_gen_server.pid();
    tokio::spawn(supervisor_gen_server.run());

    let blocks_pool = BytesPool::new();
    let thread_pool = rayon::ThreadPoolBuilder::new()
        .build()
        .map_err(Error::ThreadPool)?;

    let gen_server = GenServer::new();
    let mut pid = gen_server.pid();
    supervisor_pid.spawn_link_permanent(
        gen_server.run(supervisor_pid.clone(), Arc::new(thread_pool), blocks_pool.clone(), params),
    );

    let mut rng = rand::thread_rng();
    let (done_tx, done_rx) = mpsc::channel(0);
    pin_mut!(done_rx);
    let mut active_tasks_counter = Counter::default();
    let mut actions_counter = 0;

    enum TaskDone {
        WriteBlock(BlockTank),
        WriteBlockNoSpace,
        DeleteBlock,
        ReadBlock,
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
            TaskDone::ReadBlock => {
                counter.reads += 1;
                active_tasks_counter.reads -= 1;
            }
            TaskDone::DeleteBlock => {
                counter.deletes += 1;
                active_tasks_counter.deletes -= 1;
            },
        }
    }

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
        if blocks.is_empty() || rng.gen_range(0.0, 1.0) < 0.5 {
            // write or delete task
            let write_prob = if info.bytes_free * 2 >= info.wheel_size_bytes {
                1.0
            } else {
                (info.bytes_free * 2) as f64 / info.wheel_size_bytes as f64
            };
            let dice = rng.gen_range(0.0, 1.0);
            if blocks.is_empty() || dice < write_prob {
                // write task
                let mut blockwheel_pid = pid.clone();
                let blocks_pool = blocks_pool.clone();
                let amount = rng.gen_range(1, limits.block_size_bytes);
                spawn_task(&mut supervisor_pid, done_tx.clone(), async move {
                    let mut block = blocks_pool.lend();
                    block.extend((0 .. amount).map(|_| 0));
                    rand::thread_rng().fill(&mut block[..]);
                    let block_bytes = block.freeze();
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
                let block_index = rng.gen_range(0, blocks.len());
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
            let block_index = rng.gen_range(0, blocks.len());
            let BlockTank { block_id, block_bytes, } = blocks[block_index].clone();
            let mut blockwheel_pid = pid.clone();
            spawn_task(&mut supervisor_pid, done_tx.clone(), async move {
                let block_bytes_read = blockwheel_pid.read_block(block_id.clone()).await
                    .map_err(Error::ReadBlock)?;
                let expected_crc = block::crc(&block_bytes);
                let provided_crc = block::crc(&block_bytes_read);
                if expected_crc != provided_crc {
                    Err(Error::ReadBlockCrcMismarch { block_id, expected_crc, provided_crc, })
                } else {
                    Ok(TaskDone::ReadBlock)
                }
            });
            active_tasks_counter.reads += 1;
        }
        actions_counter += 1;
    }

    assert!(done_rx.next().await.is_none());

    let Flushed = pid.flush().await
        .map_err(|ero::NoProcError| Error::WheelGoneDuringFlush)?;
    let info = pid.info().await
        .map_err(|ero::NoProcError| Error::WheelGoneDuringInfo)?;

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
                    },
                },
            Some(IterBlocksItem::NoMoreBlocks) =>
                break,
        }
    }

    Ok::<_, Error>(())
}


#[derive(Debug)]
enum Error {
    ThreadPool(rayon::ThreadPoolBuildError),
    WheelGoneDuringInfo,
    WheelGoneDuringFlush,
    WriteBlock(super::WriteBlockError),
    DeleteBlock(super::DeleteBlockError),
    ReadBlock(super::ReadBlockError),
    ReadBlockCrcMismarch {
        block_id: block::Id,
        expected_crc: u64,
        provided_crc: u64,
    },
    IterBlocks(super::IterBlocksError),
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
