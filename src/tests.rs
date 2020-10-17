use std::fs;

use futures::{
    Future,
    SinkExt,
    StreamExt,
    select,
    pin_mut,
    channel::mpsc,
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
};

#[test]
fn stress() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let wheel_filename = "/tmp/blockwheel_stress";
    let work_block_size_bytes = 32 * 1024;
    let init_wheel_size_bytes = 1 * 1024 * 1024;

    let params = Params {
        wheel_filename: wheel_filename.into(),
        init_wheel_size_bytes,
        work_block_size_bytes,
        lru_cache_size_bytes: 0,
        defrag_parallel_tasks_limit: 4,
        ..Default::default()
    };

    let limits = Limits {
        active_tasks: 256,
        actions: 3072,
        block_size_bytes: work_block_size_bytes - 256,
    };

    let mut counter = Counter::default();
    let mut blocks = Vec::new();

    // first fill wheel from scratch
    fs::remove_file(wheel_filename).ok();
    runtime.block_on(stress_loop(params.clone(), &mut blocks, &mut counter, &limits)).unwrap();

    println!(" // CREATE: counter_reads = {}, counter_writes = {}, counter_deletes = {}", counter.reads, counter.writes, counter.deletes);
    assert_eq!(counter.reads + counter.writes + counter.deletes, limits.actions);

    // next load existing wheel and repeat stress with blocks
    fs::remove_file(wheel_filename).ok();
    counter.clear();
    runtime.block_on(stress_loop(params.clone(), &mut blocks, &mut counter, &limits)).unwrap();

    println!(" // LOAD: counter_reads = {}, counter_writes = {}, counter_deletes = {}", counter.reads, counter.writes, counter.deletes);
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
    block_bytes: block::Bytes,
}

async fn stress_loop(params: Params, blocks: &mut Vec<BlockTank>, counter: &mut Counter, limits: &Limits) -> Result<(), Error> {
    let supervisor_gen_server = SupervisorGenServer::new();
    let mut supervisor_pid = supervisor_gen_server.pid();
    tokio::spawn(supervisor_gen_server.run());

    let gen_server = GenServer::new();
    let mut pid = gen_server.pid();
    supervisor_pid.spawn_link_permanent(
        gen_server.run(supervisor_pid.clone(), params),
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

                println!(
                    " // termination await of {} active tasks ({}/{} / {}/{} / {}/{})",
                    active_tasks_counter.sum(),
                    counter.reads,
                    active_tasks_counter.reads,
                    counter.writes,
                    active_tasks_counter.writes,
                    counter.deletes,
                    active_tasks_counter.deletes,
                );

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
                let amount = rng.gen_range(1, limits.block_size_bytes);

                println!(
                    " // write task {} prob = {}/{}/{} of {} bytes when {:?}",
                    actions_counter,
                    write_prob,
                    dice,
                    blocks.len(),
                    amount,
                    info,
                );

                spawn_task(&mut supervisor_pid, done_tx.clone(), async move {
                    let mut block = blockwheel_pid.lend_block().await
                        .map_err(|ero::NoProcError| Error::WheelGoneDuringLendBlock)?;
                    block.extend((0 .. amount).map(|_| 0));
                    rand::thread_rng().fill(&mut block[..]);
                    let block_bytes = block.freeze();
                    match blockwheel_pid.write_block(block_bytes.clone()).await {
                        Ok(block_id) => {

                            println!(" // DONE write task of size = {}, id = {:?} (success)", block_bytes.len(), block_id);

                            blockwheel_pid.repay_block(block_bytes.clone()).await
                                .map_err(|ero::NoProcError| Error::WheelGoneDuringRepayBlock)?;
                            Ok(TaskDone::WriteBlock(BlockTank { block_id, block_bytes, }))
                        },
                        Err(super::WriteBlockError::NoSpaceLeft) => {

                            println!(" // DONE write task of size = {} (no space)", block_bytes.len());

                            Ok(TaskDone::WriteBlockNoSpace)
                        },
                        Err(error) =>
                            Err(Error::WriteBlock(error))
                    }
                });
                active_tasks_counter.writes += 1;
            } else {
                // delete task
                let block_index = rng.gen_range(0, blocks.len());
                let BlockTank { block_id, .. } = blocks.swap_remove(block_index);

                println!(" // delete task {} of id = {:?} when {:?}", actions_counter, block_id, info);

                let mut blockwheel_pid = pid.clone();
                spawn_task(&mut supervisor_pid, done_tx.clone(), async move {
                    let Deleted = blockwheel_pid.delete_block(block_id.clone()).await
                        .map_err(Error::DeleteBlock)?;

                    println!(" // DONE delete task {} of id = {:?}", actions_counter, block_id);

                    Ok(TaskDone::DeleteBlock)
                });
                active_tasks_counter.deletes += 1;
            }
        } else {
            // read task
            let block_index = rng.gen_range(0, blocks.len());
            let BlockTank { block_id, block_bytes, } = blocks[block_index].clone();

            println!(" // read task {} of id = {:?} when {:?}", actions_counter, block_id, info);

            let mut blockwheel_pid = pid.clone();
            spawn_task(&mut supervisor_pid, done_tx.clone(), async move {
                let block_bytes_read = blockwheel_pid.read_block(block_id.clone()).await
                    .map_err(Error::ReadBlock)?;
                let expected_crc = block::crc(&block_bytes);
                let provided_crc = block::crc(&block_bytes_read);
                if expected_crc != provided_crc {
                    Err(Error::ReadBlockCrcMismarch { block_id, expected_crc, provided_crc, })
                } else {

                    println!(" // DONE read task of id = {:?}", block_id);

                    Ok(TaskDone::ReadBlock)
                }
            });
            active_tasks_counter.reads += 1;
        }
        actions_counter += 1;
    }

    println!("// dropping tx");
    assert!(done_rx.next().await.is_none());

    println!("// flushing ...");
    let Flushed = pid.flush().await
        .map_err(|ero::NoProcError| Error::WheelGoneDuringFlush)?;
    println!("// flushed");

    Ok::<_, Error>(())
}


#[derive(Debug)]
enum Error {
    WheelGoneDuringInfo,
    WheelGoneDuringFlush,
    WheelGoneDuringLendBlock,
    WheelGoneDuringRepayBlock,
    WriteBlock(super::WriteBlockError),
    DeleteBlock(super::DeleteBlockError),
    ReadBlock(super::ReadBlockError),
    ReadBlockCrcMismarch {
        block_id: block::Id,
        expected_crc: u64,
        provided_crc: u64,
    },
}
