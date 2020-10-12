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
    Deleted,
};

#[test]
fn stress() {
    let mut runtime = tokio::runtime::Builder::new()
        .basic_scheduler()
        .build()
        .unwrap();
    let wheel_filename = "/tmp/blockwheel_stress";
    let active_tasks_limit = 256;
    let actions_limit = 4096;
    let work_block_size_bytes = 128 * 1024;
    let block_size_bytes_limit = work_block_size_bytes - 256;

    let mut counter_reads = 0;
    let mut counter_writes = 0;
    let mut counter_deletes = 0;

    runtime.block_on(async {
        let supervisor_gen_server = SupervisorGenServer::new();
        let mut supervisor_pid = supervisor_gen_server.pid();
        tokio::spawn(supervisor_gen_server.run());

        let gen_server = GenServer::new();
        let mut pid = gen_server.pid();
        supervisor_pid.spawn_link_permanent(
            gen_server.run(
                supervisor_pid.clone(),
                Params {
                    wheel_filename: wheel_filename.into(),
                    init_wheel_size_bytes: 4 * 1024 * 1024,
                    work_block_size_bytes,
                    lru_cache_size_bytes: 0,
                    ..Default::default()
                },
            ),
        );

        let mut blocks = Vec::new();
        let mut rng = rand::thread_rng();
        let (done_tx, done_rx) = mpsc::channel(0);
        pin_mut!(done_rx);
        let mut active_tasks_count = 0;
        let mut actions_counter = 0;

        enum TaskDone {
            WriteBlock {
                block_id: block::Id,
                block_bytes: block::Bytes,
            },
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

        loop {
            if actions_counter >= actions_limit {
                while active_tasks_count > 0 {
                    match done_rx.next().await.unwrap()? {
                        TaskDone::WriteBlock { .. } | TaskDone::WriteBlockNoSpace =>
                            counter_writes += 1,
                        TaskDone::ReadBlock =>
                            counter_reads += 1,
                        TaskDone::DeleteBlock =>
                            counter_deletes += 1,
                    }
                    active_tasks_count -= 1;
                }
                break;
            }
            let maybe_task_result = if active_tasks_count >= active_tasks_limit {
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
                    match task_result? {
                        TaskDone::WriteBlock { block_id, block_bytes, } => {
                            blocks.push((block_id, block_bytes));
                            counter_writes += 1;
                        },
                        TaskDone::WriteBlockNoSpace =>
                            counter_writes += 1,
                        TaskDone::ReadBlock =>
                            counter_reads += 1,
                        TaskDone::DeleteBlock =>
                            counter_deletes += 1,
                    }
                    active_tasks_count -= 1;
                    continue;
                }
            }
            // construct action and run task
            let info = pid.info().await
                .map_err(|ero::NoProcError| Error::WheelGoneDuringInfo)?;
            if blocks.is_empty() || rng.gen_range(0.0, 1.0) < 0.5 {
                // write or delete task
                let write_prob = info.bytes_free as f64 / info.wheel_size_bytes as f64;
                if blocks.is_empty() || rng.gen_range(0.0, 1.0) < write_prob {
                    // write task
                    let mut blockwheel_pid = pid.clone();
                    let amount = rng.gen_range(1, block_size_bytes_limit);

                    println!(" // write task of {} bytes when {:?}", amount, info);

                    spawn_task(&mut supervisor_pid, done_tx.clone(), async move {
                        let mut block = blockwheel_pid.lend_block().await
                            .map_err(|ero::NoProcError| Error::WheelGoneDuringLendBlock)?;
                        block.extend((0 .. amount).map(|_| 0));
                        rand::thread_rng().fill(&mut block[..]);
                        let block_bytes = block.freeze();
                        match blockwheel_pid.write_block(block_bytes.clone()).await {
                            Ok(block_id) => {

                                println!(" // DONE write task of size = {}, id = {:?} (success)", block_bytes.len(), block_id);

                                Ok(TaskDone::WriteBlock { block_id, block_bytes, })
                            },
                            Err(super::WriteBlockError::NoSpaceLeft) => {

                                println!(" // DONE write task of size = {} (no space)", block_bytes.len());

                                Ok(TaskDone::WriteBlockNoSpace)
                            },
                            Err(error) =>
                                Err(Error::WriteBlock(error))
                        }
                    });
                } else {
                    // delete task
                    let block_index = rng.gen_range(0, blocks.len());
                    let (block_id, _block_bytes) = blocks.swap_remove(block_index);

                    println!(" // delete task of id = {:?} when {:?}", block_id, info);

                    let mut blockwheel_pid = pid.clone();
                    spawn_task(&mut supervisor_pid, done_tx.clone(), async move {
                        let Deleted = blockwheel_pid.delete_block(block_id.clone()).await
                            .map_err(Error::DeleteBlock)?;

                        println!(" // DONE delete task of id = {:?}", block_id);

                        Ok(TaskDone::DeleteBlock)
                    });
                }
            } else {
                // read task
                let block_index = rng.gen_range(0, blocks.len());
                let (block_id, block_bytes) = blocks[block_index].clone();

                println!(" // read task of id = {:?} when {:?}", block_id, info);

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
            }
            actions_counter += 1;
        }

        Ok::<_, Error>(())
    }).unwrap();

    assert_eq!(counter_reads + counter_writes + counter_deletes, actions_limit);

    fs::remove_file(wheel_filename).ok();
}

#[derive(Debug)]
enum Error {
    WheelGoneDuringInfo,
    WheelGoneDuringLendBlock,
    WriteBlock(super::WriteBlockError),
    DeleteBlock(super::DeleteBlockError),
    ReadBlock(super::ReadBlockError),
    ReadBlockCrcMismarch {
        block_id: block::Id,
        expected_crc: u64,
        provided_crc: u64,
    },
}
