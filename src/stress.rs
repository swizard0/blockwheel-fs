use std::{
    sync::{
        mpsc,
    },
};

use rand::{
    Rng,
};

use alloc_pool::{
    bytes::{
        Bytes,
        BytesPool,
    },
};

use arbeitssklave::{
    komm,
};

use crate::{
    job,
    block,
    Info,
    Freie,
    Params,
    Flushed,
    Deleted,
    IterBlocks,
    AccessPolicy,
    IterBlocksItem,
    RequestReadBlockError,
    RequestWriteBlockError,
    RequestDeleteBlockError,
    Error as BlockwheelFsError,
};

#[derive(Debug)]
pub enum Error {
    ThreadPool(edeltraud::BuildError),
    Edeltraud(edeltraud::SpawnError),
    BlockwheelFs(BlockwheelFsError),
    UnexpectedFtdOrder(String),
    RequestInfo(arbeitssklave::Error),

    // ReadBlockCrcMismarch {
    //     block_id: block::Id,
    //     expected_crc: u64,
    //     provided_crc: u64,
    // },
    // IterBlocks(IterBlocksError),
    // BlocksCountMismatch {
    //     blocks_count_iter: usize,
    //     blocks_count_info: usize,
    // },
    // BlocksSizeMismatch {
    //     blocks_size_iter: usize,
    //     blocks_size_info: usize,
    // },
    // IterBlocksRxDropped,
    // IterBlocksUnexpectedBlockReceived {
    //     block_id: block::Id,
    // },
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

pub fn stress_loop(params: Params, blocks: &mut Vec<BlockTank>, counter: &mut Counter, limits: &Limits) -> Result<(), Error> {
    let thread_pool: edeltraud::Edeltraud<Job> = edeltraud::Builder::new()
        .build()
        .map_err(Error::ThreadPool)?;
    let blocks_pool = BytesPool::new();

    let blockwheel_fs_meister = Freie::new()
        .versklaven(params, blocks_pool.clone(), &edeltraud::ThreadPoolMap::new(thread_pool.clone()))
        .map_err(Error::BlockwheelFs)?;

    let (ftd_tx, ftd_rx) = mpsc::channel();
    let ftd_sklave_freie = arbeitssklave::Freie::new();
    let ftd_sendegeraet = komm::Sendegeraet::starten(&ftd_sklave_freie, thread_pool.clone())
        .unwrap();
    let ftd_sklave_meister = ftd_sklave_freie
        .versklaven(Welt { ftd_tx, }, &thread_pool)
        .unwrap();

    println!(" ;; asd");
    blockwheel_fs_meister.info(ftd_sendegeraet.rueckkopplung(ReplyInfo))
        .map_err(Error::RequestInfo)?;
    println!(" ;; qwe");
    let info = match ftd_rx.recv() {
        Ok(Order::Info(komm::Umschlag { payload: info, stamp: ReplyInfo, })) =>
            info,
        other_order =>
            return Err(Error::UnexpectedFtdOrder(format!("{other_order:?}"))),
    };
    println!(" ;; zxc");
    log::info!("start | info: {:?}", info);


    todo!()
    // let (done_tx, done_rx) = mpsc::channel(0);
    // pin_mut!(done_rx);
    // let mut active_tasks_counter = Counter::default();
    // let mut actions_counter = 0;

    // enum TaskDone {
    //     WriteBlock(BlockTank),
    //     WriteBlockNoSpace,
    //     DeleteBlock,
    //     ReadBlockSuccess,
    //     ReadBlockNotFound(block::Id),
    // }

    // fn spawn_task<T>(
    //     supervisor_pid: &mut SupervisorPid,
    //     mut done_tx: mpsc::Sender<Result<TaskDone, Error>>,
    //     task: T,
    // )
    // where T: Future<Output = Result<TaskDone, Error>> + Send + 'static
    // {
    //     supervisor_pid.spawn_link_temporary(async move {
    //         let result = task.await;
    //         done_tx.send(result).await.ok();
    //     })
    // }

    // fn process(task_done: TaskDone, blocks: &mut Vec<BlockTank>, counter: &mut Counter, active_tasks_counter: &mut Counter) {
    //     match task_done {
    //         TaskDone::WriteBlock(block_tank) => {
    //             blocks.push(block_tank);
    //             counter.writes += 1;
    //             active_tasks_counter.writes -= 1;
    //         },
    //         TaskDone::WriteBlockNoSpace => {
    //             counter.writes += 1;
    //             active_tasks_counter.writes -= 1;
    //         },
    //         TaskDone::ReadBlockSuccess => {
    //             counter.reads += 1;
    //             active_tasks_counter.reads -= 1;
    //         }
    //         TaskDone::ReadBlockNotFound(block_id) => {
    //             counter.reads += 1;
    //             active_tasks_counter.reads -= 1;
    //             assert!(blocks.iter().find(|tank| tank.block_id == block_id).is_none());
    //         },
    //         TaskDone::DeleteBlock => {
    //             counter.deletes += 1;
    //             active_tasks_counter.deletes -= 1;
    //         },
    //     }
    // }

    // let info = pid.info().await
    //     .map_err(|ero::NoProcError| Error::WheelGoneDuringInfo)?;
    // log::info!("start | info: {:?}", info);

    // loop {
    //     if actions_counter >= limits.actions {
    //         std::mem::drop(done_tx);

    //         while active_tasks_counter.sum() > 0 {
    //             process(done_rx.next().await.unwrap()?, blocks, counter, &mut active_tasks_counter);
    //         }
    //         break;
    //     }
    //     let maybe_task_result = if (active_tasks_counter.sum() >= limits.active_tasks) || (blocks.is_empty() && active_tasks_counter.writes > 0) {
    //         Some(done_rx.next().await.unwrap())
    //     } else {
    //         select! {
    //             task_result = done_rx.next() =>
    //                 Some(task_result.unwrap()),
    //             default =>
    //                 None,
    //         }
    //     };
    //     match maybe_task_result {
    //         None =>
    //             (),
    //         Some(task_result) => {
    //             process(task_result?, blocks, counter, &mut active_tasks_counter);
    //             continue;
    //         }
    //     }
    //     // construct action and run task
    //     let info = pid.info().await
    //         .map_err(|ero::NoProcError| Error::WheelGoneDuringInfo)?;
    //     if blocks.is_empty() || rand::thread_rng().gen_range(0.0f32 .. 1.0) < 0.5 {
    //         // write or delete task
    //         let write_prob = if info.bytes_free * 2 >= info.wheel_size_bytes {
    //             1.0
    //         } else {
    //             (info.bytes_free * 2) as f64 / info.wheel_size_bytes as f64
    //         };
    //         let dice = rand::thread_rng().gen_range(0.0 .. 1.0);
    //         if blocks.is_empty() || dice < write_prob {
    //             // write task
    //             let mut blockwheel_pid = pid.clone();
    //             let blocks_pool = blocks_pool.clone();
    //             let block_size_bytes = limits.block_size_bytes;
    //             let thread_pool = thread_pool.clone();
    //             spawn_task(&mut supervisor_pid, done_tx.clone(), async move {
    //                 let block_bytes =
    //                     edeltraud::job_async(
    //                         &thread_pool,
    //                         JobGenRandomBlock { blocks_pool, block_size_bytes, },
    //                     )
    //                     .map_err(Error::Edeltraud)?
    //                     .await
    //                     .map_err(Error::Edeltraud)?;
    //                 match blockwheel_pid.write_block(block_bytes.clone()).await {
    //                     Ok(block_id) =>
    //                         Ok(TaskDone::WriteBlock(BlockTank { block_id, block_bytes, })),
    //                     Err(super::WriteBlockError::NoSpaceLeft) =>
    //                         Ok(TaskDone::WriteBlockNoSpace),
    //                     Err(error) =>
    //                         Err(Error::WriteBlock(error))
    //                 }
    //             });
    //             active_tasks_counter.writes += 1;
    //         } else {
    //             // delete task
    //             let block_index = rand::thread_rng().gen_range(0 .. blocks.len());
    //             let BlockTank { block_id, .. } = blocks.swap_remove(block_index);
    //             let mut blockwheel_pid = pid.clone();
    //             spawn_task(&mut supervisor_pid, done_tx.clone(), async move {
    //                 let Deleted = blockwheel_pid.delete_block(block_id.clone()).await
    //                     .map_err(Error::DeleteBlock)?;
    //                 Ok(TaskDone::DeleteBlock)
    //             });
    //             active_tasks_counter.deletes += 1;
    //         }
    //     } else {
    //         // read task
    //         let block_index = rand::thread_rng().gen_range(0 .. blocks.len());
    //         let BlockTank { block_id, block_bytes, } = blocks[block_index].clone();
    //         let mut blockwheel_pid = pid.clone();
    //         let thread_pool = thread_pool.clone();
    //         spawn_task(&mut supervisor_pid, done_tx.clone(), async move {
    //             match blockwheel_pid.read_block(block_id.clone()).await {
    //                 Ok(block_bytes_read) => {
    //                     let JobCalcCrcOutput { expected_crc, provided_crc, } =
    //                         edeltraud::job_async(
    //                             &thread_pool,
    //                             JobCalcCrc {
    //                                 expected_block_bytes: block_bytes,
    //                                 provided_block_bytes: block_bytes_read,
    //                             },
    //                         )
    //                         .map_err(Error::Edeltraud)?
    //                         .await
    //                         .map_err(Error::Edeltraud)?;
    //                     if expected_crc != provided_crc {
    //                         Err(Error::ReadBlockCrcMismarch { block_id, expected_crc, provided_crc, })
    //                     } else {
    //                         Ok(TaskDone::ReadBlockSuccess)
    //                     }
    //                 },
    //                 Err(super::ReadBlockError::NotFound) =>
    //                     Ok(TaskDone::ReadBlockNotFound(block_id)),
    //                 Err(error) =>
    //                     Err(Error::ReadBlock(error)),
    //             }
    //         });
    //         active_tasks_counter.reads += 1;
    //     }
    //     actions_counter += 1;
    // }

    // assert!(done_rx.next().await.is_none());

    // log::info!("finish | invoking flush");

    // let Flushed = pid.flush().await
    //     .map_err(|ero::NoProcError| Error::WheelGoneDuringFlush)?;
    // let info = pid.info().await
    //     .map_err(|ero::NoProcError| Error::WheelGoneDuringInfo)?;

    // log::info!("finish | flushed: {:?}", info);

    // // backwards check with iterator
    // let mut iter_blocks = pid.iter_blocks().await
    //     .map_err(Error::IterBlocks)?;
    // if iter_blocks.blocks_total_count != info.blocks_count {
    //     return Err(Error::BlocksCountMismatch {
    //         blocks_count_iter: iter_blocks.blocks_total_count,
    //         blocks_count_info: info.blocks_count,
    //     });
    // }
    // if iter_blocks.blocks_total_size != info.data_bytes_used {
    //     return Err(Error::BlocksSizeMismatch {
    //         blocks_size_iter: iter_blocks.blocks_total_size,
    //         blocks_size_info: info.data_bytes_used,
    //     });
    // }
    // let mut actual_count = 0;
    // loop {
    //     match iter_blocks.blocks_rx.next().await {
    //         None =>
    //             return Err(Error::IterBlocksRxDropped),
    //         Some(IterBlocksItem::Block { block_id, block_bytes, }) =>
    //             match blocks.iter().find(|tank| tank.block_id == block_id) {
    //                 None =>
    //                     return Err(Error::IterBlocksUnexpectedBlockReceived { block_id, }),
    //                 Some(tank) => {
    //                     let JobCalcCrcOutput { expected_crc, provided_crc, } =
    //                         edeltraud::job_async(
    //                             &thread_pool,
    //                             JobCalcCrc {
    //                                 expected_block_bytes: tank.block_bytes.clone(),
    //                                 provided_block_bytes: block_bytes,
    //                             },
    //                         )
    //                         .map_err(Error::Edeltraud)?
    //                         .await
    //                         .map_err(Error::Edeltraud)?;
    //                     if expected_crc != provided_crc {
    //                         return Err(Error::ReadBlockCrcMismarch { block_id, expected_crc, provided_crc, });
    //                     }
    //                     actual_count += 1;
    //                 },
    //             },
    //         Some(IterBlocksItem::NoMoreBlocks) =>
    //             break,
    //     }
    // }
    // assert_eq!(actual_count, iter_blocks.blocks_total_count);

    // Ok::<_, Error>(())
}


#[derive(Debug)]
enum Order {
    InfoCancel(komm::UmschlagAbbrechen<ReplyInfo>),
    Info(komm::Umschlag<Info, ReplyInfo>),
    FlushCancel(komm::UmschlagAbbrechen<ReplyFlush>),
    Flush(komm::Umschlag<Flushed, ReplyFlush>),
    WriteBlockCancel(komm::UmschlagAbbrechen<ReplyWriteBlock>),
    WriteBlock(komm::Umschlag<Result<block::Id, RequestWriteBlockError>, ReplyWriteBlock>),
    ReadBlockCancel(komm::UmschlagAbbrechen<ReplyReadBlock>),
    ReadBlock(komm::Umschlag<Result<Bytes, RequestReadBlockError>, ReplyReadBlock>),
    DeleteBlockCancel(komm::UmschlagAbbrechen<ReplyDeleteBlock>),
    DeleteBlock(komm::Umschlag<Result<Deleted, RequestDeleteBlockError>, ReplyDeleteBlock>),
    IterBlocksInitCancel(komm::UmschlagAbbrechen<ReplyIterBlocksInit>),
    IterBlocksInit(komm::Umschlag<IterBlocks, ReplyIterBlocksInit>),
    IterBlocksNextCancel(komm::UmschlagAbbrechen<ReplyIterBlocksNext>),
    IterBlocksNext(komm::Umschlag<IterBlocksItem, ReplyIterBlocksNext>),
}

#[derive(Debug)]
struct ReplyInfo;
#[derive(Debug)]
struct ReplyFlush;
#[derive(Debug)]
struct ReplyWriteBlock;
#[derive(Debug)]
struct ReplyReadBlock;
#[derive(Debug)]
struct ReplyDeleteBlock;
#[derive(Debug)]
struct ReplyIterBlocksInit;
#[derive(Debug)]
struct ReplyIterBlocksNext;

impl From<komm::UmschlagAbbrechen<ReplyInfo>> for Order {
    fn from(v: komm::UmschlagAbbrechen<ReplyInfo>) -> Order {
        Order::InfoCancel(v)
    }
}

impl From<komm::Umschlag<Info, ReplyInfo>> for Order {
    fn from(v: komm::Umschlag<Info, ReplyInfo>) -> Order {
        Order::Info(v)
    }
}

impl From<komm::UmschlagAbbrechen<ReplyFlush>> for Order {
    fn from(v: komm::UmschlagAbbrechen<ReplyFlush>) -> Order {
        Order::FlushCancel(v)
    }
}

impl From<komm::Umschlag<Flushed, ReplyFlush>> for Order {
    fn from(v: komm::Umschlag<Flushed, ReplyFlush>) -> Order {
        Order::Flush(v)
    }
}

impl From<komm::UmschlagAbbrechen<ReplyWriteBlock>> for Order {
    fn from(v: komm::UmschlagAbbrechen<ReplyWriteBlock>) -> Order {
        Order::WriteBlockCancel(v)
    }
}

impl From<komm::Umschlag<Result<block::Id, RequestWriteBlockError>, ReplyWriteBlock>> for Order {
    fn from(v: komm::Umschlag<Result<block::Id, RequestWriteBlockError>, ReplyWriteBlock>) -> Order {
        Order::WriteBlock(v)
    }
}

impl From<komm::UmschlagAbbrechen<ReplyReadBlock>> for Order {
    fn from(v: komm::UmschlagAbbrechen<ReplyReadBlock>) -> Order {
        Order::ReadBlockCancel(v)
    }
}

impl From<komm::Umschlag<Result<Bytes, RequestReadBlockError>, ReplyReadBlock>> for Order {
    fn from(v: komm::Umschlag<Result<Bytes, RequestReadBlockError>, ReplyReadBlock>) -> Order {
        Order::ReadBlock(v)
    }
}

impl From<komm::UmschlagAbbrechen<ReplyDeleteBlock>> for Order {
    fn from(v: komm::UmschlagAbbrechen<ReplyDeleteBlock>) -> Order {
        Order::DeleteBlockCancel(v)
    }
}

impl From<komm::Umschlag<Result<Deleted, RequestDeleteBlockError>, ReplyDeleteBlock>> for Order {
    fn from(v: komm::Umschlag<Result<Deleted, RequestDeleteBlockError>, ReplyDeleteBlock>) -> Order {
        Order::DeleteBlock(v)
    }
}

impl From<komm::UmschlagAbbrechen<ReplyIterBlocksInit>> for Order {
    fn from(v: komm::UmschlagAbbrechen<ReplyIterBlocksInit>) -> Order {
        Order::IterBlocksInitCancel(v)
    }
}

impl From<komm::Umschlag<IterBlocks, ReplyIterBlocksInit>> for Order {
    fn from(v: komm::Umschlag<IterBlocks, ReplyIterBlocksInit>) -> Order {
        Order::IterBlocksInit(v)
    }
}

impl From<komm::UmschlagAbbrechen<ReplyIterBlocksNext>> for Order {
    fn from(v: komm::UmschlagAbbrechen<ReplyIterBlocksNext>) -> Order {
        Order::IterBlocksNextCancel(v)
    }
}

impl From<komm::Umschlag<IterBlocksItem, ReplyIterBlocksNext>> for Order {
    fn from(v: komm::Umschlag<IterBlocksItem, ReplyIterBlocksNext>) -> Order {
        Order::IterBlocksNext(v)
    }
}

struct LocalAccessPolicy;

impl AccessPolicy for LocalAccessPolicy {
    type Order = Order;
    type Info = ReplyInfo;
    type Flush = ReplyFlush;
    type WriteBlock = ReplyWriteBlock;
    type ReadBlock = ReplyReadBlock;
    type DeleteBlock = ReplyDeleteBlock;
    type IterBlocksInit = ReplyIterBlocksInit;
    type IterBlocksNext = ReplyIterBlocksNext;
}

struct Welt {
    ftd_tx: mpsc::Sender<Order>,
}

enum Job {
    BlockwheelFs(job::Job<LocalAccessPolicy>),
    // GenRandomBlock(JobGenRandomBlock),
    // CalcCrc(JobCalcCrc),
    FtdSklave(arbeitssklave::SklaveJob<Welt, Order>),
}

impl From<job::Job<LocalAccessPolicy>> for Job {
    fn from(job: job::Job<LocalAccessPolicy>) -> Job {
        Job::BlockwheelFs(job)
    }
}

impl From<arbeitssklave::SklaveJob<Welt, Order>> for Job {
    fn from(job: arbeitssklave::SklaveJob<Welt, Order>) -> Job {
        Job::FtdSklave(job)
    }
}

impl edeltraud::Job for Job {
    type Output = ();

    fn run<P>(self, thread_pool: &P) -> Self::Output where P: edeltraud::ThreadPool<Self> {
        match self {
            Job::BlockwheelFs(job) => {
                job.run(&edeltraud::ThreadPoolMap::new(thread_pool));
            },
            // Job::GenRandomBlock(job) => {
            //     job.run(&edeltraud::ThreadPoolMap::new(thread_pool));
            // },
            // Job::CalcCrc(job) => {
            //     job.run(&edeltraud::ThreadPoolMap::new(thread_pool));
            // },
            Job::FtdSklave(arbeitssklave::SklaveJob { mut sklave, mut sklavenwelt, }) => {
                loop {
                    match sklave.zu_ihren_diensten(sklavenwelt).unwrap() {
                        arbeitssklave::Gehorsam::Machen { befehle, sklavenwelt: next_sklavenwelt, } => {
                            sklavenwelt = next_sklavenwelt;
                            for befehl in befehle {
                                sklavenwelt.ftd_tx.send(befehl).unwrap();
                            }
                        },
                        arbeitssklave::Gehorsam::Rasten =>
                            break,
                    }
                }
            },
        }
    }
}

// impl From<JobGenRandomBlock> for Job {
//     fn from(async_job: JobGenRandomBlock) -> Job {
//         Job::GenRandomBlock(async_job)
//     }
// }

// impl From<JobCalcCrc> for Job {
//     fn from(async_job: JobCalcCrc) -> Job {
//         Job::CalcCrc(async_job)
//     }
// }

// struct JobGenRandomBlock {
//     blocks_pool: BytesPool,
//     block_size_bytes: usize,
// }

// impl edeltraud::Job for JobGenRandomBlock {
//     type Output = Bytes;

//     fn run<P>(self, _thread_pool: &P) -> Self::Output where P: edeltraud::ThreadPool<Self> {
//         let JobGenRandomBlock { blocks_pool, block_size_bytes, } = self;
//         let amount = rand::thread_rng().gen_range(1 .. block_size_bytes);
//         let mut block = blocks_pool.lend();
//         block.extend((0 .. amount).map(|_| 0));
//         rand::thread_rng().fill(&mut block[..]);
//         block.freeze()
//     }
// }

// struct JobCalcCrc {
//     expected_block_bytes: Bytes,
//     provided_block_bytes: Bytes,
// }

// struct JobCalcCrcOutput {
//     expected_crc: u64,
//     provided_crc: u64,
// }

// impl edeltraud::Job for JobCalcCrc {
//     type Output = JobCalcCrcOutput;

//     fn run<P>(self, _thread_pool: &P) -> Self::Output where P: edeltraud::ThreadPool<Self> {
//         let JobCalcCrc { expected_block_bytes, provided_block_bytes, } = self;
//         let expected_crc = block::crc(&expected_block_bytes);
//         let provided_crc = block::crc(&provided_block_bytes);
//         JobCalcCrcOutput { expected_crc, provided_crc, }
//     }
// }
