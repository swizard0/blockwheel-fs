use std::{
    sync::{
        mpsc,
        Mutex,
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
    wheel::{
        interpret,
        performer_sklave,
    },
    Info,
    Params,
    Meister,
    Flushed,
    Deleted,
    IterBlocks,
    EchoPolicy,
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
    RequestInfo(BlockwheelFsError),
    RequestFlush(BlockwheelFsError),
    RequestReadBlock(BlockwheelFsError),
    RequestWriteBlock(BlockwheelFsError),
    RequestDeleteBlock(BlockwheelFsError),
    RequestIterBlocksInit(BlockwheelFsError),
    RequestIterBlocksNext(BlockwheelFsError),
    FtdProcessIsLost,
    ReadBlock(RequestReadBlockError),
    WriteBlock(RequestWriteBlockError),
    DeleteBlock(RequestDeleteBlockError),
    JobWriteBlockCanceled,
    JobVerifyBlockCanceled,
    ReadBlockCrcMismarch {
        block_id: block::Id,
        expected_crc: u64,
        provided_crc: u64,
    },
    BlocksCountMismatch {
        blocks_count_iter: usize,
        blocks_count_info: usize,
    },
    BlocksSizeMismatch {
        blocks_size_iter: usize,
        blocks_size_info: usize,
    },
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
    pub write_jobs: usize,
    pub verify_jobs: usize,
    pub no_space_hits: usize
}

impl Counter {
    pub fn sum(&self) -> usize {
        self.reads + self.writes + self.deletes
    }

    pub fn sum_total(&self) -> usize {
        self.sum() + self.write_jobs + self.verify_jobs
    }

    pub fn clear(&mut self) {
        self.reads = 0;
        self.writes = 0;
        self.deletes = 0;
        self.write_jobs = 0;
        self.verify_jobs = 0;
        self.no_space_hits = 0;
    }
}

#[derive(Clone, Debug)]
pub struct BlockTank {
    block_id: block::Id,
    block_bytes: Bytes,
}

pub fn stress_loop(params: Params, blocks: &mut Vec<BlockTank>, counter: &mut Counter, limits: &Limits) -> Result<(), Error> {
    let edeltraud = edeltraud::Builder::new()
        .build::<_, JobUnit<_>>()
        .map_err(Error::ThreadPool)?;
    let thread_pool = edeltraud.handle();
    let blocks_pool = BytesPool::new();

    let blockwheel_fs_meister =
        Meister::versklaven(
            params.clone(),
            blocks_pool.clone(),
            &thread_pool,
        )
        .map_err(Error::BlockwheelFs)?;

    let (ftd_tx, ftd_rx) = mpsc::channel();
    let ftd_sklave_meister = arbeitssklave::Freie::new()
        .versklaven(Welt { ftd_tx: Mutex::new(ftd_tx), }, &thread_pool)
        .unwrap();
    #[allow(clippy::redundant_clone)]
    let ftd_sendegeraet =
        komm::Sendegeraet::starten(&ftd_sklave_meister, thread_pool.clone());

    blockwheel_fs_meister.info(ftd_sendegeraet.rueckkopplung(ReplyInfo), &thread_pool)
        .map_err(Error::RequestInfo)?;
    let info = match ftd_rx.recv() {
        Ok(Order::Info(komm::Umschlag { inhalt: info, stamp: ReplyInfo, })) =>
            info,
        other_order =>
            return Err(Error::UnexpectedFtdOrder(format!("{other_order:?}"))),
    };
    log::info!("start | info: {:?}", info);

    let mut active_tasks_counter = Counter::default();
    let mut actions_counter = 0;

    loop {
        if actions_counter >= limits.actions {
            while active_tasks_counter.sum_total() > 0 {
                log::debug!("final flush: {active_tasks_counter:?}");
                process(ftd_rx.recv(), blocks, counter, &mut active_tasks_counter, &ftd_sendegeraet, &thread_pool)?;
            }
            break;
        }

        let maybe_task_result =
            if (active_tasks_counter.sum() >= limits.active_tasks) || (blocks.is_empty() && active_tasks_counter.writes > 0) {
                Some(ftd_rx.recv())
            } else {
                match ftd_rx.try_recv() {
                    Ok(order) =>
                        Some(Ok(order)),
                    Err(mpsc::TryRecvError::Empty) =>
                        None,
                    Err(mpsc::TryRecvError::Disconnected) =>
                        Some(Err(mpsc::RecvError)),
                }
            };

        match maybe_task_result {
            None =>
                (),
            Some(task_result) => {
                process(task_result, blocks, counter, &mut active_tasks_counter, &ftd_sendegeraet, &thread_pool)?;
                continue;
            }
        }

        let bytes_used: usize = blocks.iter()
            .map(|block_tank| block_tank.block_bytes.len())
            .sum();
        let wheel_size_bytes = params.interpreter.init_wheel_size_bytes();
        let bytes_free = wheel_size_bytes - bytes_used;

        if blocks.is_empty() || rand::thread_rng().gen_range(0.0f32 .. 1.0) < 0.5 {
            // write or delete task
            let write_prob = if bytes_free * 2 >= wheel_size_bytes {
                1.0
            } else {
                (bytes_free * 2) as f64 / wheel_size_bytes as f64
            };
            let dice = rand::thread_rng().gen_range(0.0 .. 1.0);
            if blocks.is_empty() || dice < write_prob {
                // write task
                log::debug!("new write_task: bytes_used: {bytes_used:?}, bytes_free: {bytes_free:?}, {active_tasks_counter:?}");

                let job = JobWriteBlockArgs {
                    main: JobWriteBlockArgsMain {
                        blocks_pool: blocks_pool.clone(),
                        block_size_bytes: limits.block_size_bytes,
                        blockwheel_fs_meister: blockwheel_fs_meister.clone(),
                        ftd_sendegeraet: ftd_sendegeraet.clone(),
                    },
                    job_complete: ftd_sendegeraet.rueckkopplung(WriteBlockJob),
                };
                edeltraud::job(&thread_pool, job)
                    .map_err(Error::Edeltraud)?;
                active_tasks_counter.writes += 1;
                active_tasks_counter.write_jobs += 1;
            } else {
                // delete task
                log::debug!("new delete_task: bytes_used: {bytes_used:?}, bytes_free: {bytes_free:?}, {active_tasks_counter:?}");

                let block_index = rand::thread_rng().gen_range(0 .. blocks.len());
                let BlockTank { block_id, .. } = blocks.swap_remove(block_index);
                let rueckkopplung = ftd_sendegeraet.rueckkopplung(ReplyDeleteBlock);
                blockwheel_fs_meister.delete_block(block_id, rueckkopplung, &thread_pool)
                    .map_err(Error::RequestDeleteBlock)?;
                active_tasks_counter.deletes += 1;
            }
        } else {
            // read task
            log::debug!("new read_task: bytes_used: {bytes_used:?}, bytes_free: {bytes_free:?}, {active_tasks_counter:?}");

            let block_index = rand::thread_rng().gen_range(0 .. blocks.len());
            let BlockTank { block_id, block_bytes, } = blocks[block_index].clone();
            let stamp = ReplyReadBlock { block_id: block_id.clone(), block_bytes, };
            let rueckkopplung = ftd_sendegeraet.rueckkopplung(stamp);
            blockwheel_fs_meister
                .read_block(block_id, rueckkopplung, &thread_pool)
                .map_err(Error::RequestReadBlock)?;
            active_tasks_counter.reads += 1;
        }
        actions_counter += 1;
    }

    log::info!("before final check: active_tasks_counter = {active_tasks_counter:?}, counter = {counter:?}, actions_counter = {actions_counter}");

    match ftd_rx.try_recv() {
        Err(mpsc::TryRecvError::Empty) =>
            (),
        other_order =>
            return Err(Error::UnexpectedFtdOrder(format!("{other_order:?}"))),
    }

    log::info!("finish | invoking flush; {active_tasks_counter:?}");

    blockwheel_fs_meister.flush(ftd_sendegeraet.rueckkopplung(ReplyFlush), &thread_pool)
        .map_err(Error::RequestFlush)?;
    match ftd_rx.recv() {
        Ok(Order::Flush(komm::Umschlag { inhalt: Flushed, stamp: ReplyFlush, })) =>
            (),
        other_order =>
            return Err(Error::UnexpectedFtdOrder(format!("{other_order:?}"))),
    }

    blockwheel_fs_meister.info(ftd_sendegeraet.rueckkopplung(ReplyInfo), &thread_pool)
        .map_err(Error::RequestInfo)?;
    let info = match ftd_rx.recv() {
        Ok(Order::Info(komm::Umschlag { inhalt: info, stamp: ReplyInfo, })) =>
            info,
        other_order =>
            return Err(Error::UnexpectedFtdOrder(format!("{other_order:?}"))),
    };
    log::info!("finish | flushed: {:?}", info);

    // backwards check with iterator
    blockwheel_fs_meister.iter_blocks_init(ftd_sendegeraet.rueckkopplung(ReplyIterBlocksInit), &thread_pool)
        .map_err(Error::RequestIterBlocksInit)?;
    let iter_blocks = match ftd_rx.recv() {
        Ok(Order::IterBlocksInit(komm::Umschlag { inhalt: iter_blocks, stamp: ReplyIterBlocksInit, })) =>
            iter_blocks,
        other_order =>
            return Err(Error::UnexpectedFtdOrder(format!("{other_order:?}"))),
    };
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
    let mut current_iterator_next = iter_blocks.iterator_next;
    loop {
        blockwheel_fs_meister
            .iter_blocks_next(
                current_iterator_next,
                ftd_sendegeraet.rueckkopplung(ReplyIterBlocksNext),
                &thread_pool,
            )
            .map_err(Error::RequestIterBlocksNext)?;
        let iter_blocks_item = match ftd_rx.recv() {
            Ok(Order::IterBlocksNext(komm::Umschlag { inhalt: iter_blocks_item, stamp: ReplyIterBlocksNext, })) =>
                iter_blocks_item,
            other_order =>
                return Err(Error::UnexpectedFtdOrder(format!("{other_order:?}"))),
        };

        match iter_blocks_item {
            IterBlocksItem::Block { block_id, block_bytes, iterator_next, } =>
                match blocks.iter().find(|tank| tank.block_id == block_id) {
                    None =>
                        return Err(Error::IterBlocksUnexpectedBlockReceived { block_id, }),
                    Some(tank) => {
                        run_job_verify_block(
                            JobVerifyBlockArgsMain {
                                block_id,
                                provided_block_bytes: block_bytes,
                                expected_block_bytes: tank.block_bytes.clone(),
                            },
                        )?;
                        actual_count += 1;
                        current_iterator_next = iterator_next;
                    },
                },
            IterBlocksItem::NoMoreBlocks =>
                break,
        }
    }

    assert_eq!(actual_count, iter_blocks.blocks_total_count);

    Ok(())
}

fn process<J>(
    maybe_recv_order: Result<Order, mpsc::RecvError>,
    blocks: &mut Vec<BlockTank>,
    counter: &mut Counter,
    active_tasks_counter: &mut Counter,
    ftd_sendegeraet: &komm::Sendegeraet<Order>,
    thread_pool: &edeltraud::Handle<J>,
)
    -> Result<(), Error>
where J: From<JobVerifyBlockArgs>,
{
    let recv_order = maybe_recv_order
        .map_err(|mpsc::RecvError| Error::FtdProcessIsLost)?;
    match recv_order {
        order @ Order::Info(komm::Umschlag { stamp: ReplyInfo, .. }) =>
            Err(Error::UnexpectedFtdOrder(format!("{order:?}"))),
        order @ Order::Flush(komm::Umschlag { inhalt: Flushed, stamp: ReplyFlush, }) =>
            Err(Error::UnexpectedFtdOrder(format!("{order:?}"))),
        Order::WriteBlock(komm::Umschlag { inhalt: Ok(block_id), stamp: ReplyWriteBlock { block_bytes, }, }) => {
            blocks.push(BlockTank { block_id, block_bytes, });
            counter.writes += 1;
            active_tasks_counter.writes -= 1;
            Ok(())
        },
        Order::WriteBlock(komm::Umschlag { inhalt: Err(RequestWriteBlockError::NoSpaceLeft), stamp: ReplyWriteBlock { .. }, }) => {
            counter.no_space_hits += 1;
            counter.writes += 1;
            active_tasks_counter.writes -= 1;
            Ok(())
        },
        Order::ReadBlock(komm::Umschlag { inhalt: Ok(provided_block_bytes), stamp: ReplyReadBlock { block_id, block_bytes, }, }) => {
            let job = JobVerifyBlockArgs {
                main: JobVerifyBlockArgsMain {
                    block_id,
                    provided_block_bytes,
                    expected_block_bytes: block_bytes,
                },
                job_complete: ftd_sendegeraet.rueckkopplung(VerifyBlockJob),
            };
            edeltraud::job(thread_pool, job)
                .map_err(Error::Edeltraud)?;
            counter.reads += 1;
            active_tasks_counter.reads -= 1;
            active_tasks_counter.verify_jobs += 1;
            Ok(())
        },
        Order::ReadBlock(komm::Umschlag { inhalt: Err(RequestReadBlockError::NotFound), stamp: ReplyReadBlock { block_id, .. }, }) => {
            counter.reads += 1;
            active_tasks_counter.reads -= 1;
            assert!(blocks.iter().any(|tank| tank.block_id == block_id));
            Ok(())
        },
        Order::DeleteBlock(komm::Umschlag { inhalt: Ok(Deleted), stamp: ReplyDeleteBlock, }) => {
            counter.deletes += 1;
            active_tasks_counter.deletes -= 1;
            Ok(())
        },
        Order::DeleteBlock(komm::Umschlag { inhalt: Err(error), stamp: ReplyDeleteBlock, }) =>
            Err(Error::DeleteBlock(error)),
        order @ Order::IterBlocksInit(komm::Umschlag { stamp: ReplyIterBlocksInit, .. }) =>
            Err(Error::UnexpectedFtdOrder(format!("{order:?}"))),
        order @ Order::IterBlocksNext(komm::Umschlag { stamp: ReplyIterBlocksNext, .. }) =>
            Err(Error::UnexpectedFtdOrder(format!("{order:?}"))),

        order @ Order::InfoCancel(komm::UmschlagAbbrechen { stamp: ReplyInfo, }) =>
            Err(Error::UnexpectedFtdOrder(format!("{order:?}"))),
        order @ Order::FlushCancel(komm::UmschlagAbbrechen { stamp: ReplyFlush, }) =>
            Err(Error::UnexpectedFtdOrder(format!("{order:?}"))),
        order @ Order::WriteBlockCancel(komm::UmschlagAbbrechen { stamp: ReplyWriteBlock { .. }, }) =>
            Err(Error::UnexpectedFtdOrder(format!("{order:?}"))),
        order @ Order::ReadBlockCancel(komm::UmschlagAbbrechen { stamp: ReplyReadBlock { .. }, }) =>
            Err(Error::UnexpectedFtdOrder(format!("{order:?}"))),
        order @ Order::DeleteBlockCancel(komm::UmschlagAbbrechen { stamp: ReplyDeleteBlock, }) =>
            Err(Error::UnexpectedFtdOrder(format!("{order:?}"))),
        order @ Order::IterBlocksInitCancel(komm::UmschlagAbbrechen { stamp: ReplyIterBlocksInit, }) =>
            Err(Error::UnexpectedFtdOrder(format!("{order:?}"))),
        order @ Order::IterBlocksNextCancel(komm::UmschlagAbbrechen { stamp: ReplyIterBlocksNext, }) =>
            Err(Error::UnexpectedFtdOrder(format!("{order:?}"))),

        Order::JobWriteBlockCancel(komm::UmschlagAbbrechen { stamp: WriteBlockJob, }) =>
            Err(Error::JobWriteBlockCanceled),
        Order::JobWriteBlock(komm::Umschlag { inhalt: output, stamp: WriteBlockJob, }) => {
            counter.write_jobs += 1;
            active_tasks_counter.write_jobs -= 1;
            output
        },
        Order::JobVerifyBlockCancel(komm::UmschlagAbbrechen { stamp: VerifyBlockJob, }) =>
            Err(Error::JobVerifyBlockCanceled),
        Order::JobVerifyBlock(komm::Umschlag { inhalt: Ok(()), stamp: VerifyBlockJob, }) => {
            counter.verify_jobs += 1;
            active_tasks_counter.verify_jobs -= 1;
            Ok(())
        },
        Order::JobVerifyBlock(komm::Umschlag { inhalt: Err(error), stamp: VerifyBlockJob, }) =>
            Err(error),
    }
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

    JobWriteBlock(komm::Umschlag<Result<(), Error>, WriteBlockJob>),
    JobWriteBlockCancel(komm::UmschlagAbbrechen<WriteBlockJob>),
    JobVerifyBlock(komm::Umschlag<Result<(), Error>, VerifyBlockJob>),
    JobVerifyBlockCancel(komm::UmschlagAbbrechen<VerifyBlockJob>),
}

#[derive(Debug)]
struct ReplyInfo;

#[derive(Debug)]
struct ReplyFlush;

#[derive(Debug)]
struct ReplyWriteBlock {
    block_bytes: Bytes,
}

#[derive(Debug)]
struct ReplyReadBlock {
    block_id: block::Id,
    block_bytes: Bytes,
}

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

impl From<komm::UmschlagAbbrechen<WriteBlockJob>> for Order {
    fn from(v: komm::UmschlagAbbrechen<WriteBlockJob>) -> Order {
        Order::JobWriteBlockCancel(v)
    }
}

impl From<komm::Umschlag<Result<(), Error>, WriteBlockJob>> for Order {
    fn from(v: komm::Umschlag<Result<(), Error>, WriteBlockJob>) -> Order {
        Order::JobWriteBlock(v)
    }
}

impl From<komm::UmschlagAbbrechen<VerifyBlockJob>> for Order {
    fn from(v: komm::UmschlagAbbrechen<VerifyBlockJob>) -> Order {
        Order::JobVerifyBlockCancel(v)
    }
}

impl From<komm::Umschlag<Result<(), Error>, VerifyBlockJob>> for Order {
    fn from(v: komm::Umschlag<Result<(), Error>, VerifyBlockJob>) -> Order {
        Order::JobVerifyBlock(v)
    }
}

struct LocalEchoPolicy;

impl EchoPolicy for LocalEchoPolicy {
    type Info = komm::Rueckkopplung<Order, ReplyInfo>;
    type Flush = komm::Rueckkopplung<Order, ReplyFlush>;
    type WriteBlock = komm::Rueckkopplung<Order, ReplyWriteBlock>;
    type ReadBlock = komm::Rueckkopplung<Order, ReplyReadBlock>;
    type DeleteBlock = komm::Rueckkopplung<Order, ReplyDeleteBlock>;
    type IterBlocksInit = komm::Rueckkopplung<Order, ReplyIterBlocksInit>;
    type IterBlocksNext = komm::Rueckkopplung<Order, ReplyIterBlocksNext>;
}

struct Welt {
    ftd_tx: Mutex<mpsc::Sender<Order>>,
}

enum Job {
    BlockwheelFs(job::Job<LocalEchoPolicy>),
    WriteBlock(JobWriteBlockArgs),
    VerifyBlock(JobVerifyBlockArgs),
    FtdSklave(arbeitssklave::SklaveJob<Welt, Order>),
}

impl From<job::Job<LocalEchoPolicy>> for Job {
    fn from(job: job::Job<LocalEchoPolicy>) -> Self {
        Self::BlockwheelFs(job)
    }
}

impl From<performer_sklave::SklaveJob<LocalEchoPolicy>> for Job {
    fn from(job: performer_sklave::SklaveJob<LocalEchoPolicy>) -> Self {
        Self::BlockwheelFs(job.into())
    }
}

impl From<interpret::BlockPrepareWriteJob<LocalEchoPolicy>> for Job {
    fn from(job: interpret::BlockPrepareWriteJob<LocalEchoPolicy>) -> Self {
        Self::BlockwheelFs(job.into())
    }
}

impl From<interpret::BlockPrepareDeleteJob<LocalEchoPolicy>> for Job {
    fn from(job: interpret::BlockPrepareDeleteJob<LocalEchoPolicy>) -> Self {
        Self::BlockwheelFs(job.into())
    }
}

impl From<interpret::BlockProcessReadJob<LocalEchoPolicy>> for Job {
    fn from(job: interpret::BlockProcessReadJob<LocalEchoPolicy>) -> Self {
        Self::BlockwheelFs(job.into())
    }
}

impl From<JobWriteBlockArgs> for Job {
    fn from(args: JobWriteBlockArgs) -> Self {
        Self::WriteBlock(args)
    }
}

impl From<JobVerifyBlockArgs> for Job {
    fn from(args: JobVerifyBlockArgs) -> Self {
        Self::VerifyBlock(args)
    }
}

impl From<arbeitssklave::SklaveJob<Welt, Order>> for Job {
    fn from(job: arbeitssklave::SklaveJob<Welt, Order>) -> Self {
        Self::FtdSklave(job)
    }
}

pub struct JobUnit<J>(edeltraud::JobUnit<J, Job>);

impl<J> From<edeltraud::JobUnit<J, Job>> for JobUnit<J> {
    fn from(job_unit: edeltraud::JobUnit<J, Job>) -> Self {
        Self(job_unit)
    }
}

impl<J> edeltraud::Job for JobUnit<J>
where J: From<performer_sklave::SklaveJob<LocalEchoPolicy>>,
      J: From<interpret::BlockPrepareWriteJob<LocalEchoPolicy>>,
      J: From<interpret::BlockPrepareDeleteJob<LocalEchoPolicy>>,
      J: From<interpret::BlockProcessReadJob<LocalEchoPolicy>>,
{
    fn run(self) {
        match self.0.job {
            Job::BlockwheelFs(job) => {
                let job_unit = job::JobUnit::from(edeltraud::JobUnit {
                    handle: self.0.handle,
                    job,
                });
                job_unit.run();
            },
            Job::WriteBlock(args) =>
                job_write_block(args, &self.0.handle),
            Job::VerifyBlock(args) =>
                job_verify_block(args),
            Job::FtdSklave(mut sklave_job) => {
                #[allow(clippy::while_let_loop)]
                loop {
                    match sklave_job.zu_ihren_diensten() {
                        Ok(arbeitssklave::Gehorsam::Machen { mut befehle, }) =>
                            loop {
                                match befehle.befehl() {
                                    arbeitssklave::SklavenBefehl::Mehr { befehl, mehr_befehle, } => {
                                        befehle = mehr_befehle;
                                        let Ok(tx_lock) = befehle.ftd_tx.lock() else {
                                            log::error!("failed to lock mutex in FtdSklave job, terminating");
                                            return;
                                        };
                                        let Ok(()) = tx_lock.send(befehl) else {
                                            log::error!("failed to send back order in FtdSklave job, terminating");
                                            return;
                                        };
                                    },
                                    arbeitssklave::SklavenBefehl::Ende { sklave_job: next_sklave_job, } => {
                                        sklave_job = next_sklave_job;
                                        break;
                                    },
                                }
                            },
                        Ok(arbeitssklave::Gehorsam::Rasten) =>
                            break,
                        Err(error) => {
                            log::info!("FtdSklave::zu_ihren_diensten terminated with {error:?}");
                            break;
                        },
                    }
                }
            },
        }
    }
}

#[derive(Debug)]
struct WriteBlockJob;

struct JobWriteBlockArgs {
    main: JobWriteBlockArgsMain,
    job_complete: komm::Rueckkopplung<Order, WriteBlockJob>,
}

struct JobWriteBlockArgsMain {
    blocks_pool: BytesPool,
    block_size_bytes: usize,
    blockwheel_fs_meister: Meister<LocalEchoPolicy>,
    ftd_sendegeraet: komm::Sendegeraet<Order>,
}

fn job_write_block<J>(args: JobWriteBlockArgs, thread_pool: &edeltraud::Handle<J>)
where J: From<performer_sklave::SklaveJob<LocalEchoPolicy>>,
{
    let output = run_job_write_block(args.main, thread_pool);
    args.job_complete.commit(output).ok();
}

fn run_job_write_block<J>(args: JobWriteBlockArgsMain, thread_pool: &edeltraud::Handle<J>) -> Result<(), Error>
where J: From<performer_sklave::SklaveJob<LocalEchoPolicy>>,
{
    let mut rng = rand::thread_rng();
    let amount = rng.gen_range(1 .. args.block_size_bytes);
    let mut block = args.blocks_pool.lend();
    block.extend((0 .. amount).map(|_| 0));
    rng.fill(&mut block[..]);
    let block_bytes = block.freeze();
    let stamp = ReplyWriteBlock {
        block_bytes: block_bytes.clone(),
    };
    let rueckkopplung = args.ftd_sendegeraet.rueckkopplung(stamp);
    args.blockwheel_fs_meister
        .write_block(block_bytes, rueckkopplung, thread_pool)
        .map_err(Error::RequestWriteBlock)
}

#[derive(Debug)]
struct VerifyBlockJob;

struct JobVerifyBlockArgs {
    main: JobVerifyBlockArgsMain,
    job_complete: komm::Rueckkopplung<Order, VerifyBlockJob>,
}

struct JobVerifyBlockArgsMain {
    block_id: block::Id,
    provided_block_bytes: Bytes,
    expected_block_bytes: Bytes,
}

fn job_verify_block(args: JobVerifyBlockArgs) {
    let output = run_job_verify_block(args.main);
    args.job_complete.commit(output).ok();
}

fn run_job_verify_block(args: JobVerifyBlockArgsMain) -> Result<(), Error> {
    let expected_crc = block::crc(&args.expected_block_bytes);
    let provided_crc = block::crc(&args.provided_block_bytes);
    if expected_crc != provided_crc {
        Err(Error::ReadBlockCrcMismarch { block_id: args.block_id, expected_crc, provided_crc, })
    } else {
        Ok(())
    }
}
