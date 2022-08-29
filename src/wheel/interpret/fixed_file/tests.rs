use std::{
    fs,
    sync::{
        mpsc,
    },
};

use futures::{
    channel::{
        oneshot,
    },
};

use alloc_pool::bytes::{
    Bytes,
    BytesMut,
    BytesPool,
};

use crate::{
    block,
    context::Context,
    wheel::{
        lru,
        core::{
            task,
            schema,
            performer,
        },
        interpret,
        performer_sklave,
    },
};

use super::{
    OpenParams,
    CreateParams,
    WheelOpenStatus,
    WheelData,
};

#[test]
fn create_read_empty() {
    let wheel_filename = "/tmp/blockwheel_create_read_empty";
    let _wheel_data =
        GenServerInit::create::<LocalContext, _>(
            CreateParams {
                wheel_filename,
                init_wheel_size_bytes: 256 * 1024,
            },
            performer::PerformerBuilderInit::new(
                lru::Cache::new(0),
                None,
                64 * 1024,
            ).unwrap(),
        )
        .unwrap();
    let _wheel_open_status =
        GenServerInit::open::<LocalContext, _>(
            OpenParams {
                wheel_filename,
            },
            performer::PerformerBuilderInit::new(
                lru::Cache::new(0),
                None,
                64 * 1024,
            ).unwrap(),
        )
        .unwrap();
    fs::remove_file(wheel_filename)
        .unwrap();
}

#[test]
fn create_read_one() {
    let thread_pool: edeltraud::Edeltraud<Job> = edeltraud::Builder::new()
        .worker_threads(1)
        .build()
        .unwrap();

    let (orders_tx, orders_rx) = mpsc::channel();
    let meister =
        arbeitssklave::Meister::start(
            Welt { orders_tx, },
            &thread_pool,
        )
        .unwrap();

    let blocks_pool = BytesPool::new();

    let wheel_filename = "/tmp/blockwheel_create_read_one";
    let context = "ectx00";

    let WheelData { sync_gen_server_init: gen_server_init, performer, } =
        GenServerInit::create::<LocalContext, _>(
            CreateParams {
                wheel_filename,
                init_wheel_size_bytes: 256 * 1024,
            },
            performer::PerformerBuilderInit::new(
                lru::Cache::new(0),
                None,
                64 * 1024,
            ).unwrap(),
        )
        .unwrap();
    let gen_server = gen_server_init.finish::<LocalContext>();
    let gen_server_pid = gen_server.pid();

    let (error_tx, _error_rx) = oneshot::channel();
    gen_server.run(meister.clone(), thread_pool.clone(), blocks_pool.clone(), error_tx, std::convert::identity)
        .unwrap();

    let schema = performer.decompose();

    let block_id = block::Id::init();
    let interpret::RunBlockPrepareWriteJobDone { write_block_bytes, } =
        interpret::run_block_prepare_write_job(
            block_id.clone(),
            hello_world_bytes(),
            blocks_pool.clone(),
        )
        .unwrap();
    let task = task::Task {
        block_id,
        kind: task::TaskKind::WriteBlock(task::WriteBlock {
            write_block_bytes,
            commit: task::Commit::WithTerminator,
            context: task::WriteBlockContext::External(context),
        }),
    };
    gen_server_pid.push_request(schema.storage_layout().wheel_header_size as u64, task)
        .unwrap();
    let order = orders_rx.recv().unwrap();
    match order {
        performer_sklave::Order::TaskDoneStats(
            performer_sklave::OrderTaskDoneStats {
                task_done: task::Done {
                    task: task::TaskDone {
                        block_id,
                        kind: task::TaskDoneKind::WriteBlock(task::TaskDoneWriteBlock { context: task::WriteBlockContext::External(ctx), }),
                    },
                    ..
                },
                ..
            },
        ) if block_id == block::Id::init() && ctx == context => {
            let fctx = "fctx00";
            gen_server_pid.device_sync(fctx).unwrap();
            let order = orders_rx.recv().unwrap();
            match order {
                performer_sklave::Order::DeviceSyncDone(performer_sklave::OrderDeviceSyncDone { flush_context, }) if flush_context == fctx =>
                    (),
                other_order =>
                    panic!("unexpected order received: {other_order:?}, expected device sync done with {fctx:?} context"),
            }
        },
        other_order =>
            panic!("unexpected order received: {other_order:?}, expected task done write block {:?} with {context:?} context", block::Id::init()),
    }

    let open_status =
        GenServerInit::open::<LocalContext, _>(
            OpenParams {
                wheel_filename,
            },
            performer::PerformerBuilderInit::new(
                lru::Cache::new(0),
                None,
                64 * 1024,
            ).unwrap(),
        )
        .unwrap();
    let WheelData { sync_gen_server_init: gen_server_init, performer, } = match open_status {
        WheelOpenStatus::Success(wheel_data) =>
            wheel_data,
        WheelOpenStatus::FileNotFound { .. } =>
            panic!("file not found: {:?}", wheel_filename),
    };
    let gen_server = gen_server_init.finish::<LocalContext>();
    let gen_server_pid = gen_server.pid();

    let (error_tx, _error_rx) = oneshot::channel();
    gen_server.run(meister.clone(), thread_pool.clone(), blocks_pool.clone(), error_tx, std::convert::identity)
        .unwrap();

    let mut schema = performer.decompose();

    let block_id = block::Id::init();
    let expected_offset = schema.storage_layout().wheel_header_size as u64;
    let (block_header, block_bytes) = match schema.process_read_block_request(&block_id) {
        schema::ReadBlockOp::CacheHit(schema::ReadBlockCacheHit { .. }) =>
            panic!("unexpected cache hit for block_id = {block_id:?}"),
        schema::ReadBlockOp::Perform(schema::ReadBlockPerform { block_header, }) => {
            let task = task::Task {
                block_id: block_header.block_id.clone(),
                kind: task::TaskKind::ReadBlock(task::ReadBlock {
                    block_header: block_header.clone(),
                    context: task::ReadBlockContext::Process(task::ReadBlockProcessContext::External(context)),
                })
            };
            gen_server_pid.push_request(expected_offset, task)
                .unwrap();
            let order = orders_rx.recv().unwrap();
            match order {
                performer_sklave::Order::TaskDoneStats(
                    performer_sklave::OrderTaskDoneStats {
                        task_done: task::Done {
                            task: task::TaskDone {
                                block_id,
                                kind: task::TaskDoneKind::ReadBlock(task::TaskDoneReadBlock {
                                    block_bytes,
                                    context: task::ReadBlockContext::Process(task::ReadBlockProcessContext::External(ctx)),
                                    ..
                                }),
                            },
                            ..
                        },
                        ..
                    },
                ) if block_id == block_header.block_id && ctx == context =>
                    (block_header.clone(), block_bytes),
                other_order =>
                    panic!(
                        "unexpected order received: {other_order:?}, expected task done read block {:?} with {context:?} context",
                        block_header.block_id,
                    ),
            }
        },
        schema::ReadBlockOp::NotFound =>
            panic!("unexpected read not found for block_id = {block_id:?}"),
    };
    let interpret::RunBlockProcessReadJobDone { block_bytes, .. } =
        interpret::run_block_process_read_job(
            schema.storage_layout().clone(),
            block_header.clone(),
            block_bytes.freeze(),
        )
        .unwrap();
    assert_eq!(block_bytes, hello_world_bytes());

    fs::remove_file(wheel_filename)
        .unwrap();
}

// #[test]
// fn create_write_overlap_read_one() {
//     let runtime = tokio::runtime::Builder::new_current_thread()
//         .build()
//         .unwrap();
//     let wheel_filename = "/tmp/create_write_overlap_read_one";
//     let context = "ectx01";
//     runtime.block_on(async {
//         let WheelData { sync_gen_server: gen_server, performer, } = GenServer::create(
//             CreateParams {
//                 wheel_filename,
//                 init_wheel_size_bytes: 256 * 1024,
//             },
//             performer::PerformerBuilderInit::new(
//                 lru::Cache::new(0),
//                 None,
//                 64 * 1024,
//             ).map_err(Error::PerformerBuild)?,
//         ).map_err(Error::Create)?;
//         let schema = performer.decompose();
//         with_gen_server(gen_server, |mut pid, blocks_pool| async move {

//             // write first block
//             let block_id = block::Id::init();
//             let interpret::BlockPrepareWriteJobDone { write_block_bytes, } = interpret::block_prepare_write_job(
//                 interpret::BlockPrepareWriteJobArgs {
//                     block_id: block_id.clone(),
//                     block_bytes: hello_world_bytes(),
//                     blocks_pool: blocks_pool.clone(),
//                 },
//             ).map_err(Error::WriteBlockPrepare)?;
//             let task::Done { task: task::TaskDone { .. }, .. } = request_reply(
//                 &mut pid,
//                 schema.storage_layout().wheel_header_size as u64,
//                 block_id,
//                 task::TaskKind::WriteBlock(task::WriteBlock {
//                     write_block_bytes,
//                     commit: task::Commit::WithTerminator,
//                     context: task::WriteBlockContext::External(context),
//                 }),
//             ).await?;

//             // partially overwrite first block with second
//             let block_id = block::Id::init().next();
//             let interpret::BlockPrepareWriteJobDone { write_block_bytes, } = interpret::block_prepare_write_job(
//                 interpret::BlockPrepareWriteJobArgs {
//                     block_id: block_id.clone(),
//                     block_bytes: hello_world_bytes(),
//                     blocks_pool: blocks_pool.clone(),
//                 },
//             ).map_err(Error::WriteBlockPrepare)?;
//             let task::Done { task: task::TaskDone { .. }, .. } = request_reply(
//                 &mut pid,
//                 schema.storage_layout().wheel_header_size as u64
//                     + schema.storage_layout().block_header_size as u64,
//                 block_id,
//                 task::TaskKind::WriteBlock(task::WriteBlock {
//                     write_block_bytes,
//                     commit: task::Commit::WithTerminator,
//                     context: task::WriteBlockContext::External(context),
//                 }),
//             ).await?;

//             Ok(())
//         }).await?;

//         let open_status = GenServer::open(
//             OpenParams {
//                 wheel_filename,
//             },
//             performer::PerformerBuilderInit::new(
//                 lru::Cache::new(0),
//                 None,
//                 64 * 1024,
//             ).map_err(Error::PerformerBuild)?,
//         ).map_err(Error::Open)?;
//         let WheelData { sync_gen_server: gen_server, performer, } = match open_status {
//             WheelOpenStatus::Success(wheel_data) =>
//                 wheel_data,
//             WheelOpenStatus::FileNotFound { .. } =>
//                 panic!("file not found: {:?}", wheel_filename),
//         };
//         let mut schema = performer.decompose();
//         with_gen_server(gen_server, |mut pid, _blocks_pool| async move {
//             let block_id = block::Id::init();
//             match schema.process_read_block_request(&block_id) {
//                 schema::ReadBlockOp::CacheHit(schema::ReadBlockCacheHit { .. }) =>
//                     return Err(Error::Unexpected(UnexpectedError::ReadCacheHit { block_id, })),
//                 schema::ReadBlockOp::Perform(schema::ReadBlockPerform { .. }) =>
//                     return Err(Error::Unexpected(UnexpectedError::ReadPerform { block_id, })),
//                 schema::ReadBlockOp::NotFound =>
//                     (),
//             }
//             let block_id = block_id.next();
//             let expected_offset = schema.storage_layout().wheel_header_size as u64
//                 + schema.storage_layout().block_header_size as u64;
//             let (block_header, block_bytes) = match schema.process_read_block_request(&block_id) {
//                 schema::ReadBlockOp::CacheHit(schema::ReadBlockCacheHit { .. }) =>
//                     return Err(Error::Unexpected(UnexpectedError::ReadCacheHit { block_id, })),
//                 schema::ReadBlockOp::Perform(schema::ReadBlockPerform { block_header, }) => {
//                     let task_done = request_reply(
//                         &mut pid,
//                         expected_offset,
//                         block_header.block_id.clone(),
//                         task::TaskKind::ReadBlock(task::ReadBlock {
//                             block_header: block_header.clone(),
//                             context: task::ReadBlockContext::Process(task::ReadBlockProcessContext::External(context)),
//                         }),
//                     ).await?;
//                     match task_done {
//                         task::Done {
//                             task: task::TaskDone {
//                                 block_id,
//                                 kind: task::TaskDoneKind::ReadBlock(task::TaskDoneReadBlock {
//                                     block_bytes,
//                                     context: task::ReadBlockContext::Process(task::ReadBlockProcessContext::External(ctx)),
//                                     ..
//                                 }),
//                             },
//                             ..
//                         } if block_id == block_header.block_id && ctx == context =>
//                             Ok((block_header.clone(), block_bytes)),
//                         other_done_task =>
//                             Err(Error::Unexpected(UnexpectedError::ReadDoneTask {
//                                 expected: format!("task done read block {:?} with {:?} context", block_header.block_id, context),
//                                 received: other_done_task,
//                             })),
//                     }
//                 },
//                 schema::ReadBlockOp::NotFound =>
//                     Err(Error::Unexpected(UnexpectedError::ReadNotFound { block_id, })),
//             }?;
//             let interpret::BlockProcessReadJobDone { block_bytes, .. } =
//                 interpret::block_process_read_job(interpret::BlockProcessReadJobArgs {
//                     storage_layout: schema.storage_layout().clone(),
//                     block_header: block_header.clone(),
//                     block_bytes: block_bytes.freeze(),
//                 })
//                 .map_err(Error::ReadBlockProcess)?;
//             assert_eq!(block_bytes, hello_world_bytes());
//             Ok(())
//         }).await?;
//         Ok::<_, Error>(())
//     }).unwrap();
//     fs::remove_file(wheel_filename).unwrap();
// }

// #[test]
// fn create_write_delete_read_one() {
//     let runtime = tokio::runtime::Builder::new_current_thread()
//         .build()
//         .unwrap();
//     let wheel_filename = "/tmp/create_write_delete_read_one";
//     let context = "ectx02";
//     runtime.block_on(async {
//         let WheelData { sync_gen_server: gen_server, performer, } = GenServer::create(
//             CreateParams {
//                 wheel_filename,
//                 init_wheel_size_bytes: 256 * 1024,
//             },
//             performer::PerformerBuilderInit::new(
//                 lru::Cache::new(0),
//                 None,
//                 64 * 1024,
//             ).map_err(Error::PerformerBuild)?,
//         ).map_err(Error::Create)?;
//         let schema = performer.decompose();
//         with_gen_server(gen_server, |mut pid, blocks_pool| async move {

//             // write first block
//             let block_id = block::Id::init();
//             let interpret::BlockPrepareWriteJobDone { write_block_bytes, } = interpret::block_prepare_write_job(
//                 interpret::BlockPrepareWriteJobArgs {
//                     block_id: block_id.clone(),
//                     block_bytes: hello_world_bytes(),
//                     blocks_pool: blocks_pool.clone(),
//                 },
//             ).map_err(Error::WriteBlockPrepare)?;
//             let task::Done { current_offset, task: task::TaskDone { .. }, } = request_reply(
//                 &mut pid,
//                 schema.storage_layout().wheel_header_size as u64,
//                 block_id.clone(),
//                 task::TaskKind::WriteBlock(task::WriteBlock {
//                     write_block_bytes,
//                     commit: task::Commit::WithTerminator,
//                     context: task::WriteBlockContext::External(context),
//                 }),
//             ).await?;

//             // write second block
//             let block_id = block_id.next();
//             let interpret::BlockPrepareWriteJobDone { write_block_bytes, } = interpret::block_prepare_write_job(
//                 interpret::BlockPrepareWriteJobArgs {
//                     block_id: block_id.clone(),
//                     block_bytes: hello_world_bytes(),
//                     blocks_pool: blocks_pool.clone(),
//                 },
//             ).map_err(Error::WriteBlockPrepare)?;
//             let task::Done { task: task::TaskDone { .. }, .. } = request_reply(
//                 &mut pid,
//                 current_offset,
//                 block_id,
//                 task::TaskKind::WriteBlock(task::WriteBlock {
//                     write_block_bytes,
//                     commit: task::Commit::WithTerminator,
//                     context: task::WriteBlockContext::External(context),
//                 }),
//             ).await?;

//             // delete first block
//             let interpret::BlockPrepareDeleteJobDone { delete_block_bytes, } = interpret::block_prepare_delete_job(
//                 interpret::BlockPrepareDeleteJobArgs {
//                     blocks_pool: blocks_pool.clone(),
//                 },
//             ).map_err(Error::DeleteBlockPrepare)?;
//             let task_done = request_reply(
//                 &mut pid,
//                 schema.storage_layout().wheel_header_size as u64,
//                 block::Id::init(),
//                 task::TaskKind::DeleteBlock(task::DeleteBlock {
//                     delete_block_bytes: delete_block_bytes.freeze(),
//                     commit: task::Commit::None,
//                     context: task::DeleteBlockContext::External(context),
//                 }),
//             ).await?;

//             match task_done {
//                 task::Done {
//                     task: task::TaskDone {
//                         block_id,
//                         kind: task::TaskDoneKind::DeleteBlock(task::TaskDoneDeleteBlock { context: task::DeleteBlockContext::External(ctx), }),
//                     },
//                     ..
//                 } if block_id == block::Id::init() && ctx == context =>
//                     Ok(()),
//                 other_done_task =>
//                     Err(Error::Unexpected(UnexpectedError::DeleteDoneTask {
//                         expected: format!("task done delete block {:?} with {:?} context", block::Id::init(), context),
//                         received: other_done_task,
//                     })),
//             }
//         }).await?;

//         let open_status = GenServer::open(
//             OpenParams {
//                 wheel_filename,
//             },
//             performer::PerformerBuilderInit::new(
//                 lru::Cache::new(0),
//                 None,
//                 64 * 1024,
//             ).map_err(Error::PerformerBuild)?,
//         ).map_err(Error::Open)?;
//         let WheelData { sync_gen_server: gen_server, performer, } = match open_status {
//             WheelOpenStatus::Success(wheel_data) =>
//                 wheel_data,
//             WheelOpenStatus::FileNotFound { .. } =>
//                 panic!("file not found: {:?}", wheel_filename),
//         };
//         let mut schema = performer.decompose();
//         with_gen_server(gen_server, |mut pid, _blocks_pool| async move {
//             let block_id = block::Id::init();
//             match schema.process_read_block_request(&block_id) {
//                 schema::ReadBlockOp::CacheHit(schema::ReadBlockCacheHit { .. }) =>
//                     return Err(Error::Unexpected(UnexpectedError::ReadCacheHit { block_id, })),
//                 schema::ReadBlockOp::Perform(schema::ReadBlockPerform { .. }) =>
//                     return Err(Error::Unexpected(UnexpectedError::ReadPerform { block_id, })),
//                 schema::ReadBlockOp::NotFound =>
//                     (),
//             }
//             let block_id = block_id.next();
//             let expected_offset = schema.storage_layout().wheel_header_size as u64
//                 + schema.storage_layout().data_size_block_min() as u64
//                 + hello_world_bytes().len() as u64;
//             let (block_header, block_bytes) = match schema.process_read_block_request(&block_id) {
//                 schema::ReadBlockOp::CacheHit(schema::ReadBlockCacheHit { .. }) =>
//                     return Err(Error::Unexpected(UnexpectedError::ReadCacheHit { block_id, })),
//                 schema::ReadBlockOp::Perform(schema::ReadBlockPerform { block_header, }) => {
//                     let task_done = request_reply(
//                         &mut pid,
//                         expected_offset,
//                         block_header.block_id.clone(),
//                         task::TaskKind::ReadBlock(task::ReadBlock {
//                             block_header: block_header.clone(),
//                             context: task::ReadBlockContext::Process(task::ReadBlockProcessContext::External(context)),
//                         }),
//                     ).await?;
//                     match task_done {
//                         task::Done {
//                             task: task::TaskDone {
//                                 block_id,
//                                 kind: task::TaskDoneKind::ReadBlock(task::TaskDoneReadBlock {
//                                     block_bytes,
//                                     context: task::ReadBlockContext::Process(task::ReadBlockProcessContext::External(ctx)),
//                                     ..
//                                 }),
//                             },
//                             ..
//                         } if block_id == block_header.block_id && ctx == context =>
//                             Ok((block_header.clone(), block_bytes)),
//                         other_done_task =>
//                             Err(Error::Unexpected(UnexpectedError::ReadDoneTask {
//                                 expected: format!("task done read block {:?} with {:?} context", block_header.block_id, context),
//                                 received: other_done_task,
//                             })),
//                     }
//                 },
//                 schema::ReadBlockOp::NotFound =>
//                     Err(Error::Unexpected(UnexpectedError::ReadNotFound { block_id, })),
//             }?;
//             let interpret::BlockProcessReadJobDone { block_bytes, .. } =
//                 interpret::block_process_read_job(interpret::BlockProcessReadJobArgs {
//                     storage_layout: schema.storage_layout().clone(),
//                     block_header: block_header.clone(),
//                     block_bytes: block_bytes.freeze(),
//                 })
//                 .map_err(Error::ReadBlockProcess)?;
//             assert_eq!(block_bytes, hello_world_bytes());
//             Ok(())
//         }).await?;
//         Ok::<_, Error>(())
//     }).unwrap();
//     fs::remove_file(wheel_filename).unwrap();
// }

// #[derive(Debug)]
// enum Error {
//     PerformerBuild(performer::BuilderError),
//     Create(super::WheelCreateError),
//     Open(super::WheelOpenError),
//     Run(super::Error),
//     DeviceSync(ero::NoProcError),
//     InterpreterDetach,
//     Unexpected(UnexpectedError),
//     WriteBlockPrepare(interpret::BlockPrepareWriteJobError),
//     ReadBlockProcess(interpret::BlockProcessReadJobError),
//     DeleteBlockPrepare(interpret::BlockPrepareDeleteJobError),
// }

// #[derive(Debug)]
// pub enum UnexpectedError {
//     WriteDoneTask {
//         expected: String,
//         received: task::Done<LocalContext>,
//     },
//     ReadDoneTask {
//         expected: String,
//         received: task::Done<LocalContext>,
//     },
//     DeleteDoneTask {
//         expected: String,
//         received: task::Done<LocalContext>,
//     },
//     ReadPerform {
//         block_id: block::Id,
//     },
//     ReadCacheHit {
//         block_id: block::Id,
//     },
//     ReadNotFound {
//         block_id: block::Id,
//     },
// }

struct Welt {
    orders_tx: mpsc::Sender<performer_sklave::Order<LocalContext>>,
}

enum Job {
    Sklave(arbeitssklave::SklaveJob<Welt, performer_sklave::Order<LocalContext>>),
}

impl From<arbeitssklave::SklaveJob<Welt, performer_sklave::Order<LocalContext>>> for Job {
    fn from(sklave_job: arbeitssklave::SklaveJob<Welt, performer_sklave::Order<LocalContext>>) -> Job {
        Job::Sklave(sklave_job)
    }
}

impl edeltraud::Job for Job {
    type Output = ();

    fn run<P>(self, _thread_pool: &P) -> Self::Output where P: edeltraud::ThreadPool<Self> {
        match self {
            Job::Sklave(arbeitssklave::SklaveJob { mut sklave, mut sklavenwelt, }) => {
                loop {
                    match sklave.obey(sklavenwelt).unwrap() {
                        arbeitssklave::Obey::Order { order, sklavenwelt: next_sklavenwelt, } => {
                            sklavenwelt = next_sklavenwelt;
                            sklavenwelt.orders_tx.send(order).unwrap();
                        },
                        arbeitssklave::Obey::Rest =>
                            break,
                    }
                }
            },
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct LocalContext;

type C = &'static str;

impl Context for LocalContext {
    type Info = C;
    type Flush = C;
    type WriteBlock = C;
    type ReadBlock = C;
    type DeleteBlock = C;
    type IterBlocks = C;
    type IterBlocksStream = C;
}

type GenServerInit = super::SyncGenServerInit;

// async fn request_reply(
//     pid: &mut Pid,
//     offset: u64,
//     block_id: block::Id,
//     kind: task::TaskKind<LocalContext>,
// )
//     -> Result<task::Done<LocalContext>, Error>
// {
//     let reply_rx = pid.push_request(offset, task::Task { block_id, kind, })
//         .map_err(|ero::NoProcError| Error::InterpreterDetach)?;
//     let super::DoneTask { task_done, .. } = reply_rx.await.map_err(|_| Error::InterpreterDetach)?;
//     Ok(task_done)
// }

fn hello_world_bytes() -> Bytes {
    let mut block_bytes_mut = BytesMut::new_detached(Vec::new());
    block_bytes_mut.extend("hello, world!".as_bytes().iter().cloned());
    block_bytes_mut.freeze()
}
