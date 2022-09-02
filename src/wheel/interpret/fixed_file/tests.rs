use std::{
    fs,
    sync::{
        mpsc,
    },
};

use alloc_pool::{
    bytes::{
        Bytes,
        BytesMut,
        BytesPool,
    }
};

use arbeitssklave::{
    komm,
};

use crate::{
    block,
    wheel::{
        lru,
        core::{
            task,
            schema,
            performer,
        },
        interpret::{
            self,
            fixed_file::{
                WheelOpenStatus,
                WheelData,
            },
        },
        performer_sklave,
    },
    AccessPolicy,
    Info,
    Deleted,
    Flushed,
    IterBlocks,
    IterBlocksItem,
    RequestWriteBlockError,
    RequestReadBlockError,
    RequestDeleteBlockError,
};

#[test]
fn create_read_empty() {
    let wheel_filename = "/tmp/blockwheel_create_read_empty";
    // let _wheel_data =
    //     GenServerInit::create::<LocalContext, _>(
    //         CreateParams {
    //             wheel_filename,
    //             init_wheel_size_bytes: 256 * 1024,
    //         },
    //         performer::PerformerBuilderInit::new(
    //             lru::Cache::new(0),
    //             None,
    //             64 * 1024,
    //         ).unwrap(),
    //     )
    //     .unwrap();
    // let _wheel_open_status =
    //     GenServerInit::open::<LocalContext, _>(
    //         OpenParams {
    //             wheel_filename,
    //         },
    //         performer::PerformerBuilderInit::new(
    //             lru::Cache::new(0),
    //             None,
    //             64 * 1024,
    //         ).unwrap(),
    //     )
    //     .unwrap();
    fs::remove_file(wheel_filename)
        .unwrap();
}

// #[test]
// fn create_read_one() {
//     let thread_pool: edeltraud::Edeltraud<Job> = edeltraud::Builder::new()
//         .worker_threads(1)
//         .build()
//         .unwrap();

//     let (orders_tx, orders_rx) = mpsc::channel();
//     let meister = arbeitssklave::Freie::new()
//         .versklaven(Welt { orders_tx, }, &thread_pool)
//         .unwrap();

//     let blocks_pool = BytesPool::new();

//     let wheel_filename = "/tmp/blockwheel_create_read_one";
//     let context = "ectx00";

//     let WheelData { sync_gen_server_init: gen_server_init, performer, } =
//         GenServerInit::create::<LocalContext, _>(
//             CreateParams {
//                 wheel_filename,
//                 init_wheel_size_bytes: 256 * 1024,
//             },
//             performer::PerformerBuilderInit::new(
//                 lru::Cache::new(0),
//                 None,
//                 64 * 1024,
//             ).unwrap(),
//         )
//         .unwrap();
//     let gen_server = gen_server_init.finish::<LocalContext>();
//     let gen_server_pid = gen_server.pid();

//     gen_server.run(meister.clone(), thread_pool.clone(), blocks_pool.clone())
//         .unwrap();

//     let schema = performer.decompose();

//     let block_id = block::Id::init();
//     let interpret::RunBlockPrepareWriteJobDone { write_block_bytes, } =
//         interpret::run_block_prepare_write_job(
//             block_id.clone(),
//             hello_world_bytes(),
//             blocks_pool.clone(),
//         )
//         .unwrap();
//     let task = task::Task {
//         block_id,
//         kind: task::TaskKind::WriteBlock(task::WriteBlock {
//             write_block_bytes,
//             commit: task::Commit::WithTerminator,
//             context: task::WriteBlockContext::External(context),
//         }),
//     };
//     gen_server_pid.push_request(schema.storage_layout().wheel_header_size as u64, task)
//         .unwrap();
//     let order = orders_rx.recv().unwrap();
//     match order {
//         performer_sklave::Order::TaskDoneStats(
//             performer_sklave::OrderTaskDoneStats {
//                 task_done: task::Done {
//                     task: task::TaskDone {
//                         block_id,
//                         kind: task::TaskDoneKind::WriteBlock(task::TaskDoneWriteBlock { context: task::WriteBlockContext::External(ctx), }),
//                     },
//                     ..
//                 },
//                 ..
//             },
//         ) if block_id == block::Id::init() && ctx == context => {
//             let fctx = "fctx00";
//             gen_server_pid.device_sync(fctx).unwrap();
//             let order = orders_rx.recv().unwrap();
//             match order {
//                 performer_sklave::Order::DeviceSyncDone(performer_sklave::OrderDeviceSyncDone { flush_context, }) if flush_context == fctx =>
//                     (),
//                 other_order =>
//                     panic!("unexpected order received: {other_order:?}, expected device sync done with {fctx:?} context"),
//             }
//         },
//         other_order =>
//             panic!("unexpected order received: {other_order:?}, expected task done write block {:?} with {context:?} context", block::Id::init()),
//     }

//     let open_status =
//         GenServerInit::open::<LocalContext, _>(
//             OpenParams {
//                 wheel_filename,
//             },
//             performer::PerformerBuilderInit::new(
//                 lru::Cache::new(0),
//                 None,
//                 64 * 1024,
//             ).unwrap(),
//         )
//         .unwrap();
//     let WheelData { sync_gen_server_init: gen_server_init, performer, } = match open_status {
//         WheelOpenStatus::Success(wheel_data) =>
//             wheel_data,
//         WheelOpenStatus::FileNotFound { .. } =>
//             panic!("file not found: {:?}", wheel_filename),
//     };
//     let gen_server = gen_server_init.finish::<LocalContext>();
//     let gen_server_pid = gen_server.pid();

//     gen_server.run(meister.clone(), thread_pool.clone(), blocks_pool.clone())
//         .unwrap();

//     let mut schema = performer.decompose();

//     let block_id = block::Id::init();
//     let expected_offset = schema.storage_layout().wheel_header_size as u64;
//     let (block_header, block_bytes) = match schema.process_read_block_request(&block_id) {
//         schema::ReadBlockOp::CacheHit(schema::ReadBlockCacheHit { .. }) =>
//             panic!("unexpected cache hit for block_id = {block_id:?}"),
//         schema::ReadBlockOp::Perform(schema::ReadBlockPerform { block_header, }) => {
//             let task = task::Task {
//                 block_id: block_header.block_id.clone(),
//                 kind: task::TaskKind::ReadBlock(task::ReadBlock {
//                     block_header: block_header.clone(),
//                     context: task::ReadBlockContext::Process(task::ReadBlockProcessContext::External(context)),
//                 })
//             };
//             gen_server_pid.push_request(expected_offset, task)
//                 .unwrap();
//             let order = orders_rx.recv().unwrap();
//             match order {
//                 performer_sklave::Order::TaskDoneStats(
//                     performer_sklave::OrderTaskDoneStats {
//                         task_done: task::Done {
//                             task: task::TaskDone {
//                                 block_id,
//                                 kind: task::TaskDoneKind::ReadBlock(task::TaskDoneReadBlock {
//                                     block_bytes,
//                                     context: task::ReadBlockContext::Process(task::ReadBlockProcessContext::External(ctx)),
//                                     ..
//                                 }),
//                             },
//                             ..
//                         },
//                         ..
//                     },
//                 ) if block_id == block_header.block_id && ctx == context =>
//                     (block_header.clone(), block_bytes),
//                 other_order =>
//                     panic!(
//                         "unexpected order received: {other_order:?}, expected task done read block {:?} with {context:?} context",
//                         block_header.block_id,
//                     ),
//             }
//         },
//         schema::ReadBlockOp::NotFound =>
//             panic!("unexpected read not found for block_id = {block_id:?}"),
//     };
//     let interpret::RunBlockProcessReadJobDone { block_bytes, .. } =
//         interpret::run_block_process_read_job(
//             schema.storage_layout().clone(),
//             block_header.clone(),
//             block_bytes.freeze(),
//         )
//         .unwrap();
//     assert_eq!(block_bytes, hello_world_bytes());

//     fs::remove_file(wheel_filename)
//         .unwrap();
// }

// #[test]
// fn create_write_overlap_read_one() {
//     let thread_pool: edeltraud::Edeltraud<Job> = edeltraud::Builder::new()
//         .worker_threads(1)
//         .build()
//         .unwrap();

//     let (orders_tx, orders_rx) = mpsc::channel();
//     let meister = arbeitssklave::Freie::new()
//         .versklaven(Welt { orders_tx, }, &thread_pool)
//         .unwrap();

//     let blocks_pool = BytesPool::new();

//     let wheel_filename = "/tmp/create_write_overlap_read_one";
//     let context = "ectx01";

//     let WheelData { sync_gen_server_init: gen_server_init, performer, } =
//         GenServerInit::create::<LocalContext, _>(
//             CreateParams {
//                 wheel_filename,
//                 init_wheel_size_bytes: 256 * 1024,
//             },
//             performer::PerformerBuilderInit::new(
//                 lru::Cache::new(0),
//                 None,
//                 64 * 1024,
//             ).unwrap(),
//         )
//         .unwrap();
//     let gen_server = gen_server_init.finish::<LocalContext>();
//     let gen_server_pid = gen_server.pid();

//     gen_server.run(meister.clone(), thread_pool.clone(), blocks_pool.clone())
//         .unwrap();

//     let schema = performer.decompose();

//     // write first block
//     let block_id = block::Id::init();
//     let interpret::RunBlockPrepareWriteJobDone { write_block_bytes, } =
//         interpret::run_block_prepare_write_job(
//             block_id.clone(),
//             hello_world_bytes(),
//             blocks_pool.clone(),
//         )
//         .unwrap();
//     let task = task::Task {
//         block_id,
//         kind: task::TaskKind::WriteBlock(task::WriteBlock {
//             write_block_bytes,
//             commit: task::Commit::WithTerminator,
//             context: task::WriteBlockContext::External(context),
//         }),
//     };
//     gen_server_pid.push_request(schema.storage_layout().wheel_header_size as u64, task)
//         .unwrap();
//     let order = orders_rx.recv().unwrap();
//     assert!(matches!(order, performer_sklave::Order::TaskDoneStats(performer_sklave::OrderTaskDoneStats {
//         task_done: task::Done { task: task::TaskDone { .. }, .. },
//         ..
//     })));

//     // partially overwrite first block with second
//     let block_id = block::Id::init().next();
//     let interpret::RunBlockPrepareWriteJobDone { write_block_bytes, } =
//         interpret::run_block_prepare_write_job(
//             block_id.clone(),
//             hello_world_bytes(),
//             blocks_pool.clone(),
//         )
//         .unwrap();
//     let task = task::Task {
//         block_id,
//         kind: task::TaskKind::WriteBlock(task::WriteBlock {
//             write_block_bytes,
//             commit: task::Commit::WithTerminator,
//             context: task::WriteBlockContext::External(context),
//         }),
//     };
//     gen_server_pid.push_request(
//         schema.storage_layout().wheel_header_size as u64
//             + schema.storage_layout().block_header_size as u64,
//         task,
//     ).unwrap();
//     let order = orders_rx.recv().unwrap();
//     assert!(matches!(order, performer_sklave::Order::TaskDoneStats(performer_sklave::OrderTaskDoneStats {
//         task_done: task::Done { task: task::TaskDone { .. }, .. },
//         ..
//     })));

//     // fsync
//     let fctx = "fctx01";
//     gen_server_pid.device_sync(fctx).unwrap();
//     let order = orders_rx.recv().unwrap();
//     assert!(matches!(order, performer_sklave::Order::DeviceSyncDone(performer_sklave::OrderDeviceSyncDone {
//         flush_context,
//     }) if flush_context == fctx));

//     let open_status =
//         GenServerInit::open::<LocalContext, _>(
//             OpenParams {
//                 wheel_filename,
//             },
//             performer::PerformerBuilderInit::new(
//                 lru::Cache::new(0),
//                 None,
//                 64 * 1024,
//             ).unwrap(),
//         )
//         .unwrap();
//     let WheelData { sync_gen_server_init: gen_server_init, performer, } = match open_status {
//         WheelOpenStatus::Success(wheel_data) =>
//             wheel_data,
//         WheelOpenStatus::FileNotFound { .. } =>
//             panic!("file not found: {:?}", wheel_filename),
//     };
//     let gen_server = gen_server_init.finish::<LocalContext>();
//     let gen_server_pid = gen_server.pid();

//     gen_server.run(meister.clone(), thread_pool.clone(), blocks_pool.clone())
//         .unwrap();

//     let mut schema = performer.decompose();

//     let block_id = block::Id::init();

//     match schema.process_read_block_request(&block_id) {
//         schema::ReadBlockOp::CacheHit(schema::ReadBlockCacheHit { .. }) =>
//             panic!("unexpected cache hit for block_id = {block_id:?}"),
//         schema::ReadBlockOp::Perform(schema::ReadBlockPerform { .. }) =>
//             panic!("unexpected read perform for block_id = {block_id:?}"),
//         schema::ReadBlockOp::NotFound =>
//             (),
//     }
//     let block_id = block_id.next();
//     let expected_offset = schema.storage_layout().wheel_header_size as u64
//         + schema.storage_layout().block_header_size as u64;

//     let (block_header, block_bytes) = match schema.process_read_block_request(&block_id) {
//         schema::ReadBlockOp::CacheHit(schema::ReadBlockCacheHit { .. }) =>
//             panic!("unexpected cache hit for block_id = {block_id:?}"),
//         schema::ReadBlockOp::Perform(schema::ReadBlockPerform { block_header, }) => {
//             let task = task::Task {
//                 block_id: block_header.block_id.clone(),
//                 kind: task::TaskKind::ReadBlock(task::ReadBlock {
//                     block_header: block_header.clone(),
//                     context: task::ReadBlockContext::Process(task::ReadBlockProcessContext::External(context)),
//                 })
//             };
//             gen_server_pid.push_request(expected_offset, task)
//                 .unwrap();
//             let order = orders_rx.recv().unwrap();
//             match order {
//                 performer_sklave::Order::TaskDoneStats(
//                     performer_sklave::OrderTaskDoneStats {
//                         task_done: task::Done {
//                             task: task::TaskDone {
//                                 block_id,
//                                 kind: task::TaskDoneKind::ReadBlock(task::TaskDoneReadBlock {
//                                     block_bytes,
//                                     context: task::ReadBlockContext::Process(task::ReadBlockProcessContext::External(ctx)),
//                                     ..
//                                 }),
//                             },
//                             ..
//                         },
//                         ..
//                     },
//                 ) if block_id == block_header.block_id && ctx == context =>
//                     (block_header.clone(), block_bytes),
//                 other_order =>
//                     panic!(
//                         "unexpected order received: {other_order:?}, expected task done read block {:?} with {context:?} context",
//                         block_header.block_id,
//                     ),
//             }
//         },
//         schema::ReadBlockOp::NotFound =>
//             panic!("unexpected read not found for block_id = {block_id:?}"),
//     };

//     let interpret::RunBlockProcessReadJobDone { block_bytes, .. } =
//         interpret::run_block_process_read_job(
//             schema.storage_layout().clone(),
//             block_header.clone(),
//             block_bytes.freeze(),
//         )
//         .unwrap();
//     assert_eq!(block_bytes, hello_world_bytes());

//     fs::remove_file(wheel_filename).unwrap();
// }

// #[test]
// fn create_write_delete_read_one() {
//     let thread_pool: edeltraud::Edeltraud<Job> = edeltraud::Builder::new()
//         .worker_threads(1)
//         .build()
//         .unwrap();

//     let (orders_tx, orders_rx) = mpsc::channel();
//     let meister = arbeitssklave::Freie::new()
//         .versklaven(Welt { orders_tx, }, &thread_pool)
//         .unwrap();

//     let blocks_pool = BytesPool::new();

//     let wheel_filename = "/tmp/create_write_delete_read_one";
//     let context = "ectx02";

//     let WheelData { sync_gen_server_init: gen_server_init, performer, } =
//         GenServerInit::create::<LocalContext, _>(
//             CreateParams {
//                 wheel_filename,
//                 init_wheel_size_bytes: 256 * 1024,
//             },
//             performer::PerformerBuilderInit::new(
//                 lru::Cache::new(0),
//                 None,
//                 64 * 1024,
//             ).unwrap(),
//         )
//         .unwrap();
//     let gen_server = gen_server_init.finish::<LocalContext>();
//     let gen_server_pid = gen_server.pid();

//     gen_server.run(meister.clone(), thread_pool.clone(), blocks_pool.clone())
//         .unwrap();

//     let schema = performer.decompose();

//     // write first block
//     let block_id = block::Id::init();
//     let interpret::RunBlockPrepareWriteJobDone { write_block_bytes, } =
//         interpret::run_block_prepare_write_job(
//             block_id.clone(),
//             hello_world_bytes(),
//             blocks_pool.clone(),
//         )
//         .unwrap();
//     let task = task::Task {
//         block_id: block_id.clone(),
//         kind: task::TaskKind::WriteBlock(task::WriteBlock {
//             write_block_bytes,
//             commit: task::Commit::WithTerminator,
//             context: task::WriteBlockContext::External(context),
//         }),
//     };
//     gen_server_pid.push_request(schema.storage_layout().wheel_header_size as u64, task)
//         .unwrap();
//     let order = orders_rx.recv().unwrap();
//     let current_offset = match order {
//         performer_sklave::Order::TaskDoneStats(performer_sklave::OrderTaskDoneStats {
//             task_done: task::Done { current_offset, task: task::TaskDone { .. }, },
//             ..
//         }) =>
//             current_offset,
//         other_order =>
//             panic!("unexpected order received: {other_order:?}, expected task done write block {block_id:?} with {context:?} context"),
//     };

//     // write second block
//     let block_id = block_id.next();
//     let interpret::RunBlockPrepareWriteJobDone { write_block_bytes, } =
//         interpret::run_block_prepare_write_job(
//             block_id.clone(),
//             hello_world_bytes(),
//             blocks_pool.clone(),
//         )
//         .unwrap();
//     let task = task::Task {
//         block_id,
//         kind: task::TaskKind::WriteBlock(task::WriteBlock {
//             write_block_bytes,
//             commit: task::Commit::WithTerminator,
//             context: task::WriteBlockContext::External(context),
//         }),
//     };
//     gen_server_pid.push_request(current_offset, task)
//         .unwrap();
//     let order = orders_rx.recv().unwrap();
//     assert!(matches!(order, performer_sklave::Order::TaskDoneStats(performer_sklave::OrderTaskDoneStats {
//         task_done: task::Done { task: task::TaskDone { .. }, .. },
//         ..
//     })));

//     // delete first block
//     let block_id = block::Id::init();
//     let interpret::RunBlockPrepareDeleteJobDone { delete_block_bytes, } =
//         interpret::run_block_prepare_delete_job(
//             blocks_pool.clone(),
//         )
//         .unwrap();
//     let task = task::Task {
//         block_id: block_id.clone(),
//         kind: task::TaskKind::DeleteBlock(task::DeleteBlock {
//             delete_block_bytes: delete_block_bytes.freeze(),
//             commit: task::Commit::None,
//             context: task::DeleteBlockContext::External(context),
//         }),
//     };
//     gen_server_pid.push_request(schema.storage_layout().wheel_header_size as u64, task)
//         .unwrap();
//     let order = orders_rx.recv().unwrap();
//     match order {
//         performer_sklave::Order::TaskDoneStats(performer_sklave::OrderTaskDoneStats {
//             task_done: task::Done {
//                 task: task::TaskDone {
//                     block_id,
//                     kind: task::TaskDoneKind::DeleteBlock(task::TaskDoneDeleteBlock { context: task::DeleteBlockContext::External(ctx), }),
//                 },
//                 ..
//             },
//             ..
//         }) if block_id == block::Id::init() && ctx == context =>
//             (),
//         other_order =>
//             panic!("unexpected order received: {other_order:?}, expected task done delete block {block_id:?} with {context:?} context"),
//     }

//     // fsync
//     let fctx = "fctx02";
//     gen_server_pid.device_sync(fctx).unwrap();
//     let order = orders_rx.recv().unwrap();
//     assert!(matches!(order, performer_sklave::Order::DeviceSyncDone(performer_sklave::OrderDeviceSyncDone {
//         flush_context,
//     }) if flush_context == fctx));

//     let open_status =
//         GenServerInit::open::<LocalContext, _>(
//             OpenParams {
//                 wheel_filename,
//             },
//             performer::PerformerBuilderInit::new(
//                 lru::Cache::new(0),
//                 None,
//                 64 * 1024,
//             ).unwrap(),
//         )
//         .unwrap();
//     let WheelData { sync_gen_server_init: gen_server_init, performer, } = match open_status {
//         WheelOpenStatus::Success(wheel_data) =>
//             wheel_data,
//         WheelOpenStatus::FileNotFound { .. } =>
//             panic!("file not found: {:?}", wheel_filename),
//     };
//     let gen_server = gen_server_init.finish::<LocalContext>();
//     let gen_server_pid = gen_server.pid();

//     gen_server.run(meister.clone(), thread_pool.clone(), blocks_pool.clone())
//         .unwrap();

//     let mut schema = performer.decompose();

//     let block_id = block::Id::init();

//     match schema.process_read_block_request(&block_id) {
//         schema::ReadBlockOp::CacheHit(schema::ReadBlockCacheHit { .. }) =>
//             panic!("unexpected cache hit for block_id = {block_id:?}"),
//         schema::ReadBlockOp::Perform(schema::ReadBlockPerform { .. }) =>
//             panic!("unexpected read perform for block_id = {block_id:?}"),
//         schema::ReadBlockOp::NotFound =>
//             (),
//     }
//     let block_id = block_id.next();
//     let expected_offset = schema.storage_layout().wheel_header_size as u64
//         + schema.storage_layout().data_size_block_min() as u64
//         + hello_world_bytes().len() as u64;

//     let (block_header, block_bytes) = match schema.process_read_block_request(&block_id) {
//         schema::ReadBlockOp::CacheHit(schema::ReadBlockCacheHit { .. }) =>
//             panic!("unexpected cache hit for block_id = {block_id:?}"),
//         schema::ReadBlockOp::Perform(schema::ReadBlockPerform { block_header, }) => {
//             let task = task::Task {
//                 block_id: block_header.block_id.clone(),
//                 kind: task::TaskKind::ReadBlock(task::ReadBlock {
//                     block_header: block_header.clone(),
//                     context: task::ReadBlockContext::Process(task::ReadBlockProcessContext::External(context)),
//                 })
//             };
//             gen_server_pid.push_request(expected_offset, task)
//                 .unwrap();
//             let order = orders_rx.recv().unwrap();
//             match order {
//                 performer_sklave::Order::TaskDoneStats(
//                     performer_sklave::OrderTaskDoneStats {
//                         task_done: task::Done {
//                             task: task::TaskDone {
//                                 block_id,
//                                 kind: task::TaskDoneKind::ReadBlock(task::TaskDoneReadBlock {
//                                     block_bytes,
//                                     context: task::ReadBlockContext::Process(task::ReadBlockProcessContext::External(ctx)),
//                                     ..
//                                 }),
//                             },
//                             ..
//                         },
//                         ..
//                     },
//                 ) if block_id == block_header.block_id && ctx == context =>
//                     (block_header.clone(), block_bytes),
//                 other_order =>
//                     panic!(
//                         "unexpected order received: {other_order:?}, expected task done read block {:?} with {context:?} context",
//                         block_header.block_id,
//                     ),
//             }
//         },
//         schema::ReadBlockOp::NotFound =>
//             panic!("unexpected read not found for block_id = {block_id:?}"),
//     };

//     let interpret::RunBlockProcessReadJobDone { block_bytes, .. } =
//         interpret::run_block_process_read_job(
//             schema.storage_layout().clone(),
//             block_header.clone(),
//             block_bytes.freeze(),
//         )
//         .unwrap();
//     assert_eq!(block_bytes, hello_world_bytes());

//     fs::remove_file(wheel_filename).unwrap();
// }

enum Order {
    PerformerSklave(performer_sklave::Order<LocalAccessPolicy>),
    Reply(OrderReply),
}

enum OrderReply {
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

struct ReplyInfo;
struct ReplyFlush;
struct ReplyWriteBlock;
struct ReplyReadBlock;
struct ReplyDeleteBlock;
struct ReplyIterBlocksInit;
struct ReplyIterBlocksNext;

impl From<komm::UmschlagAbbrechen<ReplyInfo>> for Order {
    fn from(v: komm::UmschlagAbbrechen<ReplyInfo>) -> Order {
        Order::Reply(OrderReply::InfoCancel(v))
    }
}

impl From<komm::Umschlag<Info, ReplyInfo>> for Order {
    fn from(v: komm::Umschlag<Info, ReplyInfo>) -> Order {
        Order::Reply(OrderReply::Info(v))
    }
}

impl From<komm::UmschlagAbbrechen<ReplyFlush>> for Order {
    fn from(v: komm::UmschlagAbbrechen<ReplyFlush>) -> Order {
        Order::Reply(OrderReply::FlushCancel(v))
    }
}

impl From<komm::Umschlag<Flushed, ReplyFlush>> for Order {
    fn from(v: komm::Umschlag<Flushed, ReplyFlush>) -> Order {
        Order::Reply(OrderReply::Flush(v))
    }
}

impl From<komm::UmschlagAbbrechen<ReplyWriteBlock>> for Order {
    fn from(v: komm::UmschlagAbbrechen<ReplyWriteBlock>) -> Order {
        Order::Reply(OrderReply::WriteBlockCancel(v))
    }
}

impl From<komm::Umschlag<Result<block::Id, RequestWriteBlockError>, ReplyWriteBlock>> for Order {
    fn from(v: komm::Umschlag<Result<block::Id, RequestWriteBlockError>, ReplyWriteBlock>) -> Order {
        Order::Reply(OrderReply::WriteBlock(v))
    }
}

impl From<komm::UmschlagAbbrechen<ReplyReadBlock>> for Order {
    fn from(v: komm::UmschlagAbbrechen<ReplyReadBlock>) -> Order {
        Order::Reply(OrderReply::ReadBlockCancel(v))
    }
}

impl From<komm::Umschlag<Result<Bytes, RequestReadBlockError>, ReplyReadBlock>> for Order {
    fn from(v: komm::Umschlag<Result<Bytes, RequestReadBlockError>, ReplyReadBlock>) -> Order {
        Order::Reply(OrderReply::ReadBlock(v))
    }
}

impl From<komm::UmschlagAbbrechen<ReplyDeleteBlock>> for Order {
    fn from(v: komm::UmschlagAbbrechen<ReplyDeleteBlock>) -> Order {
        Order::Reply(OrderReply::DeleteBlockCancel(v))
    }
}

impl From<komm::Umschlag<Result<Deleted, RequestDeleteBlockError>, ReplyDeleteBlock>> for Order {
    fn from(v: komm::Umschlag<Result<Deleted, RequestDeleteBlockError>, ReplyDeleteBlock>) -> Order {
        Order::Reply(OrderReply::DeleteBlock(v))
    }
}

impl From<komm::UmschlagAbbrechen<ReplyIterBlocksInit>> for Order {
    fn from(v: komm::UmschlagAbbrechen<ReplyIterBlocksInit>) -> Order {
        Order::Reply(OrderReply::IterBlocksInitCancel(v))
    }
}

impl From<komm::Umschlag<IterBlocks, ReplyIterBlocksInit>> for Order {
    fn from(v: komm::Umschlag<IterBlocks, ReplyIterBlocksInit>) -> Order {
        Order::Reply(OrderReply::IterBlocksInit(v))
    }
}

impl From<komm::UmschlagAbbrechen<ReplyIterBlocksNext>> for Order {
    fn from(v: komm::UmschlagAbbrechen<ReplyIterBlocksNext>) -> Order {
        Order::Reply(OrderReply::IterBlocksNextCancel(v))
    }
}

impl From<komm::Umschlag<IterBlocksItem, ReplyIterBlocksNext>> for Order {
    fn from(v: komm::Umschlag<IterBlocksItem, ReplyIterBlocksNext>) -> Order {
        Order::Reply(OrderReply::IterBlocksNext(v))
    }
}

struct Welt {
    orders_tx: mpsc::Sender<Order>,
}

enum Job {
    Sklave(arbeitssklave::SklaveJob<Welt, Order>),
}

impl From<arbeitssklave::SklaveJob<Welt, Order>> for Job {
    fn from(sklave_job: arbeitssklave::SklaveJob<Welt, Order>) -> Job {
        Job::Sklave(sklave_job)
    }
}

impl edeltraud::Job for Job {
    type Output = ();

    fn run<P>(self, _thread_pool: &P) -> Self::Output where P: edeltraud::ThreadPool<Self> {
        match self {
            Job::Sklave(arbeitssklave::SklaveJob { mut sklave, mut sklavenwelt, }) => {
                loop {
                    match sklave.zu_ihren_diensten(sklavenwelt).unwrap() {
                        arbeitssklave::Gehorsam::Machen { befehle, sklavenwelt: next_sklavenwelt, } => {
                            sklavenwelt = next_sklavenwelt;
                            for befehl in befehle {
                                sklavenwelt.orders_tx.send(befehl).unwrap();
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

fn hello_world_bytes() -> Bytes {
    let mut block_bytes_mut = BytesMut::new_detached(Vec::new());
    block_bytes_mut.extend("hello, world!".as_bytes().iter().cloned());
    block_bytes_mut.freeze()
}
