use std::{
    fs,
};

use futures::{
    channel::{
        oneshot,
    },
    select,
    pin_mut,
    Future,
    FutureExt,
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
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let wheel_filename = "/tmp/blockwheel_create_read_empty";
    runtime.block_on(async {
        let _wheel_data = GenServer::create(
            CreateParams {
                wheel_filename,
                init_wheel_size_bytes: 256 * 1024,
            },
            performer::PerformerBuilderInit::new(
                lru::Cache::new(0),
                None,
                64 * 1024,
            ).map_err(Error::PerformerBuild)?,
        ).map_err(Error::Create)?;
        let _wheel_open_status = GenServer::open(
            OpenParams {
                wheel_filename,
            },
            performer::PerformerBuilderInit::new(
                lru::Cache::new(0),
                None,
                64 * 1024,
            ).map_err(Error::PerformerBuild)?,
        ).map_err(Error::Open)?;
        Ok::<_, Error>(())
    }).unwrap();
    fs::remove_file(wheel_filename).unwrap();
}

#[test]
fn create_read_one() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let wheel_filename = "/tmp/blockwheel_create_read_one";
    let context = "ectx00";
    runtime.block_on(async {
        let WheelData { sync_gen_server: gen_server, performer, } = GenServer::create(
            CreateParams {
                wheel_filename,
                init_wheel_size_bytes: 256 * 1024,
            },
            performer::PerformerBuilderInit::new(
                lru::Cache::new(0),
                None,
                64 * 1024,
            ).map_err(Error::PerformerBuild)?,
        ).map_err(Error::Create)?;
        let schema = performer.decompose();
        with_gen_server(gen_server, |mut pid, blocks_pool| async move {
            let block_id = block::Id::init();
            let interpret::BlockPrepareWriteJobDone { write_block_bytes, } = interpret::block_prepare_write_job(
                interpret::BlockPrepareWriteJobArgs {
                    block_id: block_id.clone(),
                    block_bytes: hello_world_bytes(),
                    blocks_pool: blocks_pool.clone(),
                },
            ).map_err(Error::WriteBlockPrepare)?;
            let task_done = request_reply(
                &mut pid,
                schema.storage_layout().wheel_header_size as u64,
                block_id,
                task::TaskKind::WriteBlock(task::WriteBlock {
                    write_block_bytes,
                    commit: task::Commit::WithTerminator,
                    context: task::WriteBlockContext::External(context),
                }),
            ).await?;
            match task_done {
                task::Done {
                    task: task::TaskDone {
                        block_id,
                        kind: task::TaskDoneKind::WriteBlock(task::TaskDoneWriteBlock { context: task::WriteBlockContext::External(ctx), }),
                    },
                    ..
                } if block_id == block::Id::init() && ctx == context => {
                    let interpret::Synced = pid.device_sync().await
                        .map_err(Error::DeviceSync)?;
                    Ok(())
                },
                other_done_task =>
                    Err(Error::Unexpected(UnexpectedError::WriteDoneTask {
                        expected: format!("task done write block {:?} with {:?} context", block::Id::init(), context),
                        received: other_done_task,
                    })),
            }
        }).await?;
        let open_status = GenServer::open(
            OpenParams {
                wheel_filename,
            },
            performer::PerformerBuilderInit::new(
                lru::Cache::new(0),
                None,
                64 * 1024,
            ).map_err(Error::PerformerBuild)?,
        ).map_err(Error::Open)?;
        let WheelData { sync_gen_server: gen_server, performer, } = match open_status {
            WheelOpenStatus::Success(wheel_data) =>
                wheel_data,
            WheelOpenStatus::FileNotFound { .. } =>
                panic!("file not found: {:?}", wheel_filename),
        };
        let mut schema = performer.decompose();
        with_gen_server(gen_server, |mut pid, _blocks_pool| async move {
            let block_id = block::Id::init();
            let expected_offset = schema.storage_layout().wheel_header_size as u64;
            let (block_header, block_bytes) = match schema.process_read_block_request(&block_id) {
                schema::ReadBlockOp::Perform(schema::ReadBlockPerform { block_header, }) => {
                    let task_done = request_reply(
                        &mut pid,
                        expected_offset,
                        block_header.block_id.clone(),
                        task::TaskKind::ReadBlock(task::ReadBlock {
                            block_header: block_header.clone(),
                            context: task::ReadBlockContext::Process(task::ReadBlockProcessContext::External(context)),
                        }),
                    ).await?;
                    match task_done {
                        task::Done {
                            task: task::TaskDone {
                                block_id,
                                kind: task::TaskDoneKind::ReadBlock(task::TaskDoneReadBlock {
                                    block_bytes,
                                    context: task::ReadBlockContext::Process(task::ReadBlockProcessContext::External(ctx)),
                                    ..
                                }),
                            },
                            ..
                        } if block_id == block_header.block_id && ctx == context =>
                            Ok((block_header.clone(), block_bytes)),
                        other_done_task =>
                            Err(Error::Unexpected(UnexpectedError::ReadDoneTask {
                                expected: format!("task done read block {:?} with {:?} context", block_header.block_id, context),
                                received: other_done_task,
                            })),
                    }
                },
                schema::ReadBlockOp::NotFound =>
                    Err(Error::Unexpected(UnexpectedError::ReadNotFound { block_id, })),
            }?;
            let interpret::BlockProcessReadJobDone { block_bytes, .. } =
                interpret::block_process_read_job(interpret::BlockProcessReadJobArgs {
                    storage_layout: schema.storage_layout().clone(),
                    block_header: block_header.clone(),
                    block_bytes: block_bytes.freeze(),
                })
                .map_err(Error::ReadBlockProcess)?;
            assert_eq!(block_bytes, hello_world_bytes());
            Ok(())
        }).await?;
        Ok::<_, Error>(())
    }).unwrap();
    fs::remove_file(wheel_filename).unwrap();
}

#[test]
fn create_write_overlap_read_one() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let wheel_filename = "/tmp/create_write_overlap_read_one";
    let context = "ectx01";
    runtime.block_on(async {
        let WheelData { sync_gen_server: gen_server, performer, } = GenServer::create(
            CreateParams {
                wheel_filename,
                init_wheel_size_bytes: 256 * 1024,
            },
            performer::PerformerBuilderInit::new(
                lru::Cache::new(0),
                None,
                64 * 1024,
            ).map_err(Error::PerformerBuild)?,
        ).map_err(Error::Create)?;
        let schema = performer.decompose();
        with_gen_server(gen_server, |mut pid, blocks_pool| async move {

            // write first block
            let block_id = block::Id::init();
            let interpret::BlockPrepareWriteJobDone { write_block_bytes, } = interpret::block_prepare_write_job(
                interpret::BlockPrepareWriteJobArgs {
                    block_id: block_id.clone(),
                    block_bytes: hello_world_bytes(),
                    blocks_pool: blocks_pool.clone(),
                },
            ).map_err(Error::WriteBlockPrepare)?;
            let task::Done { task: task::TaskDone { .. }, .. } = request_reply(
                &mut pid,
                schema.storage_layout().wheel_header_size as u64,
                block_id,
                task::TaskKind::WriteBlock(task::WriteBlock {
                    write_block_bytes,
                    commit: task::Commit::WithTerminator,
                    context: task::WriteBlockContext::External(context),
                }),
            ).await?;

            // partially overwrite first block with second
            let block_id = block::Id::init().next();
            let interpret::BlockPrepareWriteJobDone { write_block_bytes, } = interpret::block_prepare_write_job(
                interpret::BlockPrepareWriteJobArgs {
                    block_id: block_id.clone(),
                    block_bytes: hello_world_bytes(),
                    blocks_pool: blocks_pool.clone(),
                },
            ).map_err(Error::WriteBlockPrepare)?;
            let task::Done { task: task::TaskDone { .. }, .. } = request_reply(
                &mut pid,
                schema.storage_layout().wheel_header_size as u64
                    + schema.storage_layout().block_header_size as u64,
                block_id,
                task::TaskKind::WriteBlock(task::WriteBlock {
                    write_block_bytes,
                    commit: task::Commit::WithTerminator,
                    context: task::WriteBlockContext::External(context),
                }),
            ).await?;

            Ok(())
        }).await?;

        let open_status = GenServer::open(
            OpenParams {
                wheel_filename,
            },
            performer::PerformerBuilderInit::new(
                lru::Cache::new(0),
                None,
                64 * 1024,
            ).map_err(Error::PerformerBuild)?,
        ).map_err(Error::Open)?;
        let WheelData { sync_gen_server: gen_server, performer, } = match open_status {
            WheelOpenStatus::Success(wheel_data) =>
                wheel_data,
            WheelOpenStatus::FileNotFound { .. } =>
                panic!("file not found: {:?}", wheel_filename),
        };
        let mut schema = performer.decompose();
        with_gen_server(gen_server, |mut pid, _blocks_pool| async move {
            let block_id = block::Id::init();
            match schema.process_read_block_request(&block_id) {
                schema::ReadBlockOp::Perform(schema::ReadBlockPerform { .. }) =>
                    return Err(Error::Unexpected(UnexpectedError::ReadPerform { block_id, })),
                schema::ReadBlockOp::NotFound =>
                    (),
            }
            let block_id = block_id.next();
            let expected_offset = schema.storage_layout().wheel_header_size as u64
                + schema.storage_layout().block_header_size as u64;
            let (block_header, block_bytes) = match schema.process_read_block_request(&block_id) {
                schema::ReadBlockOp::Perform(schema::ReadBlockPerform { block_header, }) => {
                    let task_done = request_reply(
                        &mut pid,
                        expected_offset,
                        block_header.block_id.clone(),
                        task::TaskKind::ReadBlock(task::ReadBlock {
                            block_header: block_header.clone(),
                            context: task::ReadBlockContext::Process(task::ReadBlockProcessContext::External(context)),
                        }),
                    ).await?;
                    match task_done {
                        task::Done {
                            task: task::TaskDone {
                                block_id,
                                kind: task::TaskDoneKind::ReadBlock(task::TaskDoneReadBlock {
                                    block_bytes,
                                    context: task::ReadBlockContext::Process(task::ReadBlockProcessContext::External(ctx)),
                                    ..
                                }),
                            },
                            ..
                        } if block_id == block_header.block_id && ctx == context =>
                            Ok((block_header.clone(), block_bytes)),
                        other_done_task =>
                            Err(Error::Unexpected(UnexpectedError::ReadDoneTask {
                                expected: format!("task done read block {:?} with {:?} context", block_header.block_id, context),
                                received: other_done_task,
                            })),
                    }
                },
                schema::ReadBlockOp::NotFound =>
                    Err(Error::Unexpected(UnexpectedError::ReadNotFound { block_id, })),
            }?;
            let interpret::BlockProcessReadJobDone { block_bytes, .. } =
                interpret::block_process_read_job(interpret::BlockProcessReadJobArgs {
                    storage_layout: schema.storage_layout().clone(),
                    block_header: block_header.clone(),
                    block_bytes: block_bytes.freeze(),
                })
                .map_err(Error::ReadBlockProcess)?;
            assert_eq!(block_bytes, hello_world_bytes());
            Ok(())
        }).await?;
        Ok::<_, Error>(())
    }).unwrap();
    fs::remove_file(wheel_filename).unwrap();
}

#[test]
fn create_write_delete_read_one() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let wheel_filename = "/tmp/create_write_delete_read_one";
    let context = "ectx02";
    runtime.block_on(async {
        let WheelData { sync_gen_server: gen_server, performer, } = GenServer::create(
            CreateParams {
                wheel_filename,
                init_wheel_size_bytes: 256 * 1024,
            },
            performer::PerformerBuilderInit::new(
                lru::Cache::new(0),
                None,
                64 * 1024,
            ).map_err(Error::PerformerBuild)?,
        ).map_err(Error::Create)?;
        let schema = performer.decompose();
        with_gen_server(gen_server, |mut pid, blocks_pool| async move {

            // write first block
            let block_id = block::Id::init();
            let interpret::BlockPrepareWriteJobDone { write_block_bytes, } = interpret::block_prepare_write_job(
                interpret::BlockPrepareWriteJobArgs {
                    block_id: block_id.clone(),
                    block_bytes: hello_world_bytes(),
                    blocks_pool: blocks_pool.clone(),
                },
            ).map_err(Error::WriteBlockPrepare)?;
            let task::Done { current_offset, task: task::TaskDone { .. }, } = request_reply(
                &mut pid,
                schema.storage_layout().wheel_header_size as u64,
                block_id.clone(),
                task::TaskKind::WriteBlock(task::WriteBlock {
                    write_block_bytes,
                    commit: task::Commit::WithTerminator,
                    context: task::WriteBlockContext::External(context),
                }),
            ).await?;

            // write second block
            let block_id = block_id.next();
            let interpret::BlockPrepareWriteJobDone { write_block_bytes, } = interpret::block_prepare_write_job(
                interpret::BlockPrepareWriteJobArgs {
                    block_id: block_id.clone(),
                    block_bytes: hello_world_bytes(),
                    blocks_pool: blocks_pool.clone(),
                },
            ).map_err(Error::WriteBlockPrepare)?;
            let task::Done { task: task::TaskDone { .. }, .. } = request_reply(
                &mut pid,
                current_offset,
                block_id,
                task::TaskKind::WriteBlock(task::WriteBlock {
                    write_block_bytes,
                    commit: task::Commit::WithTerminator,
                    context: task::WriteBlockContext::External(context),
                }),
            ).await?;

            // delete first block
            let interpret::BlockPrepareDeleteJobDone { delete_block_bytes, } = interpret::block_prepare_delete_job(
                interpret::BlockPrepareDeleteJobArgs {
                    blocks_pool: blocks_pool.clone(),
                },
            ).map_err(Error::DeleteBlockPrepare)?;
            let task_done = request_reply(
                &mut pid,
                schema.storage_layout().wheel_header_size as u64,
                block::Id::init(),
                task::TaskKind::DeleteBlock(task::DeleteBlock {
                    delete_block_bytes: delete_block_bytes.freeze(),
                    commit: task::Commit::None,
                    context: task::DeleteBlockContext::External(context),
                }),
            ).await?;

            match task_done {
                task::Done {
                    task: task::TaskDone {
                        block_id,
                        kind: task::TaskDoneKind::DeleteBlock(task::TaskDoneDeleteBlock { context: task::DeleteBlockContext::External(ctx), }),
                    },
                    ..
                } if block_id == block::Id::init() && ctx == context =>
                    Ok(()),
                other_done_task =>
                    Err(Error::Unexpected(UnexpectedError::DeleteDoneTask {
                        expected: format!("task done delete block {:?} with {:?} context", block::Id::init(), context),
                        received: other_done_task,
                    })),
            }
        }).await?;

        let open_status = GenServer::open(
            OpenParams {
                wheel_filename,
            },
            performer::PerformerBuilderInit::new(
                lru::Cache::new(0),
                None,
                64 * 1024,
            ).map_err(Error::PerformerBuild)?,
        ).map_err(Error::Open)?;
        let WheelData { sync_gen_server: gen_server, performer, } = match open_status {
            WheelOpenStatus::Success(wheel_data) =>
                wheel_data,
            WheelOpenStatus::FileNotFound { .. } =>
                panic!("file not found: {:?}", wheel_filename),
        };
        let mut schema = performer.decompose();
        with_gen_server(gen_server, |mut pid, _blocks_pool| async move {
            let block_id = block::Id::init();
            match schema.process_read_block_request(&block_id) {
                schema::ReadBlockOp::Perform(schema::ReadBlockPerform { .. }) =>
                    return Err(Error::Unexpected(UnexpectedError::ReadPerform { block_id, })),
                schema::ReadBlockOp::NotFound =>
                    (),
            }
            let block_id = block_id.next();
            let expected_offset = schema.storage_layout().wheel_header_size as u64
                + schema.storage_layout().data_size_block_min() as u64
                + hello_world_bytes().len() as u64;
            let (block_header, block_bytes) = match schema.process_read_block_request(&block_id) {
                schema::ReadBlockOp::Perform(schema::ReadBlockPerform { block_header, }) => {
                    let task_done = request_reply(
                        &mut pid,
                        expected_offset,
                        block_header.block_id.clone(),
                        task::TaskKind::ReadBlock(task::ReadBlock {
                            block_header: block_header.clone(),
                            context: task::ReadBlockContext::Process(task::ReadBlockProcessContext::External(context)),
                        }),
                    ).await?;
                    match task_done {
                        task::Done {
                            task: task::TaskDone {
                                block_id,
                                kind: task::TaskDoneKind::ReadBlock(task::TaskDoneReadBlock {
                                    block_bytes,
                                    context: task::ReadBlockContext::Process(task::ReadBlockProcessContext::External(ctx)),
                                    ..
                                }),
                            },
                            ..
                        } if block_id == block_header.block_id && ctx == context =>
                            Ok((block_header.clone(), block_bytes)),
                        other_done_task =>
                            Err(Error::Unexpected(UnexpectedError::ReadDoneTask {
                                expected: format!("task done read block {:?} with {:?} context", block_header.block_id, context),
                                received: other_done_task,
                            })),
                    }
                },
                schema::ReadBlockOp::NotFound =>
                    Err(Error::Unexpected(UnexpectedError::ReadNotFound { block_id, })),
            }?;
            let interpret::BlockProcessReadJobDone { block_bytes, .. } =
                interpret::block_process_read_job(interpret::BlockProcessReadJobArgs {
                    storage_layout: schema.storage_layout().clone(),
                    block_header: block_header.clone(),
                    block_bytes: block_bytes.freeze(),
                })
                .map_err(Error::ReadBlockProcess)?;
            assert_eq!(block_bytes, hello_world_bytes());
            Ok(())
        }).await?;
        Ok::<_, Error>(())
    }).unwrap();
    fs::remove_file(wheel_filename).unwrap();
}

#[derive(Debug)]
enum Error {
    PerformerBuild(performer::BuilderError),
    Create(super::WheelCreateError),
    Open(super::WheelOpenError),
    Run(super::Error),
    DeviceSync(ero::NoProcError),
    InterpreterDetach,
    Unexpected(UnexpectedError),
    WriteBlockPrepare(interpret::BlockPrepareWriteJobError),
    ReadBlockProcess(interpret::BlockProcessReadJobError),
    DeleteBlockPrepare(interpret::BlockPrepareDeleteJobError),
}

#[derive(Debug)]
pub enum UnexpectedError {
    WriteDoneTask {
        expected: String,
        received: task::Done<LocalContext>,
    },
    ReadDoneTask {
        expected: String,
        received: task::Done<LocalContext>,
    },
    DeleteDoneTask {
        expected: String,
        received: task::Done<LocalContext>,
    },
    ReadPerform {
        block_id: block::Id,
    },
    ReadNotFound {
        block_id: block::Id,
    },
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
    type Interpreter = C;
}

type GenServer = super::SyncGenServer<LocalContext>;
type Pid = super::Pid<LocalContext>;

async fn with_gen_server<F, FF>(
    gen_server: GenServer,
    body: F,
)
    -> Result<(), Error>
where F: FnOnce(Pid, BytesPool) -> FF,
      FF: Future<Output = Result<(), Error>>,
{
    let pid = gen_server.pid();
    let blocks_pool = BytesPool::new();

    let (error_tx, mut error_rx) = oneshot::channel();
    gen_server.run(blocks_pool.clone(), error_tx, std::convert::identity)
        .map_err(Error::Run)?;

    let body_task = body(pid, blocks_pool);
    let body_task_fused = body_task.fuse();
    pin_mut!(body_task_fused);

    loop {
        select! {
            result = body_task_fused =>
                return result,
            result = &mut error_rx =>
                match result {
                    Err(oneshot::Canceled) =>
                        (),
                    Ok(error) =>
                        return Err(Error::Run(error)),
                },
        }
    }
}

async fn request_reply(
    pid: &mut Pid,
    offset: u64,
    block_id: block::Id,
    kind: task::TaskKind<LocalContext>,
)
    -> Result<task::Done<LocalContext>, Error>
{
    let reply_rx = pid.push_request(offset, task::Task { block_id, kind, })
        .map_err(|ero::NoProcError| Error::InterpreterDetach)?;
    let super::DoneTask { task_done, .. } = reply_rx.await.map_err(|_| Error::InterpreterDetach)?;
    Ok(task_done)
}

fn hello_world_bytes() -> Bytes {
    let mut block_bytes_mut = BytesMut::new_detached(Vec::new());
    block_bytes_mut.extend("hello, world!".as_bytes().iter().cloned());
    block_bytes_mut.freeze()
}
