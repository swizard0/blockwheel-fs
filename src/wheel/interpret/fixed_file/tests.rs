use std::fs;

use futures::{
    select,
    pin_mut,
    Future,
    FutureExt,
};

use tokio::runtime;

use crate::{
    block,
    context::Context,
    wheel::core::{
        task,
        schema,
    },
};

use super::{
    OpenParams,
    CreateParams,
};

#[test]
fn create_read_empty() {
    let mut runtime = runtime::Builder::new()
        .basic_scheduler()
        .build()
        .unwrap();
    let wheel_filename = "/tmp/blockwheel_create_read_empty";
    runtime.block_on(async {
        let _gen_server = GenServer::create(CreateParams {
            wheel_filename,
            work_block_size_bytes: 64 * 1024,
            init_wheel_size_bytes: 256 * 1024,
        }).await.map_err(Error::Create)?;
        let _gen_server = GenServer::open(OpenParams {
            wheel_filename,
            work_block_size_bytes: 64 * 1024,
        }).await.map_err(Error::Open)?;
        Ok::<_, Error>(())
    }).unwrap();
    fs::remove_file(wheel_filename).unwrap();
}

#[test]
fn create_read_one() {
    let mut runtime = runtime::Builder::new()
        .basic_scheduler()
        .build()
        .unwrap();
    let wheel_filename = "/tmp/blockwheel_create_read_one";
    let context = "ectx00";
    runtime.block_on(async {
        let gen_server = GenServer::create(CreateParams {
            wheel_filename,
            work_block_size_bytes: 64 * 1024,
            init_wheel_size_bytes: 256 * 1024,
        }).await.map_err(Error::Create)?;
        with_gen_server(gen_server, |mut pid, _schema| async move {
            let reply_rx = pid.push_request(0, task::Task {
                block_id: block::Id::init(),
                kind: task::TaskKind::WriteBlock(
                    task::WriteBlock {
                        block_bytes: hello_world_bytes(),
                        context: task::WriteBlockContext::External(context),
                    },
                ),
            }).await.map_err(|ero::NoProcError| Error::InterpreterDetach)?;
            match reply_rx.await {
                Ok(task::Done {
                    task: task::TaskDone {
                        block_id,
                        kind: task::TaskDoneKind::WriteBlock(task::TaskDoneWriteBlock { context: task::WriteBlockContext::External(ctx), }),
                    },
                    ..
                }) if block_id == block::Id::init() && ctx == context =>
                    Ok(()),
                Ok(other_done_task) =>
                    Err(Error::Unexpected(UnexpectedError::DoneTask {
                        expected: format!("task done write block {:?} with {:?} context", block::Id::init(), context),
                        received: other_done_task,
                    })),
                Err(..) =>
                    Err(Error::InterpreterDetach),
            }
        }).await?;
        let gen_server = GenServer::open(OpenParams {
            wheel_filename,
            work_block_size_bytes: 64 * 1024,
        }).await.map_err(Error::Open)?;
        with_gen_server(gen_server, |_pid, mut schema| async move {
            match schema.process_read_block_request(&block::Id::init()) {
                schema::ReadBlockOp::Perform(schema::ReadBlockPerform { block_offset, block_header, .. }) =>
                    unimplemented!(),
                schema::ReadBlockOp::Cached { .. } =>
                    unimplemented!(),
                schema::ReadBlockOp::NotFound =>
                    unimplemented!(),
            }
        }).await?;
        Ok::<_, Error>(())
    }).unwrap();
    fs::remove_file(wheel_filename).unwrap();
}

#[derive(Debug)]
enum Error {
    Create(super::WheelCreateError),
    Open(super::WheelOpenError),
    Run(super::Error),
    InterpreterDetach,
    Unexpected(UnexpectedError),
}

#[derive(Debug)]
enum UnexpectedError {
    DoneTask {
        expected: String,
        received: task::Done<LocalContext>,
    },
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
struct LocalContext;

type C = &'static str;

impl Context for LocalContext {
    type LendBlock = C;
    type WriteBlock = C;
    type ReadBlock = C;
    type DeleteBlock = C;
    type Interpreter = C;
}

type GenServer = super::GenServer<LocalContext>;
type Pid = super::Pid<LocalContext>;

async fn with_gen_server<F, FF>(
    gen_server: GenServer,
    body: F,
)
    -> Result<(), Error>
where F: FnOnce(Pid, schema::Schema) -> FF,
      FF: Future<Output = Result<(), Error>>,
{
    let pid = gen_server.pid();
    let (schema, interpreter_run) = gen_server.run();
    let (interpreter_task, interpreter_handle) = interpreter_run.remote_handle();
    let interpreter_handle_fused = interpreter_handle.fuse();
    pin_mut!(interpreter_handle_fused);
    tokio::spawn(interpreter_task);
    let body_task = body(pid, schema);
    let body_task_fused = body_task.fuse();
    pin_mut!(body_task_fused);

    loop {
        select! {
            result = body_task_fused =>
                return result,
            result = interpreter_handle_fused =>
                match result {
                    Ok(()) =>
                        (),
                    Err(error) =>
                        return Err(Error::Run(error)),
                },
        }
    }
}

fn hello_world_bytes() -> block::Bytes {
    let mut block_bytes_mut = block::BytesMut::new_detached();
    block_bytes_mut.extend("hello, world!".as_bytes().iter().cloned());
    block_bytes_mut.freeze()
}
