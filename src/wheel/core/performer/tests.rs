use super::{
    lru,
    task,
    block,
    proto,
    schema,
    Op,
    TaskDone,
    PerformOp,
    Performer,
    PollRequest,
    LendBlockOp,
    ReadBlockOp,
    WriteBlockOp,
    DeleteBlockOp,
    ReadBlockDoneOp,
    WriteBlockDoneOp,
    DeleteBlockDoneOp,
    TaskReadBlockDoneLoop,
    TaskDeleteBlockDoneLoop,
    InterpretTask,
    PollRequestAndInterpreter,
    super::{
        storage,
        context,
    },
};

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
struct Context;

type C = &'static str;

impl context::Context for Context {
    type LendBlock = C;
    type WriteBlock = C;
    type ReadBlock = C;
    type DeleteBlock = C;
    type Interpreter = C;
}

fn init() -> Performer<Context> {
    let storage_layout = storage::Layout {
        wheel_header_size: 24,
        block_header_size: 24,
        commit_tag_size: 16,
        eof_tag_size: 8,
    };
    let mut schema = schema::Schema::new(storage_layout);
    schema.initialize_empty(144);
    Performer::new(
        schema,
        lru::Cache::new(16),
        None,
    )
}

fn hello_world_bytes() -> block::Bytes {
    let mut block_bytes_mut = block::BytesMut::new_detached();
    block_bytes_mut.extend("hello, world!".as_bytes().iter().cloned());
    block_bytes_mut.freeze()
}

#[derive(Debug)]
enum ScriptOp {
    PerformerNext,
    ExpectPollRequest,
    ExpectPollRequestAndInterpreter { expect_context: C, },
    ExpectInterpretTask { expect_offset: u64, expect_task: ExpectTask, },
    RequestAndInterpreterIncomingRequest { request: proto::Request<Context>, interpreter_context: C, },
    RequestAndInterpreterIncomingTaskDone { task_done: task::Done<Context>, },
    RequestIncomingRequest { request: proto::Request<Context>, },
    TaskAccepted { interpreter_context: C, },
    ExpectLendBlockSuccess { expect_context: C, },
    ExpectWriteBlockNoSpaceLeft { expect_context: C, },
    ExpectReadBlockCacheHit { expect_block_bytes: block::Bytes, expect_context: C, },
    ExpectReadBlockNotFound { expect_context: C, },
    ExpectDeleteBlockNotFound { expect_context: C, },
    ExpectWriteBlockDoneDone { expect_block_id: block::Id, expect_context: C, },
    ExpectReadBlockDoneDone { expect_block_bytes: block::Bytes, expect_context: C, },
    ExpectDeleteBlockDoneDone { expect_context: C, },
}

#[derive(Debug)]
struct ExpectTask {
    block_id: block::Id,
    kind: ExpectTaskKind,
}

#[derive(Debug)]
enum ExpectTaskKind {
    WriteBlock(ExpectTaskWriteBlock),
    ReadBlock(ExpectTaskReadBlock),
    DeleteBlock(ExpectTaskDeleteBlock),
}

#[derive(Debug)]
struct ExpectTaskWriteBlock {
    block_bytes: block::Bytes,
    commit_type: task::CommitType,
    context: task::WriteBlockContext<C>,
}

#[derive(Debug)]
struct ExpectTaskReadBlock {
    block_header: storage::BlockHeader,
    context: task::ReadBlockContext<C>,
}

#[derive(Debug)]
struct ExpectTaskDeleteBlock {
    context: task::DeleteBlockContext<C>,
}

impl PartialEq<task::Task<Context>> for ExpectTask {
    fn eq(&self, task: &task::Task<Context>) -> bool {
        self.block_id == task.block_id && self.kind == task.kind
    }
}

impl PartialEq<task::TaskKind<Context>> for ExpectTaskKind {
    fn eq(&self, task: &task::TaskKind<Context>) -> bool {
        match (self, task) {
            (ExpectTaskKind::WriteBlock(a), task::TaskKind::WriteBlock(b)) =>
                a == b,
            (ExpectTaskKind::ReadBlock(a), task::TaskKind::ReadBlock(b)) =>
                a == b,
            (ExpectTaskKind::DeleteBlock(a), task::TaskKind::DeleteBlock(b)) =>
                a == b,
            _ =>
                false,
        }
    }
}

impl PartialEq<task::WriteBlock<C>> for ExpectTaskWriteBlock {
    fn eq(&self, task: &task::WriteBlock<C>) -> bool {
        self.block_bytes == task.block_bytes
            && self.commit_type == task.commit_type
            && self.context == task.context
    }
}

impl PartialEq<task::ReadBlock<C>> for ExpectTaskReadBlock {
    fn eq(&self, task: &task::ReadBlock<C>) -> bool {
        self.block_header == task.block_header
            && self.context == task.context
    }
}

impl PartialEq<task::DeleteBlock<C>> for ExpectTaskDeleteBlock {
    fn eq(&self, task: &task::DeleteBlock<C>) -> bool {
        self.context == task.context
    }
}

fn interpret(mut performer: Performer<Context>, mut script: Vec<ScriptOp>) {
    let script_len = script.len();
    script.reverse();
    loop {
        match script.pop() {
            None =>
                break,
            Some(ScriptOp::PerformerNext) =>
                (),
            Some(other_op) =>
                panic!("expected Script::PerformerNext but got {:?} @ {}", other_op, script_len - script.len()),
        }

        let op = performer.next();
        match &op {
            Op::PollRequestAndInterpreter(PollRequestAndInterpreter { interpreter_context, .. }) =>
                match script.pop() {
                    None =>
                        panic!("unexpected script end on {:?}, expecting ExpectPollRequestAndInterpreter @ {}", op, script_len - script.len()),
                    Some(ScriptOp::ExpectPollRequestAndInterpreter { ref expect_context, }) if expect_context == interpreter_context =>
                        (),
                    Some(other_op) =>
                        panic!("expecting exact ExpectPollRequestAndInterpreter for {:?} but got {:?} @ {}", op, other_op, script_len - script.len()),
                },
            Op::PollRequest(PollRequest { .. }) =>
                match script.pop() {
                    None =>
                        panic!("unexpected script end on {:?}, expecting ExpectPollRequest @ {}", op, script_len - script.len()),
                    Some(ScriptOp::ExpectPollRequest) =>
                        (),
                    Some(other_op) =>
                        panic!("expecting exact ExpectPollRequest for {:?} but got {:?} @ {}", op, other_op, script_len - script.len()),
                },
            Op::InterpretTask(InterpretTask { offset, task, .. }) =>
                match script.pop() {
                    None =>
                        panic!("unexpected script end on {:?}, expecting ExpectInterpretTask @ {}", op, script_len - script.len()),
                    Some(ScriptOp::ExpectInterpretTask { ref expect_offset, ref expect_task, })
                        if expect_offset == offset && expect_task == task
                        => (),
                    Some(other_op) =>
                        panic!("expecting exact ExpectInterpretTask for {:?} but got {:?} @ {}", op, other_op, script_len - script.len()),
                },
        };

        let perform_op = match op {
            Op::PollRequestAndInterpreter(var @ PollRequestAndInterpreter { .. }) =>
                match script.pop() {
                    None =>
                        panic!("unexpected script end on {:?}, expecting one of RequestAndInterpreterIncoming* @ {}", var, script_len - script.len()),
                    Some(ScriptOp::RequestAndInterpreterIncomingRequest { request, interpreter_context, }) =>
                        var.next.incoming_request(request, interpreter_context),
                    Some(ScriptOp::RequestAndInterpreterIncomingTaskDone { task_done, }) =>
                        var.next.incoming_task_done(task_done),
                    Some(other_op) =>
                        panic!("expecting exact RequestAndInterpreterIncoming* for {:?} but got {:?} @ {}", var, other_op, script_len - script.len()),
                },
            Op::PollRequest(var @ PollRequest { .. }) =>
                match script.pop() {
                    None =>
                        panic!("unexpected script end on {:?}, expecting RequestIncomingRequest @ {}", var, script_len - script.len()),
                    Some(ScriptOp::RequestIncomingRequest { request, }) =>
                        var.next.incoming_request(request),
                    Some(other_op) =>
                        panic!("expecting exact RequestIncomingRequest for {:?} but got {:?} @ {}", var, other_op, script_len - script.len()),
                },
            Op::InterpretTask(var @ InterpretTask { .. }) =>
                match script.pop() {
                    None =>
                        panic!("unexpected script end on {:?}, expecting TaskAccepted @ {}", var, script_len - script.len()),
                    Some(ScriptOp::TaskAccepted { interpreter_context, }) => {
                        performer = var.next.task_accepted(interpreter_context);
                        continue;
                    },
                    Some(other_op) =>
                        panic!("expecting exact TaskAccepted for {:?} but got {:?} @ {}", var, other_op, script_len - script.len()),
                },
        };

        performer = match perform_op {
            PerformOp::Idle(performer) =>
                performer,

            PerformOp::LendBlock(LendBlockOp::Success { block_bytes, context, performer, }) =>
                match script.pop() {
                    None => {
                        let perform_op = PerformOp::LendBlock(LendBlockOp::Success { block_bytes, context, performer, });
                        panic!("unexpected script end on {:?}, expecting ExpectLendBlockSuccess @ {}", perform_op, script_len - script.len());
                    },
                    Some(ScriptOp::ExpectLendBlockSuccess { expect_context, }) if expect_context == context =>
                        performer,
                    Some(other_op) => {
                        let perform_op = PerformOp::LendBlock(LendBlockOp::Success { block_bytes, context, performer, });
                        panic!("expecting exact ExpectLendBlockSuccess for {:?} but got {:?} @ {}", perform_op, other_op, script_len - script.len());
                    },
                },

            PerformOp::WriteBlock(WriteBlockOp::NoSpaceLeft { context, performer, }) =>
                match script.pop() {
                    None => {
                        let perform_op = PerformOp::WriteBlock(WriteBlockOp::NoSpaceLeft { context, performer, });
                        panic!("unexpected script end on {:?}, expecting ExpectWriteBlockNoSpaceLeft @ {}", perform_op, script_len - script.len());
                    },
                    Some(ScriptOp::ExpectWriteBlockNoSpaceLeft { expect_context, }) if expect_context == context =>
                        performer,
                    Some(other_op) => {
                        let perform_op = PerformOp::WriteBlock(WriteBlockOp::NoSpaceLeft { context, performer, });
                        panic!("expecting exact ExpectWriteBlockNoSpaceLeft for {:?} but got {:?} @ {}", perform_op, other_op, script_len - script.len());
                    },
                },

            PerformOp::ReadBlock(ReadBlockOp::CacheHit { context, block_bytes, performer, }) =>
                match script.pop() {
                    None => {
                        let perform_op = PerformOp::ReadBlock(ReadBlockOp::CacheHit { context, block_bytes, performer, });
                        panic!("unexpected script end on {:?}, expecting ExpectReadBlockCacheHit @ {}", perform_op, script_len - script.len());
                    },
                    Some(ScriptOp::ExpectReadBlockCacheHit { expect_block_bytes, expect_context, })
                        if expect_context == context && expect_block_bytes == block_bytes =>
                        performer,
                    Some(other_op) => {
                        let perform_op = PerformOp::ReadBlock(ReadBlockOp::CacheHit { context, block_bytes, performer, });
                        panic!("expecting exact ExpectReadBlockCacheHit for {:?} but got {:?} @ {}", perform_op, other_op, script_len - script.len());
                    },
                },

            PerformOp::ReadBlock(ReadBlockOp::NotFound { context, performer }) =>
                match script.pop() {
                    None => {
                        let perform_op = PerformOp::ReadBlock(ReadBlockOp::NotFound { context, performer });
                        panic!("unexpected script end on {:?}, expecting ExpectReadBlockNotFound @ {}", perform_op, script_len - script.len());
                    },
                    Some(ScriptOp::ExpectReadBlockNotFound { expect_context, }) if expect_context == context =>
                        performer,
                    Some(other_op) => {
                        let perform_op = PerformOp::ReadBlock(ReadBlockOp::NotFound { context, performer });
                        panic!("expecting exact ExpectReadBlockNotFound for {:?} but got {:?} @ {}", perform_op, other_op, script_len - script.len());
                    },
                },

            PerformOp::DeleteBlock(DeleteBlockOp::NotFound { context, performer }) =>
                match script.pop() {
                    None => {
                        let perform_op = PerformOp::DeleteBlock(DeleteBlockOp::NotFound { context, performer });
                        panic!("unexpected script end on {:?}, expecting ExpectDeleteBlockNotFound @ {}", perform_op, script_len - script.len());
                    },
                    Some(ScriptOp::ExpectDeleteBlockNotFound { expect_context, }) if expect_context == context =>
                        performer,
                    Some(other_op) => {
                        let perform_op = PerformOp::DeleteBlock(DeleteBlockOp::NotFound { context, performer });
                        panic!("expecting exact ExpectDeleteBlockNotFound for {:?} but got {:?} @ {}", perform_op, other_op, script_len - script.len());
                    },
                },

            PerformOp::TaskDone(TaskDone::WriteBlockDone(WriteBlockDoneOp { block_id, context, performer, })) =>
                match script.pop() {
                    None => {
                        let perform_op = PerformOp::TaskDone(TaskDone::WriteBlockDone(WriteBlockDoneOp { block_id, context, performer, }));
                        panic!("unexpected script end on {:?}, expecting ExpectWriteBlockDoneDone @ {}", perform_op, script_len - script.len());
                    },
                    Some(ScriptOp::ExpectWriteBlockDoneDone { expect_block_id, expect_context, })
                        if expect_context == context && expect_block_id == block_id =>
                        performer,
                    Some(other_op) => {
                        let perform_op = PerformOp::TaskDone(TaskDone::WriteBlockDone(WriteBlockDoneOp { block_id, context, performer, }));
                        panic!("expecting exact ExpectWriteBlockDoneDone for {:?} but got {:?} @ {}", perform_op, other_op, script_len - script.len());
                    },
                },

            PerformOp::TaskDone(TaskDone::ReadBlockDone(mut done_op @ ReadBlockDoneOp { .. })) =>
                loop {
                    match script.pop() {
                        None => {
                            let perform_op = PerformOp::TaskDone(TaskDone::ReadBlockDone(done_op));
                            panic!("unexpected script end on {:?}, expecting ExpectReadBlockDoneDone @ {}", perform_op, script_len - script.len());
                        },
                        Some(ScriptOp::ExpectReadBlockDoneDone { expect_block_bytes, expect_context, })
                            if expect_context == done_op.context && expect_block_bytes == done_op.block_bytes =>
                            (),
                        Some(other_op) => {
                            let perform_op = PerformOp::TaskDone(TaskDone::ReadBlockDone(done_op));
                            panic!("expecting exact ExpectReadBlockDoneDone for {:?} but got {:?} @ {}", perform_op, other_op, script_len - script.len());
                        },
                    }
                    match done_op.next.step() {
                        TaskReadBlockDoneLoop::Done(performer) =>
                            break performer,
                        TaskReadBlockDoneLoop::More(done_op_more) =>
                            done_op = done_op_more,
                    }
                },

            PerformOp::TaskDone(TaskDone::DeleteBlockDone(mut done_op @ DeleteBlockDoneOp { .. })) =>
                loop {
                    match script.pop() {
                        None => {
                            let perform_op = PerformOp::TaskDone(TaskDone::DeleteBlockDone(done_op));
                            panic!("unexpected script end on {:?}, expecting ExpectDeleteBlockDoneDone @ {}", perform_op, script_len - script.len());
                        },
                        Some(ScriptOp::ExpectDeleteBlockDoneDone { expect_context, }) if expect_context == done_op.context =>
                            (),
                        Some(other_op) => {
                            let perform_op = PerformOp::TaskDone(TaskDone::DeleteBlockDone(done_op));
                            panic!(
                                "expecting exact ExpectDeleteBlockDoneDone for {:?} but got {:?} @ {}",
                                perform_op,
                                other_op,
                                script_len - script.len(),
                            );
                        },
                    }
                    match done_op.next.step() {
                        TaskDeleteBlockDoneLoop::Done(performer) =>
                            break performer,
                        TaskDeleteBlockDoneLoop::More(done_op_more) =>
                            done_op = done_op_more,
                    }
                },
        };
    }
}

#[test]
fn script_basic() {
    let performer = init();
    let script = vec![
        ScriptOp::PerformerNext,
        ScriptOp::ExpectPollRequest,
        ScriptOp::RequestIncomingRequest {
            request: proto::Request::LendBlock(proto::RequestLendBlock { context: "ctx00", }),
        },
        ScriptOp::ExpectLendBlockSuccess {
            expect_context: "ctx00",
        },
        ScriptOp::PerformerNext,
        ScriptOp::ExpectPollRequest,
        ScriptOp::RequestIncomingRequest {
            request: proto::Request::RepayBlock(proto::RequestRepayBlock { block_bytes: hello_world_bytes(), }),
        },
        ScriptOp::PerformerNext,
        ScriptOp::ExpectPollRequest,
        ScriptOp::RequestIncomingRequest {
            request: proto::Request::ReadBlock(proto::RequestReadBlock { block_id: block::Id::init(), context: "ctx01", }),
        },
        ScriptOp::ExpectReadBlockNotFound {
            expect_context: "ctx01",
        },
        ScriptOp::PerformerNext,
        ScriptOp::ExpectPollRequest,
        ScriptOp::RequestIncomingRequest {
            request: proto::Request::WriteBlock(proto::RequestWriteBlock { block_bytes: hello_world_bytes(), context: "ctx02", }),
        },
        ScriptOp::PerformerNext,
        ScriptOp::ExpectInterpretTask {
            expect_offset: 24,
            expect_task: ExpectTask {
                block_id: block::Id::init(),
                kind: ExpectTaskKind::WriteBlock(ExpectTaskWriteBlock {
                    block_bytes: hello_world_bytes(),
                    commit_type: task::CommitType::CommitAndEof,
                    context: task::WriteBlockContext::External("ctx02"),
                }),
            },
        },
        ScriptOp::TaskAccepted { interpreter_context: "ctx03", },
        ScriptOp::PerformerNext,
        ScriptOp::ExpectPollRequestAndInterpreter {
            expect_context: "ctx03",
        },
        ScriptOp::RequestAndInterpreterIncomingRequest {
            request: proto::Request::ReadBlock(proto::RequestReadBlock { block_id: block::Id::init(), context: "ctx04", }),
            interpreter_context: "ctx05",
        },
        ScriptOp::ExpectReadBlockCacheHit {
            expect_block_bytes: hello_world_bytes(),
            expect_context: "ctx04",
        },
        ScriptOp::PerformerNext,
        ScriptOp::ExpectPollRequestAndInterpreter {
            expect_context: "ctx05",
        },
        ScriptOp::RequestAndInterpreterIncomingRequest {
            request: proto::Request::DeleteBlock(proto::RequestDeleteBlock { block_id: block::Id::init(), context: "ctx06", }),
            interpreter_context: "ctx07",
        },
        ScriptOp::PerformerNext,
        ScriptOp::ExpectPollRequestAndInterpreter {
            expect_context: "ctx07",
        },
        ScriptOp::RequestAndInterpreterIncomingRequest {
            request: proto::Request::WriteBlock(proto::RequestWriteBlock { block_bytes: hello_world_bytes(), context: "ctx08", }),
            interpreter_context: "ctx09",
        },
        ScriptOp::PerformerNext,
        ScriptOp::ExpectPollRequestAndInterpreter {
            expect_context: "ctx09",
        },
        ScriptOp::RequestAndInterpreterIncomingTaskDone {
            task_done: task::Done {
                current_offset: 77,
                task: task::TaskDone {
                    block_id: block::Id::init(),
                    kind: task::TaskDoneKind::WriteBlock(task::TaskDoneWriteBlock {
                        context: task::WriteBlockContext::External("ctx02"),
                    }),
                },
            },
        },
        ScriptOp::ExpectWriteBlockDoneDone {
            expect_block_id: block::Id::init(),
            expect_context: "ctx02",
        },
        ScriptOp::PerformerNext,
        ScriptOp::ExpectInterpretTask {
            expect_offset: 77,
            expect_task: ExpectTask {
                block_id: block::Id::init().next(),
                kind: ExpectTaskKind::WriteBlock(ExpectTaskWriteBlock {
                    block_bytes: hello_world_bytes(),
                    commit_type: task::CommitType::CommitAndEof,
                    context: task::WriteBlockContext::External("ctx08"),
                }),
            },
        },
        ScriptOp::TaskAccepted { interpreter_context: "ctx09", },
        ScriptOp::PerformerNext,
        ScriptOp::ExpectPollRequestAndInterpreter {
            expect_context: "ctx09",
        },
        ScriptOp::RequestAndInterpreterIncomingTaskDone {
            task_done: task::Done {
                current_offset: 130,
                task: task::TaskDone {
                    block_id: block::Id::init().next(),
                    kind: task::TaskDoneKind::WriteBlock(task::TaskDoneWriteBlock {
                        context: task::WriteBlockContext::External("ctx08"),
                    }),
                },
            },
        },
        ScriptOp::ExpectWriteBlockDoneDone {
            expect_block_id: block::Id::init().next(),
            expect_context: "ctx08",
        },
        ScriptOp::PerformerNext,
        ScriptOp::ExpectInterpretTask {
            expect_offset: 24,
            expect_task: ExpectTask {
                block_id: block::Id::init(),
                kind: ExpectTaskKind::DeleteBlock(ExpectTaskDeleteBlock {
                    context: task::DeleteBlockContext::External("ctx06"),
                }),
            },
        },
        ScriptOp::TaskAccepted { interpreter_context: "ctx0a", },
        ScriptOp::PerformerNext,
        ScriptOp::ExpectPollRequestAndInterpreter {
            expect_context: "ctx0a",
        },
        ScriptOp::RequestAndInterpreterIncomingRequest {
            request: proto::Request::ReadBlock(proto::RequestReadBlock { block_id: block::Id::init(), context: "ctx0b", }),
            interpreter_context: "ctx0c",
        },
        ScriptOp::ExpectReadBlockNotFound { // here
            expect_context: "ctx0b",
        },
        ScriptOp::PerformerNext,
        ScriptOp::ExpectPollRequestAndInterpreter {
            expect_context: "ctx0c",
        },
        ScriptOp::RequestAndInterpreterIncomingRequest {
            request: proto::Request::DeleteBlock(proto::RequestDeleteBlock { block_id: block::Id::init(), context: "ctx0d", }),
            interpreter_context: "ctx0e",
        },
        ScriptOp::ExpectDeleteBlockNotFound {
            expect_context: "ctx0d",
        },
        ScriptOp::PerformerNext,
        ScriptOp::ExpectPollRequestAndInterpreter {
            expect_context: "ctx0e",
        },
        ScriptOp::RequestAndInterpreterIncomingTaskDone {
            task_done: task::Done {
                current_offset: 24,
                task: task::TaskDone {
                    block_id: block::Id::init(),
                    kind: task::TaskDoneKind::DeleteBlock(task::TaskDoneDeleteBlock {
                        context: task::DeleteBlockContext::External("ctx06"),
                    }),
                },
            },
        },
        ScriptOp::ExpectDeleteBlockDoneDone {
            expect_context: "ctx06",
        },
        ScriptOp::PerformerNext,
        ScriptOp::ExpectPollRequest,
        ScriptOp::RequestIncomingRequest {
            request: proto::Request::WriteBlock(proto::RequestWriteBlock { block_bytes: hello_world_bytes(), context: "ctx0f", }),
        },
        ScriptOp::PerformerNext,
        ScriptOp::ExpectInterpretTask {
            expect_offset: 24,
            expect_task: ExpectTask {
                block_id: block::Id::init().next().next(),
                kind: ExpectTaskKind::WriteBlock(ExpectTaskWriteBlock {
                    block_bytes: hello_world_bytes(),
                    commit_type: task::CommitType::CommitOnly,
                    context: task::WriteBlockContext::External("ctx0f"),
                }),
            },
        },
        ScriptOp::TaskAccepted { interpreter_context: "ctx10", },
        ScriptOp::PerformerNext,
        ScriptOp::ExpectPollRequestAndInterpreter {
            expect_context: "ctx10",
        },
        ScriptOp::RequestAndInterpreterIncomingRequest {
            request: proto::Request::WriteBlock(proto::RequestWriteBlock { block_bytes: hello_world_bytes(), context: "ctx11", }),
            interpreter_context: "ctx12",
        },
        ScriptOp::ExpectWriteBlockNoSpaceLeft {
            expect_context: "ctx11",
        },
        ScriptOp::PerformerNext,
        ScriptOp::ExpectPollRequestAndInterpreter {
            expect_context: "ctx12",
        },
        ScriptOp::RequestAndInterpreterIncomingRequest {
            request: proto::Request::ReadBlock(proto::RequestReadBlock { block_id: block::Id::init().next(), context: "ctx13", }),
            interpreter_context: "ctx14",
        },
        ScriptOp::PerformerNext,
        ScriptOp::ExpectPollRequestAndInterpreter {
            expect_context: "ctx14",
        },
        ScriptOp::RequestAndInterpreterIncomingTaskDone {
            task_done: task::Done {
                current_offset: 24,
                task: task::TaskDone {
                    block_id: block::Id::init().next().next(),
                    kind: task::TaskDoneKind::WriteBlock(task::TaskDoneWriteBlock {
                        context: task::WriteBlockContext::External("ctx11"),
                    }),
                },
            },
        },
        ScriptOp::ExpectWriteBlockDoneDone {
            expect_block_id: block::Id::init().next().next(),
            expect_context: "ctx11",
        },
        ScriptOp::PerformerNext,
        ScriptOp::ExpectInterpretTask {
            expect_offset: 77,
            expect_task: ExpectTask {
                block_id: block::Id::init().next(),
                kind: ExpectTaskKind::ReadBlock(ExpectTaskReadBlock {
                    block_header: storage::BlockHeader {
                        block_id: block::Id::init().next(),
                        block_size: 13,
                        ..Default::default()
                    },
                    context: task::ReadBlockContext::External("ctx13"),
                }),
            },
        },
        ScriptOp::TaskAccepted { interpreter_context: "ctx15", },
        ScriptOp::PerformerNext,
        ScriptOp::ExpectPollRequestAndInterpreter {
            expect_context: "ctx15",
        },
        ScriptOp::RequestAndInterpreterIncomingTaskDone {
            task_done: task::Done {
                current_offset: 77,
                task: task::TaskDone {
                    block_id: block::Id::init().next(),
                    kind: task::TaskDoneKind::ReadBlock(task::TaskDoneReadBlock {
                        block_bytes: hello_world_bytes().into_mut().unwrap(),
                        context: task::ReadBlockContext::External("ctx13"),
                    }),
                },
            },
        },
        ScriptOp::ExpectReadBlockDoneDone {
            expect_block_bytes: hello_world_bytes(),
            expect_context: "ctx13",
        },
    ];

    interpret(performer, script)
}
