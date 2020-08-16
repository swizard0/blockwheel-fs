use super::{
    lru,
    task,
    block,
    proto,
    schema,
    Op,
    Event,
    QueryOp,
    EventOp,
    Performer,
    TaskDoneOp,
    LendBlockOp,
    ReadBlockOp,
    WriteBlockOp,
    DeleteBlockOp,
    InterpretTask,
    Context as BaseContext,
    super::{
        storage,
    },
};

mod basic;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
struct Context;

type C = &'static str;

impl BaseContext for Context {
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
    ExpectIdle,
    ExpectPollRequest,
    ExpectPollRequestAndInterpreter { expect_context: C, },
    ExpectInterpretTask { expect_offset: u64, expect_task: ExpectTask, },
    DoRequestAndInterpreterIncomingRequest { request: proto::Request<Context>, interpreter_context: C, },
    DoRequestAndInterpreterIncomingTaskDone { task_done: task::Done<Context>, },
    DoRequestIncomingRequest { request: proto::Request<Context>, },
    DoTaskAccept { interpreter_context: C, },
    ExpectLendBlockSuccess { expect_context: C, },
    ExpectWriteBlockNoSpaceLeft { expect_context: C, },
    ExpectWriteBlockDone { expect_block_id: block::Id, expect_context: C, },
    ExpectReadBlockNotFound { expect_context: C, },
    ExpectReadBlockDone { expect_block_bytes: block::Bytes, expect_context: C, },
    ExpectDeleteBlockNotFound { expect_context: C, },
    ExpectDeleteBlockDone { expect_block_id: block::Id, expect_context: C, },
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

fn interpret(performer: Performer<Context>, mut script: Vec<ScriptOp>) {
    let script_len = script.len();
    script.reverse();

    let mut op = performer.next();
    loop {
        op = match op {

            Op::Idle(performer) =>
                match script.pop() {
                    None =>
                        break,
                    Some(ScriptOp::ExpectIdle) =>
                        performer.next(),
                    Some(other_op) =>
                        panic!("expected ExpectIdle but got {:?} @ {}", other_op, script_len - script.len()),
                },

            Op::Query(QueryOp::PollRequestAndInterpreter(poll)) =>
                match script.pop() {
                    None =>
                        panic!(
                            "unexpected script end on PollRequestAndInterpreter, expecting ExpectPollRequestAndInterpreter @ {}",
                            script_len - script.len(),
                        ),
                    Some(ScriptOp::ExpectPollRequestAndInterpreter { expect_context, }) if expect_context == poll.interpreter_context =>
                        match script.pop() {
                            None =>
                                break,
                            Some(ScriptOp::DoRequestAndInterpreterIncomingRequest { request, interpreter_context, }) =>
                                poll.next.incoming_request(request, interpreter_context),
                            Some(ScriptOp::DoRequestAndInterpreterIncomingTaskDone { task_done, }) =>
                                poll.next.incoming_task_done(task_done),
                            Some(other_op) =>
                                panic!("expected DoRequestAndInterpreterIncoming* but got {:?} @ {}", other_op, script_len - script.len()),
                        },
                    Some(other_op) =>
                        panic!(
                            "expecting exact ExpectPollRequestAndInterpreter for PollRequestAndInterpreter but got {:?} @ {}",
                            other_op, script_len - script.len(),
                        ),
                },

            Op::Query(QueryOp::PollRequest(poll)) =>
                match script.pop() {
                    None =>
                        panic!("unexpected script end on PollRequest, expecting ExpectPollRequest @ {}", script_len - script.len()),
                    Some(ScriptOp::ExpectPollRequest) =>
                        match script.pop() {
                            None =>
                                break,
                            Some(ScriptOp::DoRequestIncomingRequest { request, }) =>
                                poll.next.incoming_request(request),
                            Some(other_op) =>
                                panic!("expected DoRequestIncoming* but got {:?} @ {}", other_op, script_len - script.len()),
                        },
                    Some(other_op) =>
                        panic!("expecting exact ExpectPollRequest for PollRequest but got {:?} @ {}", other_op, script_len - script.len()),
                },

            Op::Query(QueryOp::InterpretTask(InterpretTask { offset, task, next, })) =>
                match script.pop() {
                    None =>
                        panic!("unexpected script end on InterpretTask, expecting ExpectInterpretTask @ {}", script_len - script.len()),
                    Some(ScriptOp::ExpectInterpretTask { expect_offset, expect_task, }) if expect_offset == offset && expect_task == task =>
                        match script.pop() {
                            None =>
                                panic!("unexpected script end on ExpectInterpretTask, expecting DoTaskAccept @ {}", script_len - script.len()),
                            Some(ScriptOp::DoTaskAccept { interpreter_context, }) => {
                                let performer = next.task_accepted(interpreter_context);
                                performer.next()
                            },
                            Some(other_op) =>
                                panic!("expected DoTaskAccept but got {:?} @ {}", other_op, script_len - script.len()),
                        },
                    Some(other_op) =>
                        panic!("expecting exact ExpectInterpretTask for InterpretTask but got {:?} @ {}", other_op, script_len - script.len()),
                },

            Op::Event(Event { op: EventOp::LendBlock(TaskDoneOp { context, op: LendBlockOp::Success { .. }, }), performer, }) =>
                match script.pop() {
                    None =>
                        panic!("unexpected script end on LendBlockOp::Success, expecting ExpectLendBlockSuccess @ {}", script_len - script.len()),
                    Some(ScriptOp::ExpectLendBlockSuccess { expect_context, }) if expect_context == context =>
                        performer.next(),
                    Some(other_op) =>
                        panic!("expecting exact ExpectLendBlockSuccess for LendBlockOp::Success but got {:?} @ {}", other_op, script_len - script.len()),
                },

            Op::Event(Event { op: EventOp::WriteBlock(TaskDoneOp { context, op: WriteBlockOp::NoSpaceLeft, }), performer, }) =>
                match script.pop() {
                    None =>
                        panic!(
                            "unexpected script end on WriteBlockOp::NoSpaceLeft, expecting ExpectWriteBlockNoSpaceLeft @ {}",
                            script_len - script.len(),
                        ),
                    Some(ScriptOp::ExpectWriteBlockNoSpaceLeft { expect_context, }) if expect_context == context =>
                        performer.next(),
                    Some(other_op) =>
                        panic!(
                            "expecting exact ExpectWriteBlockNoSpaceLeft for WriteBlockOp::NoSpaceLeft but got {:?} @ {}",
                            other_op, script_len - script.len(),
                        ),
                },

            Op::Event(Event { op: EventOp::WriteBlock(TaskDoneOp { context, op: WriteBlockOp::Done { block_id, }, }), performer,}) =>
                match script.pop() {
                    None =>
                        panic!(
                            "unexpected script end on WriteBlockOp::Done, expecting ExpectWriteBlockDone @ {}",
                            script_len - script.len(),
                        ),
                    Some(ScriptOp::ExpectWriteBlockDone { expect_block_id, expect_context, })
                        if expect_block_id == block_id && expect_context == context =>
                        performer.next(),
                    Some(other_op) =>
                        panic!(
                            "expecting exact ExpectWriteBlockDone for WriteBlockOp::Done but got {:?} @ {}",
                            other_op, script_len - script.len(),
                        ),
                },

            Op::Event(Event { op: EventOp::ReadBlock(TaskDoneOp { context, op: ReadBlockOp::NotFound, }), performer, }) =>
                match script.pop() {
                    None =>
                        panic!(
                            "unexpected script end on ReadBlockOp::NotFound, expecting ExpectReadBlockNotFound @ {}",
                            script_len - script.len(),
                        ),
                    Some(ScriptOp::ExpectReadBlockNotFound { expect_context, }) if expect_context == context =>
                        performer.next(),
                    Some(other_op) =>
                        panic!(
                            "expecting exact ExpectReadBlockNotFound for ReadBlockOp::NotFound but got {:?} @ {}",
                            other_op, script_len - script.len(),
                        ),
                },

            Op::Event(Event { op: EventOp::ReadBlock(TaskDoneOp { context, op: ReadBlockOp::Done { block_bytes, }, }), performer, }) =>
                match script.pop() {
                    None =>
                        panic!(
                            "unexpected script end on ReadBlockOp::Done, expecting ExpectReadBlockDone @ {}",
                            script_len - script.len(),
                        ),
                    Some(ScriptOp::ExpectReadBlockDone { expect_block_bytes, expect_context, })
                        if expect_block_bytes == block_bytes && expect_context == context =>
                        performer.next(),
                    Some(other_op) =>
                        panic!(
                            "expecting exact ExpectReadBlockDone for ReadBlockOp::Done but got {:?} @ {}",
                            other_op, script_len - script.len(),
                        ),
                },

            Op::Event(Event { op: EventOp::DeleteBlock(TaskDoneOp { context, op: DeleteBlockOp::NotFound, }), performer, }) =>
                match script.pop() {
                    None =>
                        panic!(
                            "unexpected script end on DeleteBlockOp::NotFound, expecting ExpectDeleteBlockNotFound @ {}",
                            script_len - script.len(),
                        ),
                    Some(ScriptOp::ExpectDeleteBlockNotFound { expect_context, }) if expect_context == context =>
                        performer.next(),
                    Some(other_op) =>
                        panic!(
                            "expecting exact ExpectDeleteBlockNotFound for ReadBlockOp::NotFound but got {:?} @ {}",
                            other_op, script_len - script.len(),
                        ),
                },

            Op::Event(Event { op: EventOp::DeleteBlock(TaskDoneOp { context, op: DeleteBlockOp::Done { block_id, }, }), performer,}) =>
                match script.pop() {
                    None =>
                        panic!(
                            "unexpected script end on DeleteBlockOp::Done, expecting ExpectDeleteBlockDone @ {}",
                            script_len - script.len(),
                        ),
                    Some(ScriptOp::ExpectDeleteBlockDone { expect_block_id, expect_context, })
                        if expect_block_id == block_id && expect_context == context =>
                        performer.next(),
                    Some(other_op) =>
                        panic!(
                            "expecting exact ExpectDeleteBlockDone for DeleteBlockOp::Done but got {:?} @ {}",
                            other_op, script_len - script.len(),
                        ),
                },

        };
    }
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
