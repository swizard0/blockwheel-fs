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
    Expect(ExpectOp),
    Do(DoOp),
}

#[derive(Debug)]
enum ExpectOp {
    Idle,
    PollRequest,
    PollRequestAndInterpreter { expect_context: C, },
    InterpretTask { expect_offset: u64, expect_task: ExpectTask, },
    LendBlockSuccess { expect_context: C, },
    WriteBlockNoSpaceLeft { expect_context: C, },
    WriteBlockDone { expect_block_id: block::Id, expect_context: C, },
    ReadBlockNotFound { expect_context: C, },
    ReadBlockDone { expect_block_bytes: block::Bytes, expect_context: C, },
    DeleteBlockNotFound { expect_context: C, },
    DeleteBlockDone { expect_block_id: block::Id, expect_context: C, },
}

#[derive(Debug)]
enum DoOp {
    RequestAndInterpreterIncomingRequest { request: proto::Request<Context>, interpreter_context: C, },
    RequestAndInterpreterIncomingTaskDone { task_done: task::Done<Context>, },
    RequestIncomingRequest { request: proto::Request<Context>, },
    TaskAccept { interpreter_context: C, },
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
        println!(" || script line: {}", script_len - script.len());

        op = match op {

            Op::Idle(performer) =>
                match script.pop() {
                    None =>
                        panic!("unexpected script end on Idle, expecting ExpectOp::Idle @ {}", script_len - script.len()),
                    Some(ScriptOp::Expect(ExpectOp::Idle)) =>
                        performer.next(),
                    Some(other_op) =>
                        panic!("expected ExpectOp::Idle but got {:?} @ {}", other_op, script_len - script.len()),
                },

            Op::Query(QueryOp::PollRequestAndInterpreter(poll)) =>
                match script.pop() {
                    None =>
                        panic!(
                            "unexpected script end on PollRequestAndInterpreter, expecting ExpectOp::PollRequestAndInterpreter @ {}",
                            script_len - script.len(),
                        ),
                    Some(ScriptOp::Expect(ExpectOp::PollRequestAndInterpreter { expect_context, }))
                        if expect_context == poll.interpreter_context =>
                        match script.pop() {
                            None =>
                                break,
                            Some(ScriptOp::Do(DoOp::RequestAndInterpreterIncomingRequest { request, interpreter_context, })) =>
                                poll.next.incoming_request(request, interpreter_context),
                            Some(ScriptOp::Do(DoOp::RequestAndInterpreterIncomingTaskDone { task_done, })) =>
                                poll.next.incoming_task_done(task_done),
                            Some(other_op) =>
                                panic!("expected DoOp::RequestAndInterpreterIncoming* but got {:?} @ {}", other_op, script_len - script.len()),
                        },
                    Some(other_op) =>
                        panic!(
                            "expecting exact ExpectOp::PollRequestAndInterpreter for PollRequestAndInterpreter but got {:?} @ {}",
                            other_op, script_len - script.len(),
                        ),
                },

            Op::Query(QueryOp::PollRequest(poll)) =>
                match script.pop() {
                    None =>
                        panic!("unexpected script end on PollRequest, expecting ExpectOp::PollRequest @ {}", script_len - script.len()),
                    Some(ScriptOp::Expect(ExpectOp::PollRequest)) =>
                        match script.pop() {
                            None =>
                                break,
                            Some(ScriptOp::Do(DoOp::RequestIncomingRequest { request, })) =>
                                poll.next.incoming_request(request),
                            Some(other_op) =>
                                panic!("expected DoOp::RequestIncoming* but got {:?} @ {}", other_op, script_len - script.len()),
                        },
                    Some(other_op) =>
                        panic!("expecting exact ExpectOp::PollRequest for PollRequest but got {:?} @ {}", other_op, script_len - script.len()),
                },

            Op::Query(QueryOp::InterpretTask(InterpretTask { offset, task, next, })) =>
                match script.pop() {
                    None =>
                        panic!("unexpected script end on InterpretTask, expecting ExpectOp::InterpretTask @ {}", script_len - script.len()),
                    Some(ScriptOp::Expect(ExpectOp::InterpretTask { expect_offset, expect_task, }))
                        if expect_offset == offset && expect_task == task =>
                        match script.pop() {
                            None =>
                                panic!("unexpected script end on ExpectOp::InterpretTask, expecting DoOp::TaskAccept @ {}", script_len - script.len()),
                            Some(ScriptOp::Do(DoOp::TaskAccept { interpreter_context, })) => {
                                let performer = next.task_accepted(interpreter_context);
                                performer.next()
                            },
                            Some(other_op) =>
                                panic!("expected DoTaskAccept but got {:?} @ {}", other_op, script_len - script.len()),
                        },
                    Some(other_op) =>
                        panic!("expecting exact ExpectOp::InterpretTask for InterpretTask but got {:?} @ {}", other_op, script_len - script.len()),
                },

            Op::Event(Event { op: EventOp::LendBlock(TaskDoneOp { context, op: LendBlockOp::Success { .. }, }), performer, }) =>
                match script.pop() {
                    None =>
                        panic!("unexpected script end on LendBlockOp::Success, expecting ExpectOp::LendBlockSuccess @ {}", script_len - script.len()),
                    Some(ScriptOp::Expect(ExpectOp::LendBlockSuccess { expect_context, })) if expect_context == context =>
                        performer.next(),
                    Some(other_op) =>
                        panic!(
                            "expecting exact ExpectOp::LendBlockSuccess for LendBlockOp::Success but got {:?} @ {}",
                            other_op, script_len - script.len(),
                        ),
                },

            Op::Event(Event { op: EventOp::WriteBlock(TaskDoneOp { context, op: WriteBlockOp::NoSpaceLeft, }), performer, }) =>
                match script.pop() {
                    None =>
                        panic!(
                            "unexpected script end on WriteBlockOp::NoSpaceLeft, expecting ExpectOp::WriteBlockNoSpaceLeft @ {}",
                            script_len - script.len(),
                        ),
                    Some(ScriptOp::Expect(ExpectOp::WriteBlockNoSpaceLeft { expect_context, })) if expect_context == context =>
                        performer.next(),
                    Some(other_op) =>
                        panic!(
                            "expecting exact ExpectOp::WriteBlockNoSpaceLeft for WriteBlockOp::NoSpaceLeft but got {:?} @ {}",
                            other_op, script_len - script.len(),
                        ),
                },

            Op::Event(Event { op: EventOp::WriteBlock(TaskDoneOp { context, op: WriteBlockOp::Done { block_id, }, }), performer,}) =>
                match script.pop() {
                    None =>
                        panic!(
                            "unexpected script end on WriteBlockOp::Done, expecting ExpectOp::WriteBlockDone @ {}",
                            script_len - script.len(),
                        ),
                    Some(ScriptOp::Expect(ExpectOp::WriteBlockDone { expect_block_id, expect_context, }))
                        if expect_block_id == block_id && expect_context == context =>
                        performer.next(),
                    Some(other_op) =>
                        panic!(
                            "expecting exact ExpectOp::WriteBlockDone for WriteBlockOp::Done but got {:?} @ {}",
                            other_op, script_len - script.len(),
                        ),
                },

            Op::Event(Event { op: EventOp::ReadBlock(TaskDoneOp { context, op: ReadBlockOp::NotFound, }), performer, }) =>
                match script.pop() {
                    None =>
                        panic!(
                            "unexpected script end on ReadBlockOp::NotFound, expecting ExpectOp::ReadBlockNotFound @ {}",
                            script_len - script.len(),
                        ),
                    Some(ScriptOp::Expect(ExpectOp::ReadBlockNotFound { expect_context, })) if expect_context == context =>
                        performer.next(),
                    Some(other_op) =>
                        panic!(
                            "expecting exact ExpectOp::ReadBlockNotFound for ReadBlockOp::NotFound but got {:?} @ {}",
                            other_op, script_len - script.len(),
                        ),
                },

            Op::Event(Event { op: EventOp::ReadBlock(TaskDoneOp { context, op: ReadBlockOp::Done { block_bytes, }, }), performer, }) =>
                match script.pop() {
                    None =>
                        panic!(
                            "unexpected script end on ReadBlockOp::Done, expecting ExpectOp::ReadBlockDone @ {}",
                            script_len - script.len(),
                        ),
                    Some(ScriptOp::Expect(ExpectOp::ReadBlockDone { expect_block_bytes, expect_context, }))
                        if expect_block_bytes == block_bytes && expect_context == context =>
                        performer.next(),
                    Some(other_op) =>
                        panic!(
                            "expecting exact ExpectOp::ReadBlockDone for ReadBlockOp::Done but got {:?} @ {}",
                            other_op, script_len - script.len(),
                        ),
                },

            Op::Event(Event { op: EventOp::DeleteBlock(TaskDoneOp { context, op: DeleteBlockOp::NotFound, }), performer, }) =>
                match script.pop() {
                    None =>
                        panic!(
                            "unexpected script end on DeleteBlockOp::NotFound, expecting ExpectOp::DeleteBlockNotFound @ {}",
                            script_len - script.len(),
                        ),
                    Some(ScriptOp::Expect(ExpectOp::DeleteBlockNotFound { expect_context, })) if expect_context == context =>
                        performer.next(),
                    Some(other_op) =>
                        panic!(
                            "expecting exact ExpectOp::DeleteBlockNotFound for ReadBlockOp::NotFound but got {:?} @ {}",
                            other_op, script_len - script.len(),
                        ),
                },

            Op::Event(Event { op: EventOp::DeleteBlock(TaskDoneOp { context, op: DeleteBlockOp::Done { block_id, }, }), performer,}) =>
                match script.pop() {
                    None =>
                        panic!(
                            "unexpected script end on DeleteBlockOp::Done, expecting ExpectOp::DeleteBlockDone @ {}",
                            script_len - script.len(),
                        ),
                    Some(ScriptOp::Expect(ExpectOp::DeleteBlockDone { expect_block_id, expect_context, }))
                        if expect_block_id == block_id && expect_context == context =>
                        performer.next(),
                    Some(other_op) =>
                        panic!(
                            "expecting exact ExpectOp::DeleteBlockDone for DeleteBlockOp::Done but got {:?} @ {}",
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
