use super::{
    task,
    block,
    proto,
    schema,
    Op,
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
    InterpretTask,
    PollRequestAndInterpreter,
    RequestOrInterpreterIncoming,
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
    Performer::new(schema, 16)
}

fn sample_hello_world() -> block::Bytes {
    let mut block_bytes_mut = block::BytesMut::new();
    block_bytes_mut.extend("hello, world!".as_bytes().iter().cloned());
    block_bytes_mut.freeze()
}

#[derive(Debug)]
enum ScriptOp {
    PerformerNext,
    ExpectPollRequest,
    ExpectPollRequestAndInterpreter { expect_context: C, },
    ExpectInterpretTask { expect_offset: u64, expect_task_kind: task::TaskKind<Context>, },
    PollRequestAndInterpreterNext { incoming: RequestOrInterpreterIncoming<Context>, },
    PollRequestNext { incoming: proto::Request<Context>, },
    TaskAccepted { interpreter_context: C, },
    ExpectLendBlockSuccess { expect_context: C, },
    ExpectWriteBlockNoSpaceLeft { expect_context: C, },
    ExpectReadBlockCacheHit { expect_block_bytes: block::Bytes, expect_context: C, },
    ExpectReadBlockNotFound { expect_context: C, },
    ExpectDeleteBlockNotFound { expect_context: C, },
    ExpectWriteBlockDoneDone { expect_block_id: block::Id, expect_context: C, },
    ExpectReadBlockDOneDone { expect_block_bytes: block::Bytes, expect_context: C, },
    ExpectDeleteBlockDoneDone { expect_context: C, },
}

fn interpret(mut performer: Performer<Context>, mut script: Vec<ScriptOp>) {
    script.reverse();
    loop {
        match script.pop() {
            None =>
                break,
            Some(ScriptOp::PerformerNext) =>
                (),
            Some(other_op) =>
                panic!("expected Script::PerformerNext but got {:?}", other_op),
        }

        let op = performer.next();
        match &op {
            Op::PollRequestAndInterpreter(PollRequestAndInterpreter { interpreter_context, .. }) =>
                match script.pop() {
                    None =>
                        panic!("unexpected script end on {:?}, expecting ExpectPollRequestAndInterpreter", op),
                    Some(ScriptOp::ExpectPollRequestAndInterpreter { ref expect_context, }) if expect_context == interpreter_context =>
                        (),
                    Some(other_op) =>
                        panic!("expecting exact ExpectPollRequestAndInterpreter for {:?} but got {:?}", op, other_op),
                },
            Op::PollRequest(PollRequest { .. }) =>
                match script.pop() {
                    None =>
                        panic!("unexpected script end on {:?}, expecting ExpectPollRequest", op),
                    Some(ScriptOp::ExpectPollRequest) =>
                        (),
                    Some(other_op) =>
                        panic!("expecting exact ExpectPollRequest for {:?} but got {:?}", op, other_op),
                },
            Op::InterpretTask(InterpretTask { offset, task_kind, .. }) =>
                match script.pop() {
                    None =>
                        panic!("unexpected script end on {:?}, expecting ExpectInterpretTask", op),
                    Some(ScriptOp::ExpectInterpretTask { ref expect_offset, ref expect_task_kind, })
                        if expect_offset == offset && expect_task_kind == task_kind
                        => (),
                    Some(other_op) =>
                        panic!("expecting exact ExpectInterpretTask for {:?} but got {:?}", op, other_op),
                },
        };

        let perform_op = match op {
            Op::PollRequestAndInterpreter(var @ PollRequestAndInterpreter { .. }) =>
                match script.pop() {
                    None =>
                        panic!("unexpected script end on {:?}, expecting PollRequestAndInterpreterNext", var),
                    Some(ScriptOp::PollRequestAndInterpreterNext { incoming, }) =>
                        var.next.next(incoming),
                    Some(other_op) =>
                        panic!("expecting exact PollRequestAndInterpreterNext for {:?} but got {:?}", var, other_op),
                },
            Op::PollRequest(var @ PollRequest { .. }) =>
                match script.pop() {
                    None =>
                        panic!("unexpected script end on {:?}, expecting PollRequestNext", var),
                    Some(ScriptOp::PollRequestNext { incoming, }) =>
                        var.next.next(incoming),
                    Some(other_op) =>
                        panic!("expecting exact PollRequestNext for {:?} but got {:?}", var, other_op),
                },
            Op::InterpretTask(var @ InterpretTask { .. }) =>
                match script.pop() {
                    None =>
                        panic!("unexpected script end on {:?}, expecting TaskAccepted", var),
                    Some(ScriptOp::TaskAccepted { interpreter_context, }) => {
                        performer = var.next.task_accepted(interpreter_context);
                        continue;
                    },
                    Some(other_op) =>
                        panic!("expecting exact TaskAccepted for {:?} but got {:?}", var, other_op),
                },
        };

        match &perform_op {
            PerformOp::Idle(..) =>
                (),

            PerformOp::LendBlock(LendBlockOp::Success { context, .. }) =>
                match script.pop() {
                    None =>
                        panic!("unexpected script end on {:?}, expecting ExpectLendBlockSuccess", perform_op),
                    Some(ScriptOp::ExpectLendBlockSuccess { ref expect_context, }) if expect_context == context =>
                        (),
                    Some(other_op) =>
                        panic!("expecting exact ExpectLendBlockSuccess for {:?} but got {:?}", perform_op, other_op),
                },

            PerformOp::WriteBlock(WriteBlockOp::NoSpaceLeft { context, .. }) =>
                match script.pop() {
                    None =>
                        panic!("unexpected script end on {:?}, expecting ExpectWriteBlockNoSpaceLeft", perform_op),
                    Some(ScriptOp::ExpectWriteBlockNoSpaceLeft { ref expect_context, }) if expect_context == context =>
                        (),
                    Some(other_op) =>
                        panic!("expecting exact ExpectWriteBlockNoSpaceLeft for {:?} but got {:?}", perform_op, other_op),
                },

            PerformOp::ReadBlock(ReadBlockOp::CacheHit { context, block_bytes, .. }) =>
                match script.pop() {
                    None =>
                        panic!("unexpected script end on {:?}, expecting ExpectReadBlockCacheHit", perform_op),
                    Some(ScriptOp::ExpectReadBlockCacheHit { ref expect_block_bytes, ref expect_context, })
                        if expect_context == context && expect_block_bytes == block_bytes =>
                        (),
                    Some(other_op) =>
                        panic!("expecting exact ExpectReadBlockCacheHit for {:?} but got {:?}", perform_op, other_op),
                },

            PerformOp::ReadBlock(ReadBlockOp::NotFound { context, .. }) =>
                match script.pop() {
                    None =>
                        panic!("unexpected script end on {:?}, expecting ExpectReadBlockNotFound", perform_op),
                    Some(ScriptOp::ExpectReadBlockNotFound { ref expect_context, }) if expect_context == context =>
                        (),
                    Some(other_op) =>
                        panic!("expecting exact ExpectReadBlockNotFound for {:?} but got {:?}", perform_op, other_op),
                },

            PerformOp::DeleteBlock(DeleteBlockOp::NotFound { context, .. }) =>
                match script.pop() {
                    None =>
                        panic!("unexpected script end on {:?}, expecting ExpectDeleteBlockNotFound", perform_op),
                    Some(ScriptOp::ExpectDeleteBlockNotFound { ref expect_context, }) if expect_context == context =>
                        (),
                    Some(other_op) =>
                        panic!("expecting exact ExpectDeleteBlockNotFound for {:?} but got {:?}", perform_op, other_op),
                },

            PerformOp::WriteBlockDone(WriteBlockDoneOp::Done { block_id, context, .. }) =>
                match script.pop() {
                    None =>
                        panic!("unexpected script end on {:?}, expecting ExpectWriteBlockDoneDone", perform_op),
                    Some(ScriptOp::ExpectWriteBlockDoneDone { ref expect_block_id, ref expect_context, })
                        if expect_context == context && expect_block_id == block_id =>
                        (),
                    Some(other_op) =>
                        panic!("expecting exact ExpectWriteBlockDoneDone for {:?} but got {:?}", perform_op, other_op),
                },

            PerformOp::ReadBlockDone(ReadBlockDoneOp::Done { block_bytes, context, .. }) =>
                match script.pop() {
                    None =>
                        panic!("unexpected script end on {:?}, expecting ExpectReadBlockDOneDone", perform_op),
                    Some(ScriptOp::ExpectReadBlockDOneDone { ref expect_block_bytes, ref expect_context, })
                        if expect_context == context && expect_block_bytes == block_bytes =>
                        (),
                    Some(other_op) =>
                        panic!("expecting exact ExpectReadBlockDOneDone for {:?} but got {:?}", perform_op, other_op),
                },

            PerformOp::DeleteBlockDone(DeleteBlockDoneOp::Done { context, .. }) =>
                match script.pop() {
                    None =>
                        panic!("unexpected script end on {:?}, expecting ExpectDeleteBlockDoneDone", perform_op),
                    Some(ScriptOp::ExpectDeleteBlockDoneDone { ref expect_context, }) if expect_context == context =>
                        (),
                    Some(other_op) =>
                        panic!("expecting exact ExpectDeleteBlockDoneDone for {:?} but got {:?}", perform_op, other_op),
                },
        }

        performer = match perform_op {
            PerformOp::Idle(performer) =>
                performer,
            PerformOp::LendBlock(LendBlockOp::Success { performer, .. }) =>
                performer,
            PerformOp::WriteBlock(WriteBlockOp::NoSpaceLeft { performer, .. }) =>
                performer,
            PerformOp::ReadBlock(ReadBlockOp::CacheHit { performer, .. }) =>
                performer,
            PerformOp::ReadBlock(ReadBlockOp::NotFound { performer, .. }) =>
                performer,
            PerformOp::DeleteBlock(DeleteBlockOp::NotFound { performer, .. }) =>
                performer,
            PerformOp::WriteBlockDone(WriteBlockDoneOp::Done { performer, .. }) =>
                performer,
            PerformOp::ReadBlockDone(ReadBlockDoneOp::Done { performer, .. }) =>
                performer,
            PerformOp::DeleteBlockDone(DeleteBlockDoneOp::Done { performer, .. }) =>
                performer,
        };
    }
}

#[test]
fn script_00() {
    let performer = init();
    let script = vec![
        ScriptOp::PerformerNext,
        ScriptOp::ExpectPollRequest,
        ScriptOp::PollRequestNext { incoming: proto::Request::LendBlock(proto::RequestLendBlock { context: "ctx00", }), },
        ScriptOp::ExpectLendBlockSuccess { expect_context: "ctx00", },
    ];

    interpret(performer, script)
}
