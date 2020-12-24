use alloc_pool::bytes::{
    Bytes,
    BytesMut,
    BytesPool,
};

use super::{
    lru,
    task,
    block,
    proto,
    Op,
    Event,
    InfoOp,
    FlushOp,
    QueryOp,
    EventOp,
    Performer,
    TaskDoneOp,
    ReadBlockOp,
    WriteBlockOp,
    DeleteBlockOp,
    IterBlocksItemOp,
    IterBlocksFinishOp,
    IterBlocksState,
    InterpretTask,
    DefragConfig,
    PerformerBuilderInit,
    Context as BaseContext,
    super::{
        storage,
    },
};

use crate::Info;

mod basic;
mod defrag;
mod defrag_disturb;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
struct Context;

type C = &'static str;

impl BaseContext for Context {
    type Info = C;
    type Flush = C;
    type WriteBlock = C;
    type ReadBlock = C;
    type DeleteBlock = C;
    type IterBlocks = C;
    type IterBlocksStream = C;
    type Interpreter = C;
}

fn init() -> Performer<Context> {
    with_defrag_config(None)
}

fn with_defrag_config(defrag_config: Option<DefragConfig<C>>) -> Performer<Context> {
    let (performer_builder, _work_block) = PerformerBuilderInit::new(
        lru::Cache::new(16),
        BytesPool::new(),
        defrag_config,
        1024,
    )
        .unwrap()
        .start_fill();
    performer_builder.finish(160)
}

fn hello_world_write_req(context: C) -> proto::RequestWriteBlock<C> {
    let block_bytes = hello_world_bytes().freeze();
    let block_crc = block::crc(&block_bytes);
    proto::RequestWriteBlock { block_bytes, block_crc, context, }
}

fn hello_world_read_done(block_id: block::Id, context: C) -> task::TaskDone<Context> {
    let block_bytes = hello_world_bytes().freeze();
    let block_crc = block::crc(&block_bytes);
    task::TaskDone {
        block_id,
        kind: task::TaskDoneKind::ReadBlock(task::TaskDoneReadBlock {
            block_bytes,
            block_crc,
            context: task::ReadBlockContext::External(context),
        }),
    }
}

fn hello_world_bytes() -> BytesMut {
    let mut block_bytes_mut = BytesMut::new_detached(Vec::new());
    block_bytes_mut.extend("hello, world!".as_bytes().iter().cloned());
    block_bytes_mut
}

fn hello_bytes() -> BytesMut {
    let mut block_bytes_mut = BytesMut::new_detached(Vec::new());
    block_bytes_mut.extend("hello!".as_bytes().iter().cloned());
    block_bytes_mut
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
    MakeIterBlocksStream,
    InterpretTask { expect_offset: u64, expect_task: ExpectTask, },
    InfoSuccess { expect_info: Info, expect_context: C, },
    FlushSuccess { expect_context: C, },
    WriteBlockNoSpaceLeft { expect_context: C, },
    WriteBlockDone { expect_block_id: block::Id, expect_context: C, },
    ReadBlockNotFound { expect_context: C, },
    ReadBlockDone { expect_block_bytes: Bytes, expect_context: C, },
    DeleteBlockNotFound { expect_context: C, },
    DeleteBlockDone { expect_block_id: block::Id, expect_context: C, },
    IterBlocksItem { expect_block_id: block::Id, expect_block_bytes: Bytes, expect_context: C, },
    IterBlocksFinish { expect_context: C, },
}

#[allow(dead_code)]
#[derive(Debug)]
enum DoOp {
    RequestAndInterpreterIncomingRequest { request: proto::Request<Context>, interpreter_context: C, },
    RequestAndInterpreterIncomingTaskDone { task_done: task::Done<Context>, },
    RequestAndInterpreterIncomingIterBlocks { iter_blocks_state: IterBlocksState<C>, interpreter_context: C, },
    RequestIncomingRequest { request: proto::Request<Context>, },
    RequestIncomingIterBlocks { iter_blocks_state: IterBlocksState<C>, },
    TaskAccept { interpreter_context: C, },
    StreamReady { iter_context: C, },
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
    block_bytes: Bytes,
    context: task::WriteBlockContext<C>,
}

#[derive(Debug)]
struct ExpectTaskReadBlock {
    block_header: storage::BlockHeader,
    context: task::ReadBlockContext<Context>,
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
                            Some(ScriptOp::Do(DoOp::RequestAndInterpreterIncomingIterBlocks { iter_blocks_state, interpreter_context, })) =>
                                poll.next.incoming_iter_blocks(iter_blocks_state, interpreter_context),
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
                            Some(ScriptOp::Do(DoOp::RequestIncomingIterBlocks { iter_blocks_state, })) =>
                                poll.next.incoming_iter_blocks(iter_blocks_state),
                            Some(other_op) =>
                                panic!("expected DoOp::RequestIncoming* but got {:?} @ {}", other_op, script_len - script.len()),
                        },
                    Some(other_op) =>
                        panic!("expecting exact ExpectOp::PollRequest for PollRequest but got {:?} @ {}", other_op, script_len - script.len()),
                },

            Op::Query(QueryOp::MakeIterBlocksStream(make_iter_block_stream)) =>
                match script.pop() {
                    None =>
                        panic!(
                            "unexpected script end on MakeIterBlocksStream, expecting ExpectOp::MakeIterBlocksStream @ {}",
                            script_len - script.len(),
                        ),
                    Some(ScriptOp::Expect(ExpectOp::MakeIterBlocksStream)) =>
                        match script.pop() {
                            None =>
                                panic!(
                                    "unexpected script end on ExpectOp::MakeIterBlocksStream, expecting DoOp::StreamReady @ {}",
                                    script_len - script.len(),
                                ),
                            Some(ScriptOp::Do(DoOp::StreamReady { iter_context, })) =>
                                make_iter_block_stream.next.stream_ready(iter_context),
                            Some(other_op) =>
                                panic!("expected DoOp::StreamReady but got {:?} @ {}", other_op, script_len - script.len()),
                        },
                    Some(other_op) =>
                        panic!(
                            "expecting exact ExpectOp::MakeIterBlocksStream for MakeIterBlocksStream but got {:?} @ {}",
                            other_op, script_len - script.len(),
                        ),
                },

            Op::Query(QueryOp::InterpretTask(InterpretTask { offset, task, next, })) =>
                match script.pop() {
                    None =>
                        panic!(
                            "unexpected script end on InterpretTask, expecting ExpectOp::InterpretTask {{ offset: {}, task: {:?}, }} @ {}",
                            offset, task, script_len - script.len(),
                        ),
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
                        panic!(
                            "expecting exact ExpectOp::InterpretTask {{ expect_offset = {}, expect_task = {:?} }} for InterpretTask but got {:?} @ {}",
                            offset,
                            task,
                            other_op,
                            script_len - script.len(),
                        ),
                },

            Op::Event(Event { op: EventOp::Info(TaskDoneOp { context, op: InfoOp::Success { info, }, }), performer, }) =>
                match script.pop() {
                    None =>
                        panic!("unexpected script end on InfoOp::Success, expecting ExpectOp::InfoSuccess @ {}", script_len - script.len()),
                    Some(ScriptOp::Expect(ExpectOp::InfoSuccess { expect_info, expect_context, }))
                        if expect_info == info && expect_context == context =>
                        performer.next(),
                    Some(other_op) =>
                        panic!(
                            "expecting exact ExpectOp::InfoSuccess {{ info: {:?}, }} for InfoOp::Success but got {:?} @ {}",
                            info, other_op, script_len - script.len(),
                        ),
                },

            Op::Event(Event { op: EventOp::Flush(TaskDoneOp { context, op: FlushOp::Flushed, }), performer, }) =>
                match script.pop() {
                    None =>
                        panic!("unexpected script end on FlushOp::Success, expecting ExpectOp::FlushSuccess @ {}", script_len - script.len()),
                    Some(ScriptOp::Expect(ExpectOp::FlushSuccess { expect_context, })) if expect_context == context =>
                        performer.next(),
                    Some(other_op) =>
                        panic!(
                            "expecting exact ExpectOp::FlushSuccess for FlushOp::Flushed but got {:?} @ {}",
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

            Op::Event(Event { op: EventOp::DeleteBlock(TaskDoneOp { context, op: DeleteBlockOp::Done { block_id, }, }), performer, }) =>
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

            Op::Event(Event {
                op: EventOp::IterBlocksItem(IterBlocksItemOp {
                    block_id,
                    block_bytes,
                    iter_blocks_state: IterBlocksState { iter_blocks_stream_context, .. },
                }),
                performer,
            }) =>
                match script.pop() {
                    None =>
                        panic!(
                            "unexpected script end on IterBlocksItemOp, expecting ExpectOp::IterBlocksItem @ {}",
                            script_len - script.len(),
                        ),
                    Some(ScriptOp::Expect(ExpectOp::IterBlocksItem { expect_block_id, expect_block_bytes, expect_context, }))
                        if expect_block_id == block_id && expect_block_bytes == block_bytes && expect_context == iter_blocks_stream_context =>
                        performer.next(),
                    Some(other_op) =>
                        panic!(
                            "expecting exact ExpectOp::IterBlocksItem for IterBlocksItemOp but got {:?} @ {}",
                            other_op, script_len - script.len(),
                        ),
                },

            Op::Event(Event { op: EventOp::IterBlocksFinish(IterBlocksFinishOp { iter_blocks_stream_context, }, ), performer, }) =>
                match script.pop() {
                    None =>
                        panic!(
                            "unexpected script end on IterBlocksFinishOp, expecting ExpectOp::IterBlocksFinish @ {}",
                            script_len - script.len(),
                        ),
                    Some(ScriptOp::Expect(ExpectOp::IterBlocksFinish { expect_context, })) if expect_context == iter_blocks_stream_context =>
                        performer.next(),
                    Some(other_op) =>
                        panic!(
                            "expecting exact ExpectOp::IterBlocksFinish for IterBlocksFinishOp but got {:?} @ {}",
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
            && self.context == task.context
    }
}

impl PartialEq<task::ReadBlock<Context>> for ExpectTaskReadBlock {
    fn eq(&self, task: &task::ReadBlock<Context>) -> bool {
        self.block_header == task.block_header
            && self.context == task.context
    }
}

impl PartialEq<task::DeleteBlock<C>> for ExpectTaskDeleteBlock {
    fn eq(&self, task: &task::DeleteBlock<C>) -> bool {
        self.context == task.context
    }
}
