use std::{
    collections::{
        HashMap,
    },
};

use alloc_pool::{
    bytes::{
        Bytes,
        BytesMut,
    },
};

use crate::{
    block,
    proto,
    storage,
    context::{
        Context as BaseContext,
    },
    wheel::{
        lru,
        core::{
            task,
            performer::{
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
                IterBlocksInitOp,
                IterBlocksNextOp,
                PrepareInterpretTaskOp,
                PrepareInterpretTaskKind,
                PrepareInterpretTaskWriteBlock,
                PrepareInterpretTaskDeleteBlock,
                ProcessReadBlockTaskDoneOp,
                InterpretTask,
                DefragConfig,
                PerformerBuilderInit,
            },
        },
    },
    Info,
    IterBlocks,
    IterBlocksItem,
};

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
    type IterBlocksInit = C;
    type IterBlocksNext = C;
}

fn init() -> Performer<Context> {
    with_defrag_config(None)
}

fn with_defrag_config(defrag_config: Option<DefragConfig<C>>) -> Performer<Context> {
    let (performer_builder, _work_block) = PerformerBuilderInit::new(
        lru::Cache::new(16),
        defrag_config,
        1024,
    )
        .unwrap()
        .start_fill();
    performer_builder.finish(160)
}

fn hello_world_write_req(context: C) -> proto::RequestWriteBlock<C> {
    let block_bytes = hello_world_bytes().freeze();
    proto::RequestWriteBlock { block_bytes, context, }
}

fn hello_world_read_done(block_id: block::Id, context: C) -> task::TaskDone<Context> {
    let block_bytes = hello_world_bytes();
    task::TaskDone {
        block_id,
        kind: task::TaskDoneKind::ReadBlock(task::TaskDoneReadBlock {
            block_bytes,
            context: task::ReadBlockContext::Process(task::ReadBlockProcessContext::External(context)),
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
    PollRequestAndInterpreter,
    InterpretTask { expect_offset: u64, expect_task: ExpectTask, },
    InfoSuccess { expect_info: Info, expect_context: C, },
    FlushSuccess { expect_context: C, },
    WriteBlockNoSpaceLeft { expect_context: C, },
    WriteBlockDone { expect_block_id: block::Id, expect_context: C, },
    ReadBlockNotFound { expect_context: C, },
    ReadBlockDone { expect_block_bytes: Bytes, expect_context: C, },
    DeleteBlockNotFound { expect_context: C, },
    DeleteBlockDone { expect_block_id: block::Id, expect_context: C, },
    IterBlocksInit { expect_iter_blocks: IterBlocks, expect_context: C, },
    IterBlocksNext { expect_item: IterBlocksItem, expect_context: C, },
    PrepareInterpretTaskWriteBlock { expect_block_id: block::Id, expect_block_bytes: Bytes, expect_context: task::WriteBlockContext<C>, },
    PrepareInterpretTaskDeleteBlock { expect_block_id: block::Id, expect_context: task::DeleteBlockContext<C>, },
    ProcessReadBlockTaskDone { expect_block_id: block::Id, expect_block_bytes: Bytes, expect_pending_contexts_key: &'static str, },
}

#[derive(Debug)]
enum DoOp {
    RequestAndInterpreterIncomingRequest { request: proto::Request<Context>, },
    RequestAndInterpreterIncomingTaskDone { task_done: task::Done<Context>, },
    RequestAndInterpreterIncomingPreparedWriteBlockDone {
        block_id: block::Id,
        write_block_bytes: BytesMut,
        context: task::WriteBlockContext<C>,
    },
    RequestAndInterpreterIncomingPreparedDeleteBlockDone {
        block_id: block::Id,
        delete_block_bytes: BytesMut,
        context: task::DeleteBlockContext<C>,
    },
    // // temporarily not used anywhere
    // RequestAndInterpreterIncomingProcessReadBlockDone {
    //     block_id: block::Id,
    //     block_bytes: Bytes,
    //     pending_contexts_key: &'static str,
    // },
    RequestIncomingRequest { request: proto::Request<Context>, },
    RequestIncomingPreparedWriteBlockDone { block_id: block::Id, write_block_bytes: BytesMut, context: task::WriteBlockContext<C>, },
    RequestIncomingPreparedDeleteBlockDone { block_id: block::Id, delete_block_bytes: BytesMut, context: task::DeleteBlockContext<C>, },
    RequestIncomingProcessReadBlockDone {
        block_id: block::Id,
        block_bytes: Bytes,
        pending_contexts_key: &'static str,
    },
    TaskAccept,
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
    write_block_bytes: Bytes,
    commit: task::Commit,
    context: task::WriteBlockContext<C>,
}

#[derive(Debug)]
struct ExpectTaskReadBlock {
    block_header: storage::BlockHeader,
    context: task::ReadBlockContext<Context>,
}

#[derive(Debug)]
struct ExpectTaskDeleteBlock {
    delete_block_bytes: Bytes,
    commit: task::Commit,
    context: task::DeleteBlockContext<C>,
}

fn interpret(performer: Performer<Context>, mut script: Vec<ScriptOp>) {
    let script_len = script.len();
    script.reverse();

    let mut pending_contexts_table: HashMap<&'static str, task::queue::PendingReadContextBag> = HashMap::new();

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
                    Some(ScriptOp::Expect(ExpectOp::PollRequestAndInterpreter)) =>
                        match script.pop() {
                            None =>
                                break,
                            Some(ScriptOp::Do(DoOp::RequestAndInterpreterIncomingRequest { request, })) =>
                                poll.next.incoming_request(request),
                            Some(ScriptOp::Do(DoOp::RequestAndInterpreterIncomingTaskDone { task_done, })) =>
                                poll.next.incoming_task_done(task_done),
                            Some(ScriptOp::Do(DoOp::RequestAndInterpreterIncomingPreparedWriteBlockDone {
                                block_id,
                                write_block_bytes,
                                context,
                            })) =>
                                poll.next.prepared_write_block_done(
                                    block_id,
                                    task::WriteBlockBytes::Chunk(write_block_bytes.freeze()),
                                    context,
                                ),
                            // // temporarily not used anywhere
                            // Some(ScriptOp::Do(DoOp::RequestAndInterpreterIncomingProcessReadBlockDone {
                            //     block_id,
                            //     block_bytes,
                            //     pending_contexts_key,
                            // })) => {
                            //     let pending_contexts = pending_contexts_table.remove(pending_contexts_key).unwrap();
                            //     poll.next.process_read_block_done(block_id, block_bytes, pending_contexts)
                            // },
                            Some(ScriptOp::Do(DoOp::RequestAndInterpreterIncomingPreparedDeleteBlockDone {
                                block_id,
                                delete_block_bytes,
                                context,
                            })) =>
                                poll.next.prepared_delete_block_done(block_id, delete_block_bytes, context),
                            Some(other_op) =>
                                panic!(
                                    "expected DoOp::RequestAndInterpreterIncoming* but got {:?} @ {}",
                                    other_op,
                                    script_len - script.len(),
                                ),
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
                            Some(ScriptOp::Do(DoOp::RequestIncomingPreparedWriteBlockDone { block_id, write_block_bytes, context, })) =>
                                poll.next.prepared_write_block_done(
                                    block_id,
                                    task::WriteBlockBytes::Chunk(write_block_bytes.freeze()),
                                    context,
                                ),
                            Some(ScriptOp::Do(DoOp::RequestIncomingProcessReadBlockDone { block_id, block_bytes, pending_contexts_key, })) => {
                                let pending_contexts = pending_contexts_table.remove(pending_contexts_key).unwrap();
                                poll.next.process_read_block_done(block_id, block_bytes, pending_contexts)
                            },
                            Some(ScriptOp::Do(DoOp::RequestIncomingPreparedDeleteBlockDone { block_id, delete_block_bytes, context, })) =>
                                poll.next.prepared_delete_block_done(block_id, delete_block_bytes, context),
                            Some(other_op) =>
                                panic!("expected DoOp::RequestIncoming* but got {:?} @ {}", other_op, script_len - script.len()),
                        },
                    Some(other_op) =>
                        panic!("expecting exact ExpectOp::PollRequest for PollRequest but got {:?} @ {}", other_op, script_len - script.len()),
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
                            Some(ScriptOp::Do(DoOp::TaskAccept)) => {
                                let performer = next.task_accepted();
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
                    Some(ScriptOp::Expect(ExpectOp::ReadBlockDone { expect_block_bytes, expect_context, })) =>
                        panic!(
                            "expecting exact ExpectOp::ReadBlockDone for ReadBlockOp::Done but got ExpectOp::ReadBlockDone @ {}; {:?}/{:?}",
                            script_len - script.len(),
                            expect_block_bytes == block_bytes,
                            expect_context == context,
                        ),
                    Some(other_op) =>
                        panic!(
                            "expecting exact ExpectOp::ReadBlockDone for ReadBlockOp::Done but got {:?} @ {}",
                            other_op,
                            script_len - script.len(),
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

            Op::Event(Event { op: EventOp::IterBlocksInit(IterBlocksInitOp { iter_blocks, iter_blocks_init_context, }), performer, }) =>
                match script.pop() {
                    None =>
                        panic!(
                            "unexpected script end on EventOp::IterBlocksInit, expecting ExpectOp::IterBlocksInit @ {}",
                            script_len - script.len(),
                        ),
                    Some(ScriptOp::Expect(ExpectOp::IterBlocksInit { expect_iter_blocks, expect_context, }))
                        if expect_iter_blocks == iter_blocks && expect_context == iter_blocks_init_context =>
                        performer.next(),
                    Some(other_op) =>
                        panic!(
                            "expecting exact ExpectOp::IterBlocksInit with {:?} for ExpectOp::IterBlocksInit but got {:?} @ {}",
                            iter_blocks, other_op, script_len - script.len(),
                        ),
                },

            Op::Event(Event { op: EventOp::IterBlocksNext(IterBlocksNextOp { item, iter_blocks_next_context, }), performer, }) =>
                match script.pop() {
                    None =>
                        panic!(
                            "unexpected script end on EventOp::IterBlocksNext, expecting ExpectOp::IterBlocksNext @ {}",
                            script_len - script.len(),
                        ),
                    Some(ScriptOp::Expect(ExpectOp::IterBlocksNext { expect_item, expect_context, }))
                        if expect_item == item && expect_context == iter_blocks_next_context =>
                        performer.next(),
                    Some(other_op) =>
                        panic!(
                            "expecting exact ExpectOp::IterBlocksNext for ExpectOp::IterBlocksNext but got {:?} @ {}",
                            other_op, script_len - script.len(),
                        ),
                },

            Op::Event(Event {
                op: EventOp::PrepareInterpretTask(PrepareInterpretTaskOp {
                    block_id,
                    task: PrepareInterpretTaskKind::WriteBlock(PrepareInterpretTaskWriteBlock {
                        block_bytes,
                        context,
                    }),
                }),
                performer,
            }) =>
                match script.pop() {
                    None =>
                        panic!(
                            "unexpected script end on PrepareInterpretTaskOp/WriteBlock, expecting ExpectOp::PrepareInterpretTaskWriteBlock @ {}",
                            script_len - script.len(),
                        ),
                    Some(ScriptOp::Expect(ExpectOp::PrepareInterpretTaskWriteBlock { expect_block_id, expect_block_bytes, expect_context, }))
                        if expect_block_id == block_id && expect_block_bytes == block_bytes && expect_context == context =>
                        performer.next(),
                    Some(other_op) =>
                        panic!(
                            "expecting exact ExpectOp::PrepareInterpretTaskWriteBlock for PrepareInterpretTaskOp/WriteBlock but got {:?} @ {}",
                            other_op, script_len - script.len(),
                        ),
                },

            Op::Event(Event {
                op: EventOp::PrepareInterpretTask(PrepareInterpretTaskOp {
                    block_id,
                    task: PrepareInterpretTaskKind::DeleteBlock(PrepareInterpretTaskDeleteBlock {
                        context,
                    }),
                }),
                performer,
            }) =>
                match script.pop() {
                    None =>
                        panic!(
                            "unexpected script end on PrepareInterpretTaskOp/DeleteBlock, expecting ExpectOp::PrepareInterpretTaskDeleteBlock @ {}",
                            script_len - script.len(),
                        ),
                    Some(ScriptOp::Expect(ExpectOp::PrepareInterpretTaskDeleteBlock { expect_block_id, expect_context, }))
                        if expect_block_id == block_id && expect_context == context =>
                        performer.next(),
                    Some(other_op) =>
                        panic!(
                            "expecting exact ExpectOp::PrepareInterpretTaskDeleteBlock for PrepareInterpretTaskOp/DeleteBlock/{:?} but got {:?} @ {}",
                            context, other_op, script_len - script.len(),
                        ),
                },

            Op::Event(Event {
                op: EventOp::ProcessReadBlockTaskDone(ProcessReadBlockTaskDoneOp {
                    block_header,
                    block_bytes,
                    pending_contexts,
                    ..
                }),
                performer,
            }) =>
                match script.pop() {
                    None =>
                        panic!(
                            "unexpected script end on ProcessReadBlockTaskDoneOp, expecting ExpectOp::ProcessReadBlockTaskDone @ {}",
                            script_len - script.len(),
                        ),
                    Some(ScriptOp::Expect(ExpectOp::ProcessReadBlockTaskDone { expect_block_id, expect_block_bytes, expect_pending_contexts_key, }))
                        if expect_block_id == block_header.block_id && expect_block_bytes == block_bytes => {
                            let prev = pending_contexts_table.insert(expect_pending_contexts_key, pending_contexts);
                            assert_eq!(prev, None);
                            performer.next()
                        },
                    Some(other_op) =>
                        panic!(
                            "expecting exact ExpectOp::ProcessReadBlockTaskDone for ProcessReadBlockTaskDoneOp but got {:?} @ {}",
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
        match &task.write_block_bytes {
            task::WriteBlockBytes::Composite(..) =>
                false,
            task::WriteBlockBytes::Chunk(write_block_bytes) => {
                &self.write_block_bytes == write_block_bytes
                    && self.commit == task.commit
                    && self.context == task.context
            },
        }
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
        self.delete_block_bytes == task.delete_block_bytes
            && self.commit == task.commit
            && self.context == task.context
    }
}
