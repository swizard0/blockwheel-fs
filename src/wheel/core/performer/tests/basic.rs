use crate::{
    proto,
    block,
    storage,
    wheel::{
        core::{
            task,
            performer::{
                tests::{
                    init,
                    interpret,
                    hello_world_bytes,
                    hello_world_write_req,
                    hello_world_read_done,
                    Info,
                    ScriptOp,
                    ExpectOp,
                    DoOp,
                    ExpectTask,
                    ExpectTaskKind,
                    ExpectTaskWriteBlock,
                    ExpectTaskReadBlock,
                    ExpectTaskDeleteBlock,
                },
            },
        },
    },
    InterpretStats,
    IterBlocks,
    IterBlocksItem,
    IterBlocksIterator,
};

#[test]
fn script_basic() {
    let performer = init();
    let script = vec![
        // { }
        ScriptOp::Expect(ExpectOp::PollRequest),
        ScriptOp::Do(DoOp::RequestIncomingRequest {
            request: proto::Request::ReadBlock(proto::RequestReadBlock { block_id: block::Id::init(), context: "ectx01", }),
        }),
        ScriptOp::Expect(ExpectOp::ReadBlockNotFound {
            expect_context: "ectx01",
        }),
        ScriptOp::Expect(ExpectOp::PollRequest),
        // { 0: write req }
        ScriptOp::Do(DoOp::RequestIncomingRequest {
            request: proto::Request::WriteBlock(hello_world_write_req("ectx02")),
        }),
        // { 0: prep write }
        ScriptOp::Expect(ExpectOp::PrepareInterpretTaskWriteBlock {
            expect_block_id: block::Id::init(),
            expect_block_bytes: hello_world_bytes().freeze(),
            expect_context: task::WriteBlockContext::External("ectx02"),
        }),
        ScriptOp::Expect(ExpectOp::PollRequest),
        // { 0: prep write done }
        ScriptOp::Do(DoOp::RequestIncomingPreparedWriteBlockDone {
            block_id: block::Id::init(),
            write_block_bytes: hello_world_bytes(),
            context: task::WriteBlockContext::External("ectx02"),
        }),
        ScriptOp::Expect(ExpectOp::Idle),
        // { 0: write task in progress @ 18 }
        ScriptOp::Expect(ExpectOp::InterpretTask {
            expect_offset: 18,
            expect_task: ExpectTask {
                block_id: block::Id::init(),
                kind: ExpectTaskKind::WriteBlock(ExpectTaskWriteBlock {
                    write_block_bytes: hello_world_bytes().freeze(),
                    commit: task::Commit::WithTerminator,
                    context: task::WriteBlockContext::External("ectx02"),
                }),
            },
        }),
        ScriptOp::Do(DoOp::TaskAccept),
        ScriptOp::Expect(ExpectOp::PollRequestAndInterpreter),
        // { 0: write task in progress @ 18, 0: read req }
        ScriptOp::Do(DoOp::RequestAndInterpreterIncomingRequest {
            request: proto::Request::ReadBlock(proto::RequestReadBlock { block_id: block::Id::init(), context: "ectx03", }),
        }),
        ScriptOp::Expect(ExpectOp::Idle),
        ScriptOp::Expect(ExpectOp::PollRequestAndInterpreter),
        // { 0: write task in progress @ 18, 0: read req, 0: delete req }
        ScriptOp::Do(DoOp::RequestAndInterpreterIncomingRequest {
            request: proto::Request::DeleteBlock(proto::RequestDeleteBlock { block_id: block::Id::init(), context: "ectx04", }),
        }),
        // { 0: write task in progress @ 18, 0: read req, 0: prep delete }
        ScriptOp::Expect(ExpectOp::PrepareInterpretTaskDeleteBlock {
            expect_block_id: block::Id::init(),
            expect_context: task::DeleteBlockContext::External("ectx04"),
        }),
        ScriptOp::Expect(ExpectOp::PollRequestAndInterpreter),
        // { 0: write task in progress @ 18, 0: read req, 0: prep delete, 1: write req }
        ScriptOp::Do(DoOp::RequestAndInterpreterIncomingRequest {
            request: proto::Request::WriteBlock(hello_world_write_req("ectx05")),
        }),
        // { 0: write task in progress @ 18, 0: read req, 0: prep delete, 1: prep write }
        ScriptOp::Expect(ExpectOp::PrepareInterpretTaskWriteBlock {
            expect_block_id: block::Id::init().next(),
            expect_block_bytes: hello_world_bytes().freeze(),
            expect_context: task::WriteBlockContext::External("ectx05"),
        }),
        ScriptOp::Expect(ExpectOp::PollRequestAndInterpreter),
        // { 0: write task in progress @ 18, 0: read req, 0: prep delete done, 1: prep write }
        ScriptOp::Do(DoOp::RequestAndInterpreterIncomingPreparedDeleteBlockDone {
            block_id: block::Id::init(),
            delete_block_bytes: hello_world_bytes(),
            context: task::DeleteBlockContext::External("ectx04"),
        }),
        ScriptOp::Expect(ExpectOp::Idle),
        ScriptOp::Expect(ExpectOp::PollRequestAndInterpreter),
        // { 0: write task in progress @ 18, 0: read req, 0: prep delete done, 1: prep write done }
        ScriptOp::Do(DoOp::RequestAndInterpreterIncomingPreparedWriteBlockDone {
            block_id: block::Id::init().next(),
            write_block_bytes: hello_world_bytes(),
            context: task::WriteBlockContext::External("ectx05"),
        }),
        ScriptOp::Expect(ExpectOp::Idle),
        ScriptOp::Expect(ExpectOp::PollRequestAndInterpreter),
        // { 0: write task done @ 18 .. 79, 0: read req, 0: prep delete done, 1: prep write done }
        ScriptOp::Do(DoOp::RequestAndInterpreterIncomingTaskDone {
            task_done: task::Done {
                current_offset: 79,
                task: task::TaskDone {
                    block_id: block::Id::init(),
                    kind: task::TaskDoneKind::WriteBlock(task::TaskDoneWriteBlock {
                        context: task::WriteBlockContext::External("ectx02"),
                    }),
                },
            },
        }),
        ScriptOp::Expect(ExpectOp::WriteBlockDone {
            expect_block_id: block::Id::init(),
            expect_context: "ectx02",
        }),
        // { 0: ready @ 18 .. 79, 0: read req, 0: prep delete done, 1: write task in progress @ 79 }
        ScriptOp::Expect(ExpectOp::InterpretTask {
            expect_offset: 79,
            expect_task: ExpectTask {
                block_id: block::Id::init().next(),
                kind: ExpectTaskKind::WriteBlock(ExpectTaskWriteBlock {
                    write_block_bytes: hello_world_bytes().freeze(),
                    commit: task::Commit::WithTerminator,
                    context: task::WriteBlockContext::External("ectx05"),
                }),
            },
        }),
        ScriptOp::Do(DoOp::TaskAccept),
        ScriptOp::Expect(ExpectOp::PollRequestAndInterpreter),
        // { 0: ready @ 18 .. 79, 0: read req, 0: prep delete done, 1: write task done @ 79 .. 124 }
        ScriptOp::Do(DoOp::RequestAndInterpreterIncomingTaskDone {
            task_done: task::Done {
                current_offset: 124,
                task: task::TaskDone {
                    block_id: block::Id::init().next(),
                    kind: task::TaskDoneKind::WriteBlock(task::TaskDoneWriteBlock {
                        context: task::WriteBlockContext::External("ectx05"),
                    }),
                },
            },
        }),
        ScriptOp::Expect(ExpectOp::WriteBlockDone {
            expect_block_id: block::Id::init().next(),
            expect_context: "ectx05",
        }),
        // { 0: ready @ 18 .. 79, 0: read task in progress @ 24, 0: prep delete done, 1: write task done @ 79 .. 124 }
        ScriptOp::Expect(ExpectOp::InterpretTask {
            expect_offset: 18,
            expect_task: ExpectTask {
                block_id: block::Id::init(),
                kind: ExpectTaskKind::ReadBlock(ExpectTaskReadBlock {
                    block_header: storage::BlockHeader {
                        block_id: block::Id::init(),
                        block_size: 13,
                        ..Default::default()
                    },
                    context: task::ReadBlockContext::Process(task::ReadBlockProcessContext::External("ectx03")),
                }),
            },
        }),
        ScriptOp::Do(DoOp::TaskAccept),
        ScriptOp::Expect(ExpectOp::PollRequestAndInterpreter),
        // { 0: ready @ 18 .. 79, 0: read task done @ 18 .. 79, 0: prep delete done, 1: ready @ 79 .. 124 }
        ScriptOp::Do(DoOp::RequestAndInterpreterIncomingTaskDone {
            task_done: task::Done {
                current_offset: 79,
                task: hello_world_read_done(block::Id::init(), "ectx03"),
            },
        }),
        ScriptOp::Expect(ExpectOp::Idle),
        ScriptOp::Expect(ExpectOp::Idle),
        // { 0: ready @ 18 .. 79, 0: read task done process @ 18 .. 79, 0: prep delete done, 1: ready @ 79 .. 124 }
        ScriptOp::Expect(ExpectOp::ProcessReadBlockTaskDone {
            expect_block_id: block::Id::init(),
            expect_block_bytes: hello_world_bytes().freeze(),
            expect_pending_contexts_key: "pk0",
        }),
        // { 0: ready @ 18 .. 79, 0: read task done process @ 18 .. 79, 0: delete task in progress @ 18, 1: ready @ 79 .. 124 }
        ScriptOp::Expect(ExpectOp::InterpretTask {
            expect_offset: 18,
            expect_task: ExpectTask {
                block_id: block::Id::init(),
                kind: ExpectTaskKind::DeleteBlock(ExpectTaskDeleteBlock {
                    delete_block_bytes: hello_world_bytes().freeze(),
                    commit: task::Commit::None,
                    context: task::DeleteBlockContext::External("ectx04"),
                }),
            },
        }),
        ScriptOp::Do(DoOp::TaskAccept),
        ScriptOp::Expect(ExpectOp::PollRequestAndInterpreter),
        // { 0: ready @ 18 .. 79, 0: read task done process @ 18 .. 79, 0: delete task in progress @ 18, 0: read req, 1: ready @ 79 .. 124 }
        ScriptOp::Do(DoOp::RequestAndInterpreterIncomingRequest {
            request: proto::Request::ReadBlock(proto::RequestReadBlock { block_id: block::Id::init(), context: "ectx06", }),
        }),
        ScriptOp::Expect(ExpectOp::Idle),
        ScriptOp::Expect(ExpectOp::PollRequestAndInterpreter),
        // { 0: ready @ 18 .. 79, 0: read task done process @ 18 .. 79, 0: delete task in progress @ 18, 0: read req, 0: delete req,
        //   1: ready @ 79 .. 124 }
        ScriptOp::Do(DoOp::RequestAndInterpreterIncomingRequest {
            request: proto::Request::DeleteBlock(proto::RequestDeleteBlock { block_id: block::Id::init(), context: "ectx07", }),
        }),
        // { 0: ready @ 18 .. 79, 0: read task done process @ 18 .. 79, 0: delete task in progress @ 18, 0: read req, 0: prep delete,
        //   1: ready @ 79 .. 124 }
        ScriptOp::Expect(ExpectOp::PrepareInterpretTaskDeleteBlock {
            expect_block_id: block::Id::init(),
            expect_context: task::DeleteBlockContext::External("ectx07"),
        }),
        ScriptOp::Expect(ExpectOp::PollRequestAndInterpreter),
        // { 0: ready @ 18 .. 79, 0: read task done process @ 18 .. 79, 0: delete task in progress @ 18, 0: read req, 0: prep delete done,
        //   1: ready @ 79 .. 124 }
        ScriptOp::Do(DoOp::RequestAndInterpreterIncomingPreparedDeleteBlockDone {
            block_id: block::Id::init(),
            delete_block_bytes: hello_world_bytes(),
            context: task::DeleteBlockContext::External("ectx07"),
        }),
        ScriptOp::Expect(ExpectOp::Idle),
        ScriptOp::Expect(ExpectOp::PollRequestAndInterpreter),
        // { 0: ready @ 18 .. 79, 0: read task done process @ 18 .. 79, 0: delete task done @ 18, 0: read req, 0: prep delete done,
        //   1: ready @ 79 .. 124 }
        ScriptOp::Do(DoOp::RequestAndInterpreterIncomingTaskDone {
            task_done: task::Done {
                current_offset: 18,
                task: task::TaskDone {
                    block_id: block::Id::init(),
                    kind: task::TaskDoneKind::DeleteBlock(task::TaskDoneDeleteBlock {
                        context: task::DeleteBlockContext::External("ectx04"),
                    }),
                },
            },
        }),
        ScriptOp::Expect(ExpectOp::DeleteBlockDone {
            expect_block_id: block::Id::init(),
            expect_context: "ectx04",
        }),
        // { 0: ready @ 18 .. 79, 0: read task done process @ 18 .. 79, 0: delete task done @ 18, 0: prep delete done,
        //   1: ready @ 79 .. 124 }
        ScriptOp::Expect(ExpectOp::ReadBlockNotFound {
            expect_context: "ectx06",
        }),
        // { 0: ready @ 18 .. 79, 0: read task done process @ 18 .. 79, 0: delete task done @ 18, 1: ready @ 79 .. 124 }
        ScriptOp::Expect(ExpectOp::DeleteBlockNotFound {
            expect_context: "ectx07",
        }),
        // { 0: read task done process @ 18 .. 79, 1: ready @ 79 .. 124 }
        ScriptOp::Expect(ExpectOp::PollRequest),
        // { 0: read task done process done @ 18 .. 79, 1: ready @ 79 .. 124 }
        ScriptOp::Do(DoOp::RequestIncomingProcessReadBlockDone {
            block_id: block::Id::init(),
            block_bytes: hello_world_bytes().freeze(),
            pending_contexts_key: "pk0",
        }),
        ScriptOp::Expect(ExpectOp::Idle),
        // { 1: ready @ 79 .. 124 }
        ScriptOp::Expect(ExpectOp::ReadBlockNotFound {
            expect_context: "ectx03",
        }),
        ScriptOp::Expect(ExpectOp::PollRequest),
        // { 1: ready @ 79 .. 124, 2: write req }
        ScriptOp::Do(DoOp::RequestIncomingRequest {
            request: proto::Request::WriteBlock(hello_world_write_req("ectx08")),
        }),
        // { 1: ready @ 79 .. 124, 2: prep write }
        ScriptOp::Expect(ExpectOp::PrepareInterpretTaskWriteBlock {
            expect_block_id: block::Id::init().next().next(),
            expect_block_bytes: hello_world_bytes().freeze(),
            expect_context: task::WriteBlockContext::External("ectx08"),
        }),
        ScriptOp::Expect(ExpectOp::PollRequest),
        // { 1: ready @ 79 .. 124, 2: prep write done }
        ScriptOp::Do(DoOp::RequestIncomingPreparedWriteBlockDone {
            block_id: block::Id::init().next().next(),
            write_block_bytes: hello_world_bytes(),
            context: task::WriteBlockContext::External("ectx08"),
        }),
        ScriptOp::Expect(ExpectOp::Idle),
        // { 1: ready @ 79 .. 124, 2: write task in progress @ 18 }
        ScriptOp::Expect(ExpectOp::InterpretTask {
            expect_offset: 18,
            expect_task: ExpectTask {
                block_id: block::Id::init().next().next(),
                kind: ExpectTaskKind::WriteBlock(ExpectTaskWriteBlock {
                    write_block_bytes: hello_world_bytes().freeze(),
                    commit: task::Commit::None,
                    context: task::WriteBlockContext::External("ectx08"),
                }),
            },
        }),
        ScriptOp::Do(DoOp::TaskAccept),
        ScriptOp::Expect(ExpectOp::PollRequestAndInterpreter),
        ScriptOp::Do(DoOp::RequestAndInterpreterIncomingRequest {
            request: proto::Request::WriteBlock(hello_world_write_req("ectx09")),
        }),
        ScriptOp::Expect(ExpectOp::WriteBlockNoSpaceLeft {
            expect_context: "ectx09",
        }),
        ScriptOp::Expect(ExpectOp::PollRequestAndInterpreter),
        // { 1: ready @ 79 .. 124, 1: read req, 2: write task in progress @ 18 }
        ScriptOp::Do(DoOp::RequestAndInterpreterIncomingRequest {
            request: proto::Request::ReadBlock(proto::RequestReadBlock { block_id: block::Id::init().next(), context: "ectx0a", }),
        }),
        ScriptOp::Expect(ExpectOp::Idle),
        ScriptOp::Expect(ExpectOp::PollRequestAndInterpreter),
        // { 1: ready @ 79 .. 124, 1: read req, 2: write task done @ 18 .. 79 }
        ScriptOp::Do(DoOp::RequestAndInterpreterIncomingTaskDone {
            task_done: task::Done {
                current_offset: 79,
                task: task::TaskDone {
                    block_id: block::Id::init().next().next(),
                    kind: task::TaskDoneKind::WriteBlock(task::TaskDoneWriteBlock {
                        context: task::WriteBlockContext::External("ectx08"),
                    }),
                },
            },
        }),
        ScriptOp::Expect(ExpectOp::WriteBlockDone {
            expect_block_id: block::Id::init().next().next(),
            expect_context: "ectx08",
        }),
        // { 1: ready @ 79 .. 124, 1: read task in progress, 2: ready @ 18 .. 79 }
        ScriptOp::Expect(ExpectOp::InterpretTask {
            expect_offset: 79,
            expect_task: ExpectTask {
                block_id: block::Id::init().next(),
                kind: ExpectTaskKind::ReadBlock(ExpectTaskReadBlock {
                    block_header: storage::BlockHeader {
                        block_id: block::Id::init().next(),
                        block_size: 13,
                        ..Default::default()
                    },
                    context: task::ReadBlockContext::Process(task::ReadBlockProcessContext::External("ectx0a")),
                }),
            },
        }),
        ScriptOp::Do(DoOp::TaskAccept),
        ScriptOp::Expect(ExpectOp::PollRequestAndInterpreter),
        ScriptOp::Do(DoOp::RequestAndInterpreterIncomingTaskDone {
            task_done: task::Done {
                current_offset: 79,
                task: hello_world_read_done(block::Id::init().next(), "ectx0a"),
            },
        }),
        ScriptOp::Expect(ExpectOp::Idle),
        ScriptOp::Expect(ExpectOp::Idle),
        // { 1: ready @ 79 .. 124, 1: read task done process, 2: ready @ 18 .. 79 }
        ScriptOp::Expect(ExpectOp::ProcessReadBlockTaskDone {
            expect_block_id: block::Id::init().next(),
            expect_block_bytes: hello_world_bytes().freeze(),
            expect_pending_contexts_key: "pk1",
        }),
        ScriptOp::Expect(ExpectOp::PollRequest),
        // { 1: ready @ 79 .. 124, 1: read task done process done, 2: ready @ 18 .. 79 }
        ScriptOp::Do(DoOp::RequestIncomingProcessReadBlockDone {
            block_id: block::Id::init().next(),
            block_bytes: hello_world_bytes().freeze(),
            pending_contexts_key: "pk1",
        }),
        ScriptOp::Expect(ExpectOp::Idle),
        ScriptOp::Expect(ExpectOp::ReadBlockDone {
            expect_block_bytes: hello_world_bytes().freeze(),
            expect_context: "ectx0a",
        }),
        ScriptOp::Expect(ExpectOp::PollRequest),
        ScriptOp::Do(DoOp::RequestIncomingRequest {
            request: proto::Request::Info(proto::RequestInfo { context: "ectx0b", }),
        }),
        ScriptOp::Expect(ExpectOp::InfoSuccess {
            expect_info: Info {
                blocks_count: 2,
                wheel_size_bytes: 160,
                service_bytes_used: 122,
                data_bytes_used: 26,
                defrag_write_pending_bytes: 0,
                bytes_free: 12,
                interpret_stats: InterpretStats {
                    count_total: 0,
                    count_no_seek: 0,
                    count_seek_forward: 0,
                    count_seek_backward: 0,
                },
                read_block_cache_hits: 0,
                read_block_cache_misses: 2,
            },
            expect_context: "ectx0b",
        }),
        ScriptOp::Expect(ExpectOp::PollRequest),
        ScriptOp::Do(DoOp::RequestIncomingRequest {
            request: proto::Request::Flush(proto::RequestFlush { context: "ectx0c", }),
        }),
        ScriptOp::Expect(ExpectOp::Idle),
        ScriptOp::Expect(ExpectOp::FlushSuccess { expect_context: "ectx0c", }),
        ScriptOp::Expect(ExpectOp::PollRequest),
    ];

    interpret(performer, script)
}

#[test]
fn script_iter() {
    let performer = init();
    let script = vec![
        // { }
        ScriptOp::Expect(ExpectOp::PollRequest),
        // { 0: write req }
        ScriptOp::Do(DoOp::RequestIncomingRequest {
            request: proto::Request::WriteBlock(hello_world_write_req("ectx02")),
        }),
        // { 0: prep write }
        ScriptOp::Expect(ExpectOp::PrepareInterpretTaskWriteBlock {
            expect_block_id: block::Id::init(),
            expect_block_bytes: hello_world_bytes().freeze(),
            expect_context: task::WriteBlockContext::External("ectx02"),
        }),
        ScriptOp::Expect(ExpectOp::PollRequest),
        // { 0: prep write done }
        ScriptOp::Do(DoOp::RequestIncomingPreparedWriteBlockDone {
            block_id: block::Id::init(),
            write_block_bytes: hello_world_bytes(),
            context: task::WriteBlockContext::External("ectx02"),
        }),
        ScriptOp::Expect(ExpectOp::Idle),
        // { 0: write task in progress @ 18 }
        ScriptOp::Expect(ExpectOp::InterpretTask {
            expect_offset: 18,
            expect_task: ExpectTask {
                block_id: block::Id::init(),
                kind: ExpectTaskKind::WriteBlock(ExpectTaskWriteBlock {
                    write_block_bytes: hello_world_bytes().freeze(),
                    commit: task::Commit::WithTerminator,
                    context: task::WriteBlockContext::External("ectx02"),
                }),
            },
        }),
        ScriptOp::Do(DoOp::TaskAccept),
        ScriptOp::Expect(ExpectOp::PollRequestAndInterpreter),
        // { 0: write task in progress @ 18, 1: write req }
        ScriptOp::Do(DoOp::RequestAndInterpreterIncomingRequest {
            request: proto::Request::WriteBlock(hello_world_write_req("ectx03")),
        }),
        // { 0: write task in progress @ 18, 1: prep write }
        ScriptOp::Expect(ExpectOp::PrepareInterpretTaskWriteBlock {
            expect_block_id: block::Id::init().next(),
            expect_block_bytes: hello_world_bytes().freeze(),
            expect_context: task::WriteBlockContext::External("ectx03"),
        }),
        ScriptOp::Expect(ExpectOp::PollRequestAndInterpreter),
        // { 0: write task in progress @ 18, 1: prep write done }
        ScriptOp::Do(DoOp::RequestAndInterpreterIncomingPreparedWriteBlockDone {
            block_id: block::Id::init().next(),
            write_block_bytes: hello_world_bytes(),
            context: task::WriteBlockContext::External("ectx03"),
        }),
        ScriptOp::Expect(ExpectOp::Idle),
        ScriptOp::Expect(ExpectOp::PollRequestAndInterpreter),
        // { 0: write task done @ 18 .. 79, 1: prep write done }
        ScriptOp::Do(DoOp::RequestAndInterpreterIncomingTaskDone {
            task_done: task::Done {
                current_offset: 79,
                task: task::TaskDone {
                    block_id: block::Id::init(),
                    kind: task::TaskDoneKind::WriteBlock(task::TaskDoneWriteBlock {
                        context: task::WriteBlockContext::External("ectx02"),
                    }),
                },
            },
        }),
        ScriptOp::Expect(ExpectOp::WriteBlockDone {
            expect_block_id: block::Id::init(),
            expect_context: "ectx02",
        }),
        // { 0: ready @ 18 .. 79, 1: write task in progress @ 79 }
        ScriptOp::Expect(ExpectOp::InterpretTask {
            expect_offset: 79,
            expect_task: ExpectTask {
                block_id: block::Id::init().next(),
                kind: ExpectTaskKind::WriteBlock(ExpectTaskWriteBlock {
                    write_block_bytes: hello_world_bytes().freeze(),
                    commit: task::Commit::WithTerminator,
                    context: task::WriteBlockContext::External("ectx03"),
                }),
            },
        }),
        ScriptOp::Do(DoOp::TaskAccept),
        ScriptOp::Expect(ExpectOp::PollRequestAndInterpreter),
        // { 0: ready @ 18 .. 79, 1: write task done @ 79 }
        ScriptOp::Do(DoOp::RequestAndInterpreterIncomingTaskDone {
            task_done: task::Done {
                current_offset: 124,
                task: task::TaskDone {
                    block_id: block::Id::init().next(),
                    kind: task::TaskDoneKind::WriteBlock(task::TaskDoneWriteBlock {
                        context: task::WriteBlockContext::External("ectx03"),
                    }),
                },
            },
        }),
        ScriptOp::Expect(ExpectOp::WriteBlockDone {
            expect_block_id: block::Id::init().next(),
            expect_context: "ectx03",
        }),
        ScriptOp::Expect(ExpectOp::PollRequest),

        // request iter init
        ScriptOp::Do(DoOp::RequestIncomingRequest {
            request: proto::Request::IterBlocksInit(proto::RequestIterBlocksInit { context: "ectx04", }),
        }),
        ScriptOp::Expect(ExpectOp::IterBlocksInit {
            expect_iter_blocks: IterBlocks {
                blocks_total_count: 2,
                blocks_total_size: 26,
                iterator_next: IterBlocksIterator {
                    block_id_from: block::Id::init(),
                },
            },
            expect_context: "ectx04",
        }),
        ScriptOp::Expect(ExpectOp::PollRequest),

        // request iter next
        ScriptOp::Do(DoOp::RequestIncomingRequest {
            request: proto::Request::IterBlocksNext(proto::RequestIterBlocksNext {
                iterator_next: IterBlocksIterator {
                    block_id_from: block::Id::init(),
                },
                context: "ectx05",
            }),
        }),
        ScriptOp::Expect(ExpectOp::Idle),
        ScriptOp::Expect(ExpectOp::InterpretTask {
            expect_offset: 18,
            expect_task: ExpectTask {
                block_id: block::Id::init(),
                kind: ExpectTaskKind::ReadBlock(ExpectTaskReadBlock {
                    block_header: storage::BlockHeader {
                        block_id: block::Id::init(),
                        block_size: 13,
                        ..Default::default()
                    },
                    context: task::ReadBlockContext::Process(task::ReadBlockProcessContext::IterBlocks {
                        iter_blocks_next_context: "ectx05",
                        next_block_id: block::Id::init().next(),
                    }),
                }),
            },
        }),
        ScriptOp::Do(DoOp::TaskAccept),
        ScriptOp::Expect(ExpectOp::PollRequestAndInterpreter),
        ScriptOp::Do(DoOp::RequestAndInterpreterIncomingTaskDone {
            task_done: task::Done {
                current_offset: 79,
                task: task::TaskDone {
                    block_id: block::Id::init(),
                    kind: task::TaskDoneKind::ReadBlock(task::TaskDoneReadBlock {
                        block_bytes: hello_world_bytes(),
                        context: task::ReadBlockContext::Process(task::ReadBlockProcessContext::IterBlocks {
                            iter_blocks_next_context: "ectx05",
                            next_block_id: block::Id::init().next(),
                        }),
                    }),
                },
            },
        }),
        ScriptOp::Expect(ExpectOp::Idle),
        ScriptOp::Expect(ExpectOp::Idle),
        ScriptOp::Expect(ExpectOp::ProcessReadBlockTaskDone {
            expect_block_id: block::Id::init(),
            expect_block_bytes: hello_world_bytes().freeze(),
            expect_pending_contexts_key: "pk2",
        }),
        ScriptOp::Expect(ExpectOp::PollRequest),
        ScriptOp::Do(DoOp::RequestIncomingProcessReadBlockDone {
            block_id: block::Id::init(),
            block_bytes: hello_world_bytes().freeze(),
            pending_contexts_key: "pk2",
        }),
        ScriptOp::Expect(ExpectOp::Idle),
        ScriptOp::Expect(ExpectOp::IterBlocksNext {
            expect_item: IterBlocksItem::Block {
                block_id: block::Id::init(),
                block_bytes: hello_world_bytes().freeze(),
                iterator_next: IterBlocksIterator {
                    block_id_from: block::Id::init().next(),
                },
            },
            expect_context: "ectx05",
        }),
        ScriptOp::Expect(ExpectOp::PollRequest),

        // request iter next
        ScriptOp::Do(DoOp::RequestIncomingRequest {
            request: proto::Request::IterBlocksNext(proto::RequestIterBlocksNext {
                iterator_next: IterBlocksIterator {
                    block_id_from: block::Id::init().next(),
                },
                context: "ectx06",
            }),
        }),
        ScriptOp::Expect(ExpectOp::Idle),
        ScriptOp::Expect(ExpectOp::InterpretTask {
            expect_offset: 79,
            expect_task: ExpectTask {
                block_id: block::Id::init().next(),
                kind: ExpectTaskKind::ReadBlock(ExpectTaskReadBlock {
                    block_header: storage::BlockHeader {
                        block_id: block::Id::init().next(),
                        block_size: 13,
                        ..Default::default()
                    },
                    context: task::ReadBlockContext::Process(task::ReadBlockProcessContext::IterBlocks {
                        iter_blocks_next_context: "ectx06",
                        next_block_id: block::Id::init().next().next(),
                    }),
                }),
            },
        }),
        ScriptOp::Do(DoOp::TaskAccept),
        ScriptOp::Expect(ExpectOp::PollRequestAndInterpreter),
        ScriptOp::Do(DoOp::RequestAndInterpreterIncomingTaskDone {
            task_done: task::Done {
                current_offset: 124,
                task: task::TaskDone {
                    block_id: block::Id::init().next(),
                    kind: task::TaskDoneKind::ReadBlock(task::TaskDoneReadBlock {
                        block_bytes: hello_world_bytes(),
                        context: task::ReadBlockContext::Process(task::ReadBlockProcessContext::IterBlocks {
                            iter_blocks_next_context: "ectx06",
                            next_block_id: block::Id::init().next().next(),
                        }),
                    }),
                },
            },
        }),
        ScriptOp::Expect(ExpectOp::Idle),
        ScriptOp::Expect(ExpectOp::Idle),
        ScriptOp::Expect(ExpectOp::ProcessReadBlockTaskDone {
            expect_block_id: block::Id::init().next(),
            expect_block_bytes: hello_world_bytes().freeze(),
            expect_pending_contexts_key: "pk3",
        }),
        ScriptOp::Expect(ExpectOp::PollRequest),
        ScriptOp::Do(DoOp::RequestIncomingProcessReadBlockDone {
            block_id: block::Id::init().next(),
            block_bytes: hello_world_bytes().freeze(),
            pending_contexts_key: "pk3",
        }),
        ScriptOp::Expect(ExpectOp::Idle),
        ScriptOp::Expect(ExpectOp::IterBlocksNext {
            expect_item: IterBlocksItem::Block {
                block_id: block::Id::init().next(),
                block_bytes: hello_world_bytes().freeze(),
                iterator_next: IterBlocksIterator {
                    block_id_from: block::Id::init().next().next(),
                },
            },
            expect_context: "ectx06",
        }),
        ScriptOp::Expect(ExpectOp::PollRequest),

        // request iter next
        ScriptOp::Do(DoOp::RequestIncomingRequest {
            request: proto::Request::IterBlocksNext(proto::RequestIterBlocksNext {
                iterator_next: IterBlocksIterator {
                    block_id_from: block::Id::init().next().next(),
                },
                context: "ectx07",
            }),
        }),
        ScriptOp::Expect(ExpectOp::IterBlocksNext {
            expect_item: IterBlocksItem::NoMoreBlocks,
            expect_context: "ectx07",
        }),
        ScriptOp::Expect(ExpectOp::PollRequest),
    ];

    interpret(performer, script)
}
