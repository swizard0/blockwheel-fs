use super::{
    task,
    proto,
    block,
    storage,
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
};

use crate::{
    InterpretStats,
    wheel::{
        core::{
            performer::{
                IterBlocksState,
                IterBlocksCursor,
            },
        },
    },
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
        // { 0: write task in progress @ 24 }
        ScriptOp::Expect(ExpectOp::InterpretTask {
            expect_offset: 24,
            expect_task: ExpectTask {
                block_id: block::Id::init(),
                kind: ExpectTaskKind::WriteBlock(ExpectTaskWriteBlock {
                    write_block_bytes: hello_world_bytes().freeze(),
                    commit: task::Commit::WithTerminator,
                    context: task::WriteBlockContext::External("ectx02"),
                }),
            },
        }),
        ScriptOp::Do(DoOp::TaskAccept { interpreter_context: "ictx00", }),
        ScriptOp::Expect(ExpectOp::PollRequestAndInterpreter {
            expect_context: "ictx00",
        }),
        // { 0: write task in progress @ 24, 0: read req }
        ScriptOp::Do(DoOp::RequestAndInterpreterIncomingRequest {
            request: proto::Request::ReadBlock(proto::RequestReadBlock { block_id: block::Id::init(), context: "ectx03", }),
            interpreter_context: "ictx01",
        }),
        ScriptOp::Expect(ExpectOp::Idle),
        ScriptOp::Expect(ExpectOp::PollRequestAndInterpreter {
            expect_context: "ictx01",
        }),
        // { 0: write task in progress @ 24, 0: read req, 0: delete req }
        ScriptOp::Do(DoOp::RequestAndInterpreterIncomingRequest {
            request: proto::Request::DeleteBlock(proto::RequestDeleteBlock { block_id: block::Id::init(), context: "ectx04", }),
            interpreter_context: "ictx02",
        }),
        // { 0: write task in progress @ 24, 0: read req, 0: prep delete }
        ScriptOp::Expect(ExpectOp::PrepareInterpretTaskDeleteBlock {
            expect_block_id: block::Id::init(),
            expect_context: task::DeleteBlockContext::External("ectx04"),
        }),
        ScriptOp::Expect(ExpectOp::PollRequestAndInterpreter {
            expect_context: "ictx02",
        }),
        // { 0: write task in progress @ 24, 0: read req, 0: prep delete, 1: write req }
        ScriptOp::Do(DoOp::RequestAndInterpreterIncomingRequest {
            request: proto::Request::WriteBlock(hello_world_write_req("ectx05")),
            interpreter_context: "ictx03",
        }),
        // { 0: write task in progress @ 24, 0: read req, 0: prep delete, 1: prep write }
        ScriptOp::Expect(ExpectOp::PrepareInterpretTaskWriteBlock {
            expect_block_id: block::Id::init().next(),
            expect_block_bytes: hello_world_bytes().freeze(),
            expect_context: task::WriteBlockContext::External("ectx05"),
        }),
        ScriptOp::Expect(ExpectOp::PollRequestAndInterpreter {
            expect_context: "ictx03",
        }),
        // { 0: write task in progress @ 24, 0: read req, 0: prep delete done, 1: prep write }
        ScriptOp::Do(DoOp::RequestAndInterpreterIncomingPreparedDeleteBlockDone {
            block_id: block::Id::init(),
            delete_block_bytes: hello_world_bytes(),
            context: task::DeleteBlockContext::External("ectx04"),
            interpreter_context: "ictx03",
        }),
        ScriptOp::Expect(ExpectOp::Idle),
        ScriptOp::Expect(ExpectOp::PollRequestAndInterpreter {
            expect_context: "ictx03",
        }),
        // { 0: write task in progress @ 24, 0: read req, 0: prep delete done, 1: prep write done }
        ScriptOp::Do(DoOp::RequestAndInterpreterIncomingPreparedWriteBlockDone {
            block_id: block::Id::init().next(),
            write_block_bytes: hello_world_bytes(),
            context: task::WriteBlockContext::External("ectx05"),
            interpreter_context: "ictx03",
        }),
        ScriptOp::Expect(ExpectOp::Idle),
        ScriptOp::Expect(ExpectOp::PollRequestAndInterpreter {
            expect_context: "ictx03",
        }),
        // { 0: write task done @ 24 .. 85, 0: read req, 0: prep delete done, 1: prep write done }
        ScriptOp::Do(DoOp::RequestAndInterpreterIncomingTaskDone {
            task_done: task::Done {
                current_offset: 85,
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
        // { 0: ready @ 24 .. 85, 0: read req, 0: prep delete done, 1: write task in progress @ 85 }
        ScriptOp::Expect(ExpectOp::InterpretTask {
            expect_offset: 85,
            expect_task: ExpectTask {
                block_id: block::Id::init().next(),
                kind: ExpectTaskKind::WriteBlock(ExpectTaskWriteBlock {
                    write_block_bytes: hello_world_bytes().freeze(),
                    commit: task::Commit::WithTerminator,
                    context: task::WriteBlockContext::External("ectx05"),
                }),
            },
        }),
        ScriptOp::Do(DoOp::TaskAccept { interpreter_context: "ictx04", }),
        ScriptOp::Expect(ExpectOp::PollRequestAndInterpreter {
            expect_context: "ictx04",
        }),
        // { 0: ready @ 24 .. 85, 0: read req, 0: prep delete done, 1: write task done @ 85 .. 130 }
        ScriptOp::Do(DoOp::RequestAndInterpreterIncomingTaskDone {
            task_done: task::Done {
                current_offset: 130,
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
        // { 0: ready @ 24 .. 85, 0: read task in progress @ 24, 0: prep delete done, 1: write task done @ 85 .. 130 }
        ScriptOp::Expect(ExpectOp::InterpretTask {
            expect_offset: 24,
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
        ScriptOp::Do(DoOp::TaskAccept { interpreter_context: "ictx05", }),
        ScriptOp::Expect(ExpectOp::PollRequestAndInterpreter {
            expect_context: "ictx05",
        }),
        // { 0: ready @ 24 .. 85, 0: read task done @ 24 .. 85, 0: prep delete done, 1: ready @ 85 .. 130 }
        ScriptOp::Do(DoOp::RequestAndInterpreterIncomingTaskDone {
            task_done: task::Done {
                current_offset: 85,
                task: hello_world_read_done(block::Id::init(), "ectx03"),
            },
        }),
        ScriptOp::Expect(ExpectOp::Idle),
        ScriptOp::Expect(ExpectOp::Idle),
        // { 0: ready @ 24 .. 85, 0: read task done process @ 24 .. 85, 0: prep delete done, 1: ready @ 85 .. 130 }
        ScriptOp::Expect(ExpectOp::ProcessReadBlockTaskDone {
            expect_block_id: block::Id::init(),
            expect_block_bytes: hello_world_bytes().freeze(),
            expect_pending_contexts_key: "pk0",
        }),
        // { 0: ready @ 24 .. 85, 0: read task done process @ 24 .. 85, 0: delete task in progress @ 24, 1: ready @ 85 .. 130 }
        ScriptOp::Expect(ExpectOp::InterpretTask {
            expect_offset: 24,
            expect_task: ExpectTask {
                block_id: block::Id::init(),
                kind: ExpectTaskKind::DeleteBlock(ExpectTaskDeleteBlock {
                    delete_block_bytes: hello_world_bytes().freeze(),
                    commit: task::Commit::None,
                    context: task::DeleteBlockContext::External("ectx04"),
                }),
            },
        }),
        ScriptOp::Do(DoOp::TaskAccept { interpreter_context: "ictx05", }),
        ScriptOp::Expect(ExpectOp::PollRequestAndInterpreter {
            expect_context: "ictx05",
        }),
        // { 0: ready @ 24 .. 85, 0: read task done process @ 24 .. 85, 0: delete task in progress @ 24, 0: read req, 1: ready @ 85 .. 130 }
        ScriptOp::Do(DoOp::RequestAndInterpreterIncomingRequest {
            request: proto::Request::ReadBlock(proto::RequestReadBlock { block_id: block::Id::init(), context: "ectx06", }),
            interpreter_context: "ictx06",
        }),
        ScriptOp::Expect(ExpectOp::Idle),
        ScriptOp::Expect(ExpectOp::PollRequestAndInterpreter {
            expect_context: "ictx06",
        }),
        // { 0: ready @ 24 .. 85, 0: read task done process @ 24 .. 85, 0: delete task in progress @ 24, 0: read req, 0: delete req,
        //   1: ready @ 85 .. 130 }
        ScriptOp::Do(DoOp::RequestAndInterpreterIncomingRequest {
            request: proto::Request::DeleteBlock(proto::RequestDeleteBlock { block_id: block::Id::init(), context: "ectx07", }),
            interpreter_context: "ictx07",
        }),
        // { 0: ready @ 24 .. 85, 0: read task done process @ 24 .. 85, 0: delete task in progress @ 24, 0: read req, 0: prep delete,
        //   1: ready @ 85 .. 130 }
        ScriptOp::Expect(ExpectOp::PrepareInterpretTaskDeleteBlock {
            expect_block_id: block::Id::init(),
            expect_context: task::DeleteBlockContext::External("ectx07"),
        }),
        ScriptOp::Expect(ExpectOp::PollRequestAndInterpreter {
            expect_context: "ictx07",
        }),
        // { 0: ready @ 24 .. 85, 0: read task done process @ 24 .. 85, 0: delete task in progress @ 24, 0: read req, 0: prep delete done,
        //   1: ready @ 85 .. 130 }
        ScriptOp::Do(DoOp::RequestAndInterpreterIncomingPreparedDeleteBlockDone {
            block_id: block::Id::init(),
            delete_block_bytes: hello_world_bytes(),
            context: task::DeleteBlockContext::External("ectx07"),
            interpreter_context: "ictx07",
        }),
        ScriptOp::Expect(ExpectOp::Idle),
        ScriptOp::Expect(ExpectOp::PollRequestAndInterpreter {
            expect_context: "ictx07",
        }),
        // { 0: ready @ 24 .. 85, 0: read task done process @ 24 .. 85, 0: delete task done @ 24, 0: read req, 0: prep delete done,
        //   1: ready @ 85 .. 130 }
        ScriptOp::Do(DoOp::RequestAndInterpreterIncomingTaskDone {
            task_done: task::Done {
                current_offset: 24,
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
        // { 0: ready @ 24 .. 85, 0: read task done process @ 24 .. 85, 0: delete task done @ 24, 0: prep delete done,
        //   1: ready @ 85 .. 130 }
        ScriptOp::Expect(ExpectOp::ReadBlockNotFound {
            expect_context: "ectx06",
        }),
        // { 0: ready @ 24 .. 85, 0: read task done process @ 24 .. 85, 0: delete task done @ 24, 1: ready @ 85 .. 130 }
        ScriptOp::Expect(ExpectOp::DeleteBlockNotFound {
            expect_context: "ectx07",
        }),
        // { 0: read task done process @ 24 .. 85, 1: ready @ 85 .. 130 }
        ScriptOp::Expect(ExpectOp::PollRequest),
        // { 0: read task done process done @ 24 .. 85, 1: ready @ 85 .. 130 }
        ScriptOp::Do(DoOp::RequestIncomingProcessReadBlockDone {
            block_id: block::Id::init(),
            block_bytes: hello_world_bytes().freeze(),
            pending_contexts_key: "pk0",
        }),
        ScriptOp::Expect(ExpectOp::Idle),
        // { 1: ready @ 85 .. 130 }
        ScriptOp::Expect(ExpectOp::ReadBlockNotFound {
            expect_context: "ectx03",
        }),
        ScriptOp::Expect(ExpectOp::PollRequest),
        // { 1: ready @ 85 .. 130, 2: write req }
        ScriptOp::Do(DoOp::RequestIncomingRequest {
            request: proto::Request::WriteBlock(hello_world_write_req("ectx08")),
        }),
        // { 1: ready @ 85 .. 130, 2: prep write }
        ScriptOp::Expect(ExpectOp::PrepareInterpretTaskWriteBlock {
            expect_block_id: block::Id::init().next().next(),
            expect_block_bytes: hello_world_bytes().freeze(),
            expect_context: task::WriteBlockContext::External("ectx08"),
        }),
        ScriptOp::Expect(ExpectOp::PollRequest),
        // { 1: ready @ 85 .. 130, 2: prep write done }
        ScriptOp::Do(DoOp::RequestIncomingPreparedWriteBlockDone {
            block_id: block::Id::init().next().next(),
            write_block_bytes: hello_world_bytes(),
            context: task::WriteBlockContext::External("ectx08"),
        }),
        ScriptOp::Expect(ExpectOp::Idle),
        // { 1: ready @ 85 .. 130, 2: write task in progress @ 24 }
        ScriptOp::Expect(ExpectOp::InterpretTask {
            expect_offset: 24,
            expect_task: ExpectTask {
                block_id: block::Id::init().next().next(),
                kind: ExpectTaskKind::WriteBlock(ExpectTaskWriteBlock {
                    write_block_bytes: hello_world_bytes().freeze(),
                    commit: task::Commit::None,
                    context: task::WriteBlockContext::External("ectx08"),
                }),
            },
        }),
        ScriptOp::Do(DoOp::TaskAccept { interpreter_context: "ictx08", }),
        ScriptOp::Expect(ExpectOp::PollRequestAndInterpreter {
            expect_context: "ictx08",
        }),
        ScriptOp::Do(DoOp::RequestAndInterpreterIncomingRequest {
            request: proto::Request::WriteBlock(hello_world_write_req("ectx09")),
            interpreter_context: "ictx09",
        }),
        ScriptOp::Expect(ExpectOp::WriteBlockNoSpaceLeft {
            expect_context: "ectx09",
        }),
        ScriptOp::Expect(ExpectOp::PollRequestAndInterpreter {
            expect_context: "ictx09",
        }),
        // { 1: ready @ 85 .. 130, 1: read req, 2: write task in progress @ 24 }
        ScriptOp::Do(DoOp::RequestAndInterpreterIncomingRequest {
            request: proto::Request::ReadBlock(proto::RequestReadBlock { block_id: block::Id::init().next(), context: "ectx0a", }),
            interpreter_context: "ictx0a",
        }),
        ScriptOp::Expect(ExpectOp::Idle),
        ScriptOp::Expect(ExpectOp::PollRequestAndInterpreter {
            expect_context: "ictx0a",
        }),
        // { 1: ready @ 85 .. 130, 1: read req, 2: write task done @ 24 .. 85 }
        ScriptOp::Do(DoOp::RequestAndInterpreterIncomingTaskDone {
            task_done: task::Done {
                current_offset: 85,
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
        // { 1: ready @ 85 .. 130, 1: read task in progress, 2: ready @ 24 .. 85 }
        ScriptOp::Expect(ExpectOp::InterpretTask {
            expect_offset: 85,
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
        ScriptOp::Do(DoOp::TaskAccept { interpreter_context: "ictx0b", }),
        ScriptOp::Expect(ExpectOp::PollRequestAndInterpreter {
            expect_context: "ictx0b",
        }),
        ScriptOp::Do(DoOp::RequestAndInterpreterIncomingTaskDone {
            task_done: task::Done {
                current_offset: 85,
                task: hello_world_read_done(block::Id::init().next(), "ectx0a"),
            },
        }),
        ScriptOp::Expect(ExpectOp::Idle),
        ScriptOp::Expect(ExpectOp::Idle),
        // { 1: ready @ 85 .. 130, 1: read task done process, 2: ready @ 24 .. 85 }
        ScriptOp::Expect(ExpectOp::ProcessReadBlockTaskDone {
            expect_block_id: block::Id::init().next(),
            expect_block_bytes: hello_world_bytes().freeze(),
            expect_pending_contexts_key: "pk1",
        }),
        ScriptOp::Expect(ExpectOp::PollRequest),
        // { 1: ready @ 85 .. 130, 1: read task done process done, 2: ready @ 24 .. 85 }
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
                service_bytes_used: 128,
                data_bytes_used: 26,
                defrag_write_pending_bytes: 0,
                bytes_free: 6,
                interpret_stats: InterpretStats {
                    count_total: 0,
                    count_no_seek: 0,
                    count_seek_forward: 0,
                    count_seek_backward: 0,
                },
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
        // { 0: write task in progress @ 24 }
        ScriptOp::Expect(ExpectOp::InterpretTask {
            expect_offset: 24,
            expect_task: ExpectTask {
                block_id: block::Id::init(),
                kind: ExpectTaskKind::WriteBlock(ExpectTaskWriteBlock {
                    write_block_bytes: hello_world_bytes().freeze(),
                    commit: task::Commit::WithTerminator,
                    context: task::WriteBlockContext::External("ectx02"),
                }),
            },
        }),
        ScriptOp::Do(DoOp::TaskAccept { interpreter_context: "ictx00", }),
        ScriptOp::Expect(ExpectOp::PollRequestAndInterpreter {
            expect_context: "ictx00",
        }),
        // { 0: write task in progress @ 24, 1: write req }
        ScriptOp::Do(DoOp::RequestAndInterpreterIncomingRequest {
            request: proto::Request::WriteBlock(hello_world_write_req("ectx03")),
            interpreter_context: "ictx00",
        }),
        // { 0: write task in progress @ 24, 1: prep write }
        ScriptOp::Expect(ExpectOp::PrepareInterpretTaskWriteBlock {
            expect_block_id: block::Id::init().next(),
            expect_block_bytes: hello_world_bytes().freeze(),
            expect_context: task::WriteBlockContext::External("ectx03"),
        }),
        ScriptOp::Expect(ExpectOp::PollRequestAndInterpreter {
            expect_context: "ictx00",
        }),
        // { 0: write task in progress @ 24, 1: prep write done }
        ScriptOp::Do(DoOp::RequestAndInterpreterIncomingPreparedWriteBlockDone {
            block_id: block::Id::init().next(),
            write_block_bytes: hello_world_bytes(),
            context: task::WriteBlockContext::External("ectx03"),
            interpreter_context: "ictx00",
        }),
        ScriptOp::Expect(ExpectOp::Idle),
        ScriptOp::Expect(ExpectOp::PollRequestAndInterpreter {
            expect_context: "ictx00",
        }),
        // { 0: write task done @ 24 .. 85, 1: prep write done }
        ScriptOp::Do(DoOp::RequestAndInterpreterIncomingTaskDone {
            task_done: task::Done {
                current_offset: 85,
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
        // { 0: ready @ 24 .. 85, 1: write task in progress @ 85 }
        ScriptOp::Expect(ExpectOp::InterpretTask {
            expect_offset: 85,
            expect_task: ExpectTask {
                block_id: block::Id::init().next(),
                kind: ExpectTaskKind::WriteBlock(ExpectTaskWriteBlock {
                    write_block_bytes: hello_world_bytes().freeze(),
                    commit: task::Commit::WithTerminator,
                    context: task::WriteBlockContext::External("ectx03"),
                }),
            },
        }),
        ScriptOp::Do(DoOp::TaskAccept { interpreter_context: "ictx01", }),
        ScriptOp::Expect(ExpectOp::PollRequestAndInterpreter {
            expect_context: "ictx01",
        }),
        // { 0: ready @ 24 .. 85, 1: write task done @ 85 }
        ScriptOp::Do(DoOp::RequestAndInterpreterIncomingTaskDone {
            task_done: task::Done {
                current_offset: 130,
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

        // request iter
        ScriptOp::Do(DoOp::RequestIncomingRequest {
            request: proto::Request::IterBlocks(proto::RequestIterBlocks { context: "ectx04", }),
        }),
        ScriptOp::Expect(ExpectOp::MakeIterBlocksStream),
        ScriptOp::Do(DoOp::StreamReady { iter_context: "sctx00", }),
        ScriptOp::Expect(ExpectOp::Idle),
        ScriptOp::Expect(ExpectOp::InterpretTask {
            expect_offset: 24,
            expect_task: ExpectTask {
                block_id: block::Id::init(),
                kind: ExpectTaskKind::ReadBlock(ExpectTaskReadBlock {
                    block_header: storage::BlockHeader {
                        block_id: block::Id::init(),
                        block_size: 13,
                        ..Default::default()
                    },
                    context: task::ReadBlockContext::Process(task::ReadBlockProcessContext::IterBlocks {
                        iter_blocks_stream_context: "sctx00",
                        next_block_id: block::Id::init().next(),
                    }),
                }),
            },
        }),
        ScriptOp::Do(DoOp::TaskAccept { interpreter_context: "ictx03", }),
        ScriptOp::Expect(ExpectOp::PollRequestAndInterpreter {
            expect_context: "ictx03",
        }),
        ScriptOp::Do(DoOp::RequestAndInterpreterIncomingTaskDone {
            task_done: task::Done {
                current_offset: 85,
                task: task::TaskDone {
                    block_id: block::Id::init(),
                    kind: task::TaskDoneKind::ReadBlock(task::TaskDoneReadBlock {
                        block_bytes: hello_world_bytes(),
                        context: task::ReadBlockContext::Process(task::ReadBlockProcessContext::IterBlocks {
                            iter_blocks_stream_context: "sctx00",
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
        ScriptOp::Expect(ExpectOp::IterBlocksItem {
            expect_block_id: block::Id::init(),
            expect_block_bytes: hello_world_bytes().freeze(),
            expect_context: "sctx00",
        }),
        ScriptOp::Expect(ExpectOp::PollRequest),
        ScriptOp::Do(DoOp::RequestIncomingIterBlocks {
            iter_blocks_state: IterBlocksState {
                iter_blocks_stream_context: "sctx00",
                iter_blocks_cursor: IterBlocksCursor {
                    block_id: block::Id::init().next(),
                },
            },
        }),
        ScriptOp::Expect(ExpectOp::Idle),
        ScriptOp::Expect(ExpectOp::InterpretTask {
            expect_offset: 85,
            expect_task: ExpectTask {
                block_id: block::Id::init().next(),
                kind: ExpectTaskKind::ReadBlock(ExpectTaskReadBlock {
                    block_header: storage::BlockHeader {
                        block_id: block::Id::init().next(),
                        block_size: 13,
                        ..Default::default()
                    },
                    context: task::ReadBlockContext::Process(task::ReadBlockProcessContext::IterBlocks {
                        iter_blocks_stream_context: "sctx00",
                        next_block_id: block::Id::init().next().next(),
                    }),
                }),
            },
        }),
        ScriptOp::Do(DoOp::TaskAccept { interpreter_context: "ictx04", }),
        ScriptOp::Expect(ExpectOp::PollRequestAndInterpreter {
            expect_context: "ictx04",
        }),
        ScriptOp::Do(DoOp::RequestAndInterpreterIncomingTaskDone {
            task_done: task::Done {
                current_offset: 130,
                task: task::TaskDone {
                    block_id: block::Id::init().next(),
                    kind: task::TaskDoneKind::ReadBlock(task::TaskDoneReadBlock {
                        block_bytes: hello_world_bytes(),
                        context: task::ReadBlockContext::Process(task::ReadBlockProcessContext::IterBlocks {
                            iter_blocks_stream_context: "sctx00",
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
        ScriptOp::Expect(ExpectOp::IterBlocksItem {
            expect_block_id: block::Id::init().next(),
            expect_block_bytes: hello_world_bytes().freeze(),
            expect_context: "sctx00",
        }),
        ScriptOp::Expect(ExpectOp::PollRequest),
        ScriptOp::Do(DoOp::RequestIncomingIterBlocks {
            iter_blocks_state: IterBlocksState {
                iter_blocks_stream_context: "sctx00",
                iter_blocks_cursor: IterBlocksCursor {
                    block_id: block::Id::init().next().next(),
                },
            },
        }),
        ScriptOp::Expect(ExpectOp::IterBlocksFinish {
            expect_context: "sctx00",
        }),
        ScriptOp::Expect(ExpectOp::PollRequest),
    ];

    interpret(performer, script)
}
