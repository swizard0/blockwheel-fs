use crate::{
    block,
    proto,
    wheel::{
        core::{
            task,
            storage,
            performer::{
                tests::{
                    with_defrag_config,
                    hello_world_bytes,
                    hello_world_write_req,
                    interpret,
                    ScriptOp,
                    ExpectOp,
                    DoOp,
                    ExpectTask,
                    ExpectTaskKind,
                    ExpectTaskWriteBlock,
                    ExpectTaskReadBlock,
                    ExpectTaskDeleteBlock,
                    DefragConfig,
                },
            },
            SpaceKey,
            DefragGaps,
        },
    },
};

#[test]
fn script_simple_defrag() {
    let performer = with_defrag_config(Some(DefragConfig::new(1)));
    let script = vec![
        // { }
        ScriptOp::Expect(ExpectOp::PollRequest),
        // { 0: write req }
        ScriptOp::Do(DoOp::RequestIncomingRequest {
            request: proto::Request::WriteBlock(hello_world_write_req("ectx00")),
        }),
        // { 0: prep write }
        ScriptOp::Expect(ExpectOp::PrepareInterpretTaskWriteBlock {
            expect_block_id: block::Id::init(),
            expect_block_bytes: hello_world_bytes().freeze(),
            expect_context: task::WriteBlockContext::External("ectx00"),
        }),
        ScriptOp::Expect(ExpectOp::PollRequest),
        // { 0: prep write done }
        ScriptOp::Do(DoOp::RequestIncomingPreparedWriteBlockDone {
            block_id: block::Id::init(),
            write_block_bytes: hello_world_bytes(),
            context: task::WriteBlockContext::External("ectx00"),
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
                    context: task::WriteBlockContext::External("ectx00"),
                }),
            },
        }),
        ScriptOp::Do(DoOp::TaskAccept),
        ScriptOp::Expect(ExpectOp::PollRequestAndInterpreter),

        // { 0: write task in progress @ 18, 1: write req }
        ScriptOp::Do(DoOp::RequestAndInterpreterIncomingRequest {
            request: proto::Request::WriteBlock(hello_world_write_req("ectx01")),
        }),
        // { 0: write task in progress @ 18, 1: prep write }
        ScriptOp::Expect(ExpectOp::PrepareInterpretTaskWriteBlock {
            expect_block_id: block::Id::init().next(),
            expect_block_bytes: hello_world_bytes().freeze(),
            expect_context: task::WriteBlockContext::External("ectx01"),
        }),
        ScriptOp::Expect(ExpectOp::PollRequestAndInterpreter),
        // { 0: write task in progress @ 18, 1: prep write done }
        ScriptOp::Do(DoOp::RequestAndInterpreterIncomingPreparedWriteBlockDone {
            block_id: block::Id::init().next(),
            write_block_bytes: hello_world_bytes(),
            context: task::WriteBlockContext::External("ectx01"),
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
                        context: task::WriteBlockContext::External("ectx00"),
                    }),
                },
            },
        }),
        ScriptOp::Expect(ExpectOp::WriteBlockDone {
            expect_block_id: block::Id::init(),
            expect_context: "ectx00",
        }),
        // { 0: ready @ 18 .. 79, 1: write task in progress @ 79 }
        ScriptOp::Expect(ExpectOp::InterpretTask {
            expect_offset: 79,
            expect_task: ExpectTask {
                block_id: block::Id::init().next(),
                kind: ExpectTaskKind::WriteBlock(ExpectTaskWriteBlock {
                    write_block_bytes: hello_world_bytes().freeze(),
                    commit: task::Commit::WithTerminator,
                    context: task::WriteBlockContext::External("ectx01"),
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
                        context: task::WriteBlockContext::External("ectx01"),
                    }),
                },
            },
        }),
        ScriptOp::Expect(ExpectOp::WriteBlockDone {
            expect_block_id: block::Id::init().next(),
            expect_context: "ectx01",
        }),
        // { 0: ready @ 18 .. 79, 1: ready @ 79 }
        ScriptOp::Expect(ExpectOp::PollRequest),

        // { 0: ready @ 18 .. 79, 0: delete req, 1: ready @ 79 }
        ScriptOp::Do(DoOp::RequestIncomingRequest {
            request: proto::Request::DeleteBlock(proto::RequestDeleteBlock { block_id: block::Id::init(), context: "ectx02", }),
        }),
        // { 0: ready @ 18 .. 79, 0: prep delete, 1: ready @ 79 }
        ScriptOp::Expect(ExpectOp::PrepareInterpretTaskDeleteBlock {
            expect_block_id: block::Id::init(),
            expect_context: task::DeleteBlockContext::External("ectx02"),
        }),
        ScriptOp::Expect(ExpectOp::PollRequest),
        // { 0: ready @ 18 .. 79, 0: prep delete done, 1: ready @ 79 }
        ScriptOp::Do(DoOp::RequestIncomingPreparedDeleteBlockDone {
            block_id: block::Id::init(),
            delete_block_bytes: hello_world_bytes(),
            context: task::DeleteBlockContext::External("ectx02"),
        }),
        ScriptOp::Expect(ExpectOp::Idle),
        // { 0: ready @ 18 .. 79, 0: delete task in progress @ 18, 1: ready @ 79 }
        ScriptOp::Expect(ExpectOp::InterpretTask {
            expect_offset: 18,
            expect_task: ExpectTask {
                block_id: block::Id::init(),
                kind: ExpectTaskKind::DeleteBlock(ExpectTaskDeleteBlock {
                    delete_block_bytes: hello_world_bytes().freeze(),
                    commit: task::Commit::None,
                    context: task::DeleteBlockContext::External("ectx02"),
                }),
            },
        }),
        ScriptOp::Do(DoOp::TaskAccept),
        ScriptOp::Expect(ExpectOp::PollRequestAndInterpreter),
        // { 0: ready @ 18 .. 79, 0: delete task done @ 18, 1: ready @ 79 }
        ScriptOp::Do(DoOp::RequestAndInterpreterIncomingTaskDone {
            task_done: task::Done {
                current_offset: 18,
                task: task::TaskDone {
                    block_id: block::Id::init(),
                    kind: task::TaskDoneKind::DeleteBlock(task::TaskDoneDeleteBlock {
                        context: task::DeleteBlockContext::External("ectx02"),
                    }),
                },
            },
        }),
        ScriptOp::Expect(ExpectOp::DeleteBlockDone {
            expect_block_id: block::Id::init(),
            expect_context: "ectx02",
        }),

        // { 1: ready @ 79 }
        // defragmentation has started
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
                    context: task::ReadBlockContext::Defrag(task::ReadBlockDefragContext {
                        defrag_id: 0,
                        defrag_gaps: DefragGaps::Both {
                            space_key_left: SpaceKey { space_available: 61, serial: 4, },
                            space_key_right: SpaceKey { space_available: 12, serial: 3, },
                            block_offset: 79,
                        },
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
                    block_id: block::Id::init().next(),
                    kind: task::TaskDoneKind::ReadBlock(task::TaskDoneReadBlock {
                        block_bytes: hello_world_bytes(),
                        context: task::ReadBlockContext::Defrag(task::ReadBlockDefragContext {
                            defrag_id: 0,
                            defrag_gaps: DefragGaps::OnlyLeft {
                                space_key_left: SpaceKey { space_available: 61, serial: 4, },
                                block_offset: 79,
                            },
                        }),
                    }),
                },
            },
        }),
        ScriptOp::Expect(ExpectOp::Idle),
        ScriptOp::Expect(ExpectOp::PrepareInterpretTaskDeleteBlock {
            expect_block_id: block::Id::init().next(),
            expect_context: task::DeleteBlockContext::Defrag(task::DeleteBlockDefragContext {
                defrag_id: 0,
                block_bytes: hello_world_bytes().freeze(),
                defrag_gaps: DefragGaps::OnlyLeft {
                    space_key_left: SpaceKey { space_available: 61, serial: 4, },
                    block_offset: 79,
                },
            }),
        }),
        ScriptOp::Expect(ExpectOp::PollRequest),
        ScriptOp::Do(DoOp::RequestIncomingPreparedDeleteBlockDone {
            block_id: block::Id::init().next(),
            delete_block_bytes: hello_world_bytes(),
            context: task::DeleteBlockContext::Defrag(task::DeleteBlockDefragContext {
                defrag_id: 0,
                block_bytes: hello_world_bytes().freeze(),
                defrag_gaps: DefragGaps::OnlyLeft {
                    space_key_left: SpaceKey { space_available: 61, serial: 4, },
                    block_offset: 79,
                },
            }),
        }),
        ScriptOp::Expect(ExpectOp::Idle),
        ScriptOp::Expect(ExpectOp::InterpretTask {
            expect_offset: 79,
            expect_task: ExpectTask {
                block_id: block::Id::init().next(),
                kind: ExpectTaskKind::DeleteBlock(ExpectTaskDeleteBlock {
                    delete_block_bytes: hello_world_bytes().freeze(),
                    commit: task::Commit::WithTerminator,
                    context: task::DeleteBlockContext::Defrag(task::DeleteBlockDefragContext {
                        defrag_id: 0,
                        defrag_gaps: DefragGaps::OnlyLeft {
                            space_key_left: SpaceKey { space_available: 61, serial: 4, },
                            block_offset: 79,
                        },
                        block_bytes: hello_world_bytes().freeze(),
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
                    block_id: block::Id::init().next(),
                    kind: task::TaskDoneKind::DeleteBlock(task::TaskDoneDeleteBlock {
                        context: task::DeleteBlockContext::Defrag(task::DeleteBlockDefragContext {
                            defrag_id: 0,
                            defrag_gaps: DefragGaps::OnlyLeft {
                                space_key_left: SpaceKey { space_available: 61, serial: 4, },
                                block_offset: 79,
                            },
                            block_bytes: hello_world_bytes().freeze(),
                        }),
                    }),
                },
            },
        }),
        ScriptOp::Expect(ExpectOp::Idle),
        ScriptOp::Expect(ExpectOp::InterpretTask {
            expect_offset: 18,
            expect_task: ExpectTask {
                block_id: block::Id::init().next(),
                kind: ExpectTaskKind::WriteBlock(ExpectTaskWriteBlock {
                    write_block_bytes: hello_world_bytes().freeze(),
                    commit: task::Commit::WithTerminator,
                    context: task::WriteBlockContext::Defrag(task::WriteBlockDefragContext {
                        defrag_id: 0,
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
                    block_id: block::Id::init().next(),
                    kind: task::TaskDoneKind::WriteBlock(task::TaskDoneWriteBlock {
                        context: task::WriteBlockContext::Defrag(task::WriteBlockDefragContext {
                            defrag_id: 0,
                        }),
                    }),
                },
            },
        }),
        ScriptOp::Expect(ExpectOp::Idle),
        ScriptOp::Expect(ExpectOp::PollRequest),

        ScriptOp::Do(DoOp::RequestIncomingRequest {
            request: proto::Request::WriteBlock(hello_world_write_req("ectx03")),
        }),
        ScriptOp::Expect(ExpectOp::PrepareInterpretTaskWriteBlock {
            expect_block_id: block::Id::init().next().next(),
            expect_block_bytes: hello_world_bytes().freeze(),
            expect_context: task::WriteBlockContext::External("ectx03"),
        }),
        ScriptOp::Expect(ExpectOp::PollRequest),
        ScriptOp::Do(DoOp::RequestIncomingPreparedWriteBlockDone {
            block_id: block::Id::init().next().next(),
            write_block_bytes: hello_world_bytes(),
            context: task::WriteBlockContext::External("ectx03"),
        }),
        ScriptOp::Expect(ExpectOp::Idle),
        ScriptOp::Expect(ExpectOp::InterpretTask {
            expect_offset: 79,
            expect_task: ExpectTask {
                block_id: block::Id::init().next().next(),
                kind: ExpectTaskKind::WriteBlock(ExpectTaskWriteBlock {
                    write_block_bytes: hello_world_bytes().freeze(),
                    commit: task::Commit::WithTerminator,
                    context: task::WriteBlockContext::External("ectx03"),
                }),
            },
        }),
        ScriptOp::Do(DoOp::TaskAccept),
        ScriptOp::Expect(ExpectOp::PollRequestAndInterpreter),
        ScriptOp::Do(DoOp::RequestAndInterpreterIncomingTaskDone {
            task_done: task::Done {
                current_offset: 124,
                task: task::TaskDone {
                    block_id: block::Id::init().next().next(),
                    kind: task::TaskDoneKind::WriteBlock(task::TaskDoneWriteBlock {
                        context: task::WriteBlockContext::External("ectx03"),
                    }),
                },
            },
        }),
        ScriptOp::Expect(ExpectOp::WriteBlockDone {
            expect_block_id: block::Id::init().next().next(),
            expect_context: "ectx03",
        }),
        ScriptOp::Expect(ExpectOp::PollRequest),
    ];

    interpret(performer, script)
}
