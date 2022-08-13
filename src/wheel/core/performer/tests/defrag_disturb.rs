use super::{
    task,
    proto,
    block,
    storage,
    with_defrag_config,
    hello_world_bytes,
    hello_world_write_req,
    hello_bytes,
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
};

use crate::wheel::core::{
    SpaceKey,
    DefragGaps,
};

#[test]
fn script_defrag_disturb() {
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
        // { 0: write task in progress @ 24 }
        ScriptOp::Expect(ExpectOp::InterpretTask {
            expect_offset: 24,
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

        // { 0: write task in progress @ 24, 1: write req }
        ScriptOp::Do(DoOp::RequestAndInterpreterIncomingRequest {
            request: proto::Request::WriteBlock(hello_world_write_req("ectx01")),
        }),
        // { 0: write task in progress @ 24, 1: prep write }
        ScriptOp::Expect(ExpectOp::PrepareInterpretTaskWriteBlock {
            expect_block_id: block::Id::init().next(),
            expect_block_bytes: hello_world_bytes().freeze(),
            expect_context: task::WriteBlockContext::External("ectx01"),
        }),
        ScriptOp::Expect(ExpectOp::PollRequestAndInterpreter),
        // { 0: write task in progress @ 24, 1: prep write done }
        ScriptOp::Do(DoOp::RequestAndInterpreterIncomingPreparedWriteBlockDone {
            block_id: block::Id::init().next(),
            write_block_bytes: hello_world_bytes(),
            context: task::WriteBlockContext::External("ectx01"),
        }),
        ScriptOp::Expect(ExpectOp::Idle),
        ScriptOp::Expect(ExpectOp::PollRequestAndInterpreter),
        // { 0: write task done @ 24 .. 85, 1: prep write done }
        ScriptOp::Do(DoOp::RequestAndInterpreterIncomingTaskDone {
            task_done: task::Done {
                current_offset: 85,
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
        // { 0: ready @ 24 .. 85, 1: write task in progress @ 85 }
        ScriptOp::Expect(ExpectOp::InterpretTask {
            expect_offset: 85,
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
        // { 0: ready @ 24 .. 85, 1: write task done @ 85 }
        ScriptOp::Do(DoOp::RequestAndInterpreterIncomingTaskDone {
            task_done: task::Done {
                current_offset: 130,
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
        // { 0: ready @ 24 .. 85, 1: ready @ 85 }
        ScriptOp::Expect(ExpectOp::PollRequest),

        // { 0: ready @ 24 .. 85, 0: delete req, 1: ready @ 85 }
        ScriptOp::Do(DoOp::RequestIncomingRequest {
            request: proto::Request::DeleteBlock(proto::RequestDeleteBlock { block_id: block::Id::init(), context: "ectx02", }),
        }),
        // { 0: ready @ 24 .. 85, 0: prep delete, 1: ready @ 85 }
        ScriptOp::Expect(ExpectOp::PrepareInterpretTaskDeleteBlock {
            expect_block_id: block::Id::init(),
            expect_context: task::DeleteBlockContext::External("ectx02"),
        }),
        ScriptOp::Expect(ExpectOp::PollRequest),
        // { 0: ready @ 24 .. 85, 0: prep delete done, 1: ready @ 85 }
        ScriptOp::Do(DoOp::RequestIncomingPreparedDeleteBlockDone {
            block_id: block::Id::init(),
            delete_block_bytes: hello_world_bytes(),
            context: task::DeleteBlockContext::External("ectx02"),
        }),
        ScriptOp::Expect(ExpectOp::Idle),
        // { 0: ready @ 24 .. 85, 0: delete task in progress @ 24, 1: ready @ 85 }
        ScriptOp::Expect(ExpectOp::InterpretTask {
            expect_offset: 24,
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
        // { 0: ready @ 24 .. 85, 0: delete task done @ 24, 1: ready @ 85 }
        ScriptOp::Do(DoOp::RequestAndInterpreterIncomingTaskDone {
            task_done: task::Done {
                current_offset: 24,
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

        // { 1: ready @ 85 }
        // defragmentation #0 has started
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
                    context: task::ReadBlockContext::Defrag(task::ReadBlockDefragContext {
                        defrag_gaps: DefragGaps::Both {
                            space_key_left: SpaceKey { space_available: 61, serial: 4, },
                            space_key_right: SpaceKey { space_available: 6, serial: 3 },
                        },
                    }),
                }),
            },
        }),
        ScriptOp::Do(DoOp::TaskAccept),
        ScriptOp::Expect(ExpectOp::PollRequestAndInterpreter),

        // request user block read as well (expect delay until defrag read)
        ScriptOp::Do(DoOp::RequestAndInterpreterIncomingRequest {
            request: proto::Request::ReadBlock(proto::RequestReadBlock { block_id: block::Id::init().next(), context: "ectx03", }),
        }),
        ScriptOp::Expect(ExpectOp::Idle),
        ScriptOp::Expect(ExpectOp::PollRequestAndInterpreter),
        // request user block write as well (expected to be immediately scheduled task to write and defrag should be canceled)
        ScriptOp::Do(DoOp::RequestAndInterpreterIncomingRequest {
            request: proto::Request::WriteBlock(proto::RequestWriteBlock {
                block_bytes: hello_bytes().freeze(),
                context: "ectx04",
            }),
        }),
        ScriptOp::Expect(ExpectOp::PrepareInterpretTaskWriteBlock {
            expect_block_id: block::Id::init().next().next(),
            expect_block_bytes: hello_bytes().freeze(),
            expect_context: task::WriteBlockContext::External("ectx04"),
        }),
        ScriptOp::Expect(ExpectOp::PollRequestAndInterpreter),
        ScriptOp::Do(DoOp::RequestAndInterpreterIncomingPreparedWriteBlockDone {
            block_id: block::Id::init().next().next(),
            write_block_bytes: hello_bytes(),
            context: task::WriteBlockContext::External("ectx04"),
        }),
        ScriptOp::Expect(ExpectOp::Idle),
        ScriptOp::Expect(ExpectOp::PollRequestAndInterpreter),
        // defrag #0 read done
        ScriptOp::Do(DoOp::RequestAndInterpreterIncomingTaskDone {
            task_done: task::Done {
                current_offset: 146,
                task: task::TaskDone {
                    block_id: block::Id::init().next(),
                    kind: task::TaskDoneKind::ReadBlock(task::TaskDoneReadBlock {
                        block_bytes: hello_world_bytes(),
                        context: task::ReadBlockContext::Defrag(task::ReadBlockDefragContext {
                            defrag_gaps: DefragGaps::Both {
                                space_key_left: SpaceKey { space_available: 61, serial: 4, },
                                space_key_right: SpaceKey { space_available: 6, serial: 3 },
                            },
                        }),
                    }),
                },
            },
        }),
        // defragmentation #0 proceeded with delete block
        ScriptOp::Expect(ExpectOp::Idle),
        ScriptOp::Expect(ExpectOp::PrepareInterpretTaskDeleteBlock {
            expect_block_id: block::Id::init().next(),
            expect_context: task::DeleteBlockContext::Defrag {
                block_bytes: hello_world_bytes().freeze(),
                defrag_gaps: DefragGaps::Both {
                    space_key_left: SpaceKey { space_available: 61, serial: 4, },
                    space_key_right: SpaceKey { space_available: 6, serial: 3 },
                },
            },
        }),
        ScriptOp::Expect(ExpectOp::Idle),
        // user read data immediately ready, proceed with process
        ScriptOp::Expect(ExpectOp::ProcessReadBlockTaskDone {
            expect_block_id: block::Id::init().next(),
            expect_block_bytes: hello_world_bytes().freeze(),
            expect_pending_contexts_key: "pk0",
        }),
        // proceed with user write
        ScriptOp::Expect(ExpectOp::InterpretTask {
            expect_offset: 24,
            expect_task: ExpectTask {
                block_id: block::Id::init().next().next(),
                kind: ExpectTaskKind::WriteBlock(ExpectTaskWriteBlock {
                    write_block_bytes: hello_bytes().freeze(),
                    commit: task::Commit::None,
                    context: task::WriteBlockContext::External("ectx04"),
                }),
            },
        }),
        ScriptOp::Do(DoOp::TaskAccept),
        ScriptOp::Expect(ExpectOp::PollRequestAndInterpreter),
        // defrag #0 delete block bytes ready
        ScriptOp::Do(DoOp::RequestAndInterpreterIncomingPreparedDeleteBlockDone {
            block_id: block::Id::init().next(),
            delete_block_bytes: hello_world_bytes(),
            context: task::DeleteBlockContext::Defrag {
                block_bytes: hello_world_bytes().freeze(),
                defrag_gaps: DefragGaps::Both {
                    space_key_left: SpaceKey { space_available: 61, serial: 4, },
                    space_key_right: SpaceKey { space_available: 6, serial: 3 },
                },
            },
        }),
        ScriptOp::Expect(ExpectOp::Idle),
        ScriptOp::Expect(ExpectOp::PollRequestAndInterpreter),
        // user write block task done
        ScriptOp::Do(DoOp::RequestAndInterpreterIncomingTaskDone {
            task_done: task::Done {
                current_offset: 78,
                task: task::TaskDone {
                    block_id: block::Id::init().next().next(),
                    kind: task::TaskDoneKind::WriteBlock(task::TaskDoneWriteBlock {
                        context: task::WriteBlockContext::External("ectx04"),
                    }),
                },
            },
        }),
        ScriptOp::Expect(ExpectOp::WriteBlockDone {
            expect_block_id: block::Id::init().next().next(),
            expect_context: "ectx04",
        }),
        // defragmentation #0 continue (delete task)
        ScriptOp::Expect(ExpectOp::InterpretTask {
            expect_offset: 85,
            expect_task: ExpectTask {
                block_id: block::Id::init().next(),
                kind: ExpectTaskKind::DeleteBlock(ExpectTaskDeleteBlock {
                    delete_block_bytes: hello_world_bytes().freeze(),
                    commit: task::Commit::WithTerminator,
                    context: task::DeleteBlockContext::Defrag {
                        defrag_gaps: DefragGaps::Both {
                            space_key_left: SpaceKey { space_available: 61, serial: 4, },
                            space_key_right: SpaceKey { space_available: 6, serial: 3 },
                        },
                        block_bytes: hello_world_bytes().freeze(),
                    },
                }),
            },
        }),
        ScriptOp::Do(DoOp::TaskAccept),
        ScriptOp::Expect(ExpectOp::PollRequestAndInterpreter),
        // defragmentation #0 continue (delete task ready)
        ScriptOp::Do(DoOp::RequestAndInterpreterIncomingTaskDone {
            task_done: task::Done {
                current_offset: 93,
                task: task::TaskDone {
                    block_id: block::Id::init().next(),
                    kind: task::TaskDoneKind::DeleteBlock(task::TaskDoneDeleteBlock {
                        context: task::DeleteBlockContext::Defrag {
                            defrag_gaps: DefragGaps::Both {
                                space_key_left: SpaceKey { space_available: 61, serial: 4, },
                                space_key_right: SpaceKey { space_available: 6, serial: 3 },
                            },
                            block_bytes: hello_world_bytes().freeze(),
                        },
                    }),
                },
            },
        }),
        ScriptOp::Expect(ExpectOp::Idle),
        // defragmentation #0 continue (write task)
        ScriptOp::Expect(ExpectOp::InterpretTask {
            expect_offset: 78,
            expect_task: ExpectTask {
                block_id: block::Id::init().next(),
                kind: ExpectTaskKind::WriteBlock(ExpectTaskWriteBlock {
                    write_block_bytes: hello_world_bytes().freeze(),
                    commit: task::Commit::WithTerminator,
                    context: task::WriteBlockContext::Defrag,
                }),
            },
        }),
        ScriptOp::Do(DoOp::TaskAccept),
        ScriptOp::Expect(ExpectOp::PollRequestAndInterpreter),
        ScriptOp::Do(DoOp::RequestAndInterpreterIncomingTaskDone {
            task_done: task::Done {
                current_offset: 139,
                task: task::TaskDone {
                    block_id: block::Id::init().next(),
                    kind: task::TaskDoneKind::WriteBlock(task::TaskDoneWriteBlock {
                        context: task::WriteBlockContext::Defrag,
                    }),
                },
            },
        }),
        ScriptOp::Expect(ExpectOp::Idle),
        ScriptOp::Expect(ExpectOp::PollRequest),
    ];

    interpret(performer, script)
}
