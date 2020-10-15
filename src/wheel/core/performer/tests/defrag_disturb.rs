use super::{
    task,
    proto,
    block,
    storage,
    with_defrag_config,
    hello_world_bytes,
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
        ScriptOp::Expect(ExpectOp::PollRequest),
        ScriptOp::Do(DoOp::RequestIncomingRequest {
            request: proto::Request::WriteBlock(proto::RequestWriteBlock { block_bytes: hello_world_bytes(), context: "ectx00", }),
        }),
        ScriptOp::Expect(ExpectOp::Idle),
        ScriptOp::Expect(ExpectOp::InterpretTask {
            expect_offset: 24,
            expect_task: ExpectTask {
                block_id: block::Id::init(),
                kind: ExpectTaskKind::WriteBlock(ExpectTaskWriteBlock {
                    block_bytes: hello_world_bytes(),
                    context: task::WriteBlockContext::External("ectx00"),
                }),
            },
        }),
        ScriptOp::Do(DoOp::TaskAccept { interpreter_context: "ictx00", }),
        ScriptOp::Expect(ExpectOp::PollRequestAndInterpreter {
            expect_context: "ictx00",
        }),
        ScriptOp::Do(DoOp::RequestAndInterpreterIncomingRequest {
            request: proto::Request::WriteBlock(proto::RequestWriteBlock { block_bytes: hello_world_bytes(), context: "ectx01", }),
            interpreter_context: "ictx01",
        }),
        ScriptOp::Expect(ExpectOp::Idle),
        ScriptOp::Expect(ExpectOp::PollRequestAndInterpreter {
            expect_context: "ictx01",
        }),
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
        ScriptOp::Expect(ExpectOp::InterpretTask {
            expect_offset: 85,
            expect_task: ExpectTask {
                block_id: block::Id::init().next(),
                kind: ExpectTaskKind::WriteBlock(ExpectTaskWriteBlock {
                    block_bytes: hello_world_bytes(),
                    context: task::WriteBlockContext::External("ectx01"),
                }),
            },
        }),
        ScriptOp::Do(DoOp::TaskAccept { interpreter_context: "ictx02", }),
        ScriptOp::Expect(ExpectOp::PollRequestAndInterpreter {
            expect_context: "ictx02",
        }),
        ScriptOp::Do(DoOp::RequestAndInterpreterIncomingTaskDone {
            task_done: task::Done {
                current_offset: 146,
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
        ScriptOp::Expect(ExpectOp::PollRequest),
        ScriptOp::Do(DoOp::RequestIncomingRequest {
            request: proto::Request::DeleteBlock(proto::RequestDeleteBlock { block_id: block::Id::init(), context: "ectx02", }),
        }),
        ScriptOp::Expect(ExpectOp::Idle),
        ScriptOp::Expect(ExpectOp::InterpretTask {
            expect_offset: 24,
            expect_task: ExpectTask {
                block_id: block::Id::init(),
                kind: ExpectTaskKind::DeleteBlock(ExpectTaskDeleteBlock {
                    context: task::DeleteBlockContext::External("ectx02"),
                }),
            },
        }),
        ScriptOp::Do(DoOp::TaskAccept { interpreter_context: "ictx03", }),
        ScriptOp::Expect(ExpectOp::PollRequestAndInterpreter {
            expect_context: "ictx03",
        }),
        ScriptOp::Do(DoOp::RequestAndInterpreterIncomingTaskDone {
            task_done: task::Done {
                current_offset: 32,
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
        // defragmentation has started (read task)
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
                    context: task::ReadBlockContext::Defrag {
                        defrag_gaps: DefragGaps::Both {
                            space_key_left: SpaceKey { space_available: 61, serial: 4, },
                            space_key_right: SpaceKey { space_available: 14, serial: 3 },
                        },
                    },
                }),
            },
        }),
        ScriptOp::Do(DoOp::TaskAccept { interpreter_context: "ictx04", }),
        ScriptOp::Expect(ExpectOp::PollRequestAndInterpreter {
            expect_context: "ictx04",
        }),
        // request user block read as well (expect delay until defrag read)
        ScriptOp::Do(DoOp::RequestAndInterpreterIncomingRequest {
            request: proto::Request::ReadBlock(proto::RequestReadBlock { block_id: block::Id::init().next(), context: "ectx03", }),
            interpreter_context: "ictx05",
        }),
        ScriptOp::Expect(ExpectOp::Idle),
        ScriptOp::Expect(ExpectOp::PollRequestAndInterpreter {
            expect_context: "ictx05",
        }),
        // request user block write as well (expected to be immediately scheduled task to write and defrag should be canceled)
        ScriptOp::Do(DoOp::RequestAndInterpreterIncomingRequest {
            request: proto::Request::WriteBlock(proto::RequestWriteBlock { block_bytes: hello_bytes(), context: "ectx04", }),
            interpreter_context: "ictx06",
        }),
        ScriptOp::Expect(ExpectOp::Idle),
        ScriptOp::Expect(ExpectOp::PollRequestAndInterpreter {
            expect_context: "ictx06",
        }),
        // defrag read done (defrag delete should be canceled here)
        ScriptOp::Do(DoOp::RequestAndInterpreterIncomingTaskDone {
            task_done: task::Done {
                current_offset: 85,
                task: task::TaskDone {
                    block_id: block::Id::init().next(),
                    kind: task::TaskDoneKind::ReadBlock(task::TaskDoneReadBlock {
                        block_bytes: hello_world_bytes().into_mut().unwrap(),
                        context: task::ReadBlockContext::Defrag {
                            defrag_gaps: DefragGaps::Both {
                                space_key_left: SpaceKey { space_available: 61, serial: 4, },
                                space_key_right: SpaceKey { space_available: 14, serial: 3 },
                            },
                        },
                    }),
                },
            },
        }),
        ScriptOp::Expect(ExpectOp::Idle),
        ScriptOp::Expect(ExpectOp::ReadBlockDone {
            expect_block_bytes: hello_world_bytes(),
            expect_context: "ectx03",
        }),
        // new defragmentation has started with adjusted parameters (read task)
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
                    context: task::ReadBlockContext::Defrag {
                        defrag_gaps: DefragGaps::Both {
                            space_key_left: SpaceKey { space_available: 7, serial: 5, },
                            space_key_right: SpaceKey { space_available: 14, serial: 3 },
                        },
                    },
                }),
            },
        }),
        ScriptOp::Do(DoOp::TaskAccept { interpreter_context: "ictx07", }),
        ScriptOp::Expect(ExpectOp::PollRequestAndInterpreter {
            expect_context: "ictx07",
        }),
        // defrag read done, start delete task
        ScriptOp::Do(DoOp::RequestAndInterpreterIncomingTaskDone {
            task_done: task::Done {
                current_offset: 85,
                task: task::TaskDone {
                    block_id: block::Id::init().next(),
                    kind: task::TaskDoneKind::ReadBlock(task::TaskDoneReadBlock {
                        block_bytes: hello_world_bytes().into_mut().unwrap(),
                        context: task::ReadBlockContext::Defrag {
                            defrag_gaps: DefragGaps::Both {
                                space_key_left: SpaceKey { space_available: 7, serial: 5, },
                                space_key_right: SpaceKey { space_available: 14, serial: 3 },
                            },
                        },
                    }),
                },
            },
        }),
        ScriptOp::Expect(ExpectOp::Idle),
        // defragmentation continue (delete task)
        ScriptOp::Expect(ExpectOp::InterpretTask {
            expect_offset: 85,
            expect_task: ExpectTask {
                block_id: block::Id::init().next(),
                kind: ExpectTaskKind::DeleteBlock(ExpectTaskDeleteBlock {
                    context: task::DeleteBlockContext::Defrag {
                        defrag_gaps: DefragGaps::Both {
                            space_key_left: SpaceKey { space_available: 7, serial: 5, },
                            space_key_right: SpaceKey { space_available: 14, serial: 3 },
                        },
                    },
                }),
            },
        }),
        ScriptOp::Do(DoOp::TaskAccept { interpreter_context: "ictx08", }),
        ScriptOp::Expect(ExpectOp::PollRequestAndInterpreter {
            expect_context: "ictx08",
        }),
        // request user block read as well (expect instant cached response)
        ScriptOp::Do(DoOp::RequestAndInterpreterIncomingRequest {
            request: proto::Request::ReadBlock(proto::RequestReadBlock { block_id: block::Id::init().next(), context: "ectx05", }),
            interpreter_context: "ictx09",
        }),
        ScriptOp::Expect(ExpectOp::ReadBlockDone {
            expect_block_bytes: hello_world_bytes(),
            expect_context: "ectx05",
        }),
        ScriptOp::Expect(ExpectOp::PollRequestAndInterpreter {
            expect_context: "ictx09",
        }),
        ScriptOp::Do(DoOp::RequestAndInterpreterIncomingTaskDone {
            task_done: task::Done {
                current_offset: 93,
                task: task::TaskDone {
                    block_id: block::Id::init().next(),
                    kind: task::TaskDoneKind::DeleteBlock(task::TaskDoneDeleteBlock {
                        context: task::DeleteBlockContext::Defrag {
                            defrag_gaps: DefragGaps::Both {
                                space_key_left: SpaceKey { space_available: 7, serial: 5, },
                                space_key_right: SpaceKey { space_available: 14, serial: 3 },
                            },
                        },
                    }),
                },
            },
        }),
        ScriptOp::Expect(ExpectOp::Idle),
        // proceed with user write
        ScriptOp::Expect(ExpectOp::InterpretTask {
            expect_offset: 24,
            expect_task: ExpectTask {
                block_id: block::Id::init().next().next(),
                kind: ExpectTaskKind::WriteBlock(ExpectTaskWriteBlock {
                    block_bytes: hello_bytes(),
                    context: task::WriteBlockContext::External("ectx04"),
                }),
            },
        }),
        ScriptOp::Do(DoOp::TaskAccept { interpreter_context: "ictx0a", }),
        ScriptOp::Expect(ExpectOp::PollRequestAndInterpreter {
            expect_context: "ictx0a",
        }),
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
        // defragmentation continue (write task)
        ScriptOp::Expect(ExpectOp::InterpretTask {
            expect_offset: 78,
            expect_task: ExpectTask {
                block_id: block::Id::init().next(),
                kind: ExpectTaskKind::WriteBlock(ExpectTaskWriteBlock {
                    block_bytes: hello_world_bytes(),
                    context: task::WriteBlockContext::Defrag,
                }),
            },
        }),
        ScriptOp::Do(DoOp::TaskAccept { interpreter_context: "ictx0b", }),
        ScriptOp::Expect(ExpectOp::PollRequestAndInterpreter {
            expect_context: "ictx0b",
        }),
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
