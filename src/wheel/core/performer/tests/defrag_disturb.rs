use super::{
    task,
    proto,
    block,
    storage,
    with_defrag_config,
    hello_world_bytes,
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

use crate::wheel::core::SpaceKey;

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
                    commit_type: task::CommitType::CommitAndEof,
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
                current_offset: 77,
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
            expect_offset: 77,
            expect_task: ExpectTask {
                block_id: block::Id::init().next(),
                kind: ExpectTaskKind::WriteBlock(ExpectTaskWriteBlock {
                    block_bytes: hello_world_bytes(),
                    commit_type: task::CommitType::CommitAndEof,
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
        // defragmentation has started (read task)
        ScriptOp::Expect(ExpectOp::InterpretTask {
            expect_offset: 77,
            expect_task: ExpectTask {
                block_id: block::Id::init().next(),
                kind: ExpectTaskKind::ReadBlock(ExpectTaskReadBlock {
                    block_header: storage::BlockHeader {
                        block_id: block::Id::init().next(),
                        block_size: 13,
                        ..Default::default()
                    },
                    context: task::ReadBlockContext::Defrag {
                        space_key: SpaceKey { space_available: 53, serial: 4, },
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
        // request user block write as well (expected to be put in pending queue)
        ScriptOp::Do(DoOp::RequestAndInterpreterIncomingRequest {
            request: proto::Request::WriteBlock(proto::RequestWriteBlock { block_bytes: hello_world_bytes(), context: "ectx04", }),
            interpreter_context: "ictx06",
        }),
        ScriptOp::Expect(ExpectOp::Idle),
        ScriptOp::Expect(ExpectOp::PollRequestAndInterpreter {
            expect_context: "ictx06",
        }),
        // defrag read done
        ScriptOp::Do(DoOp::RequestAndInterpreterIncomingTaskDone {
            task_done: task::Done {
                current_offset: 77,
                task: task::TaskDone {
                    block_id: block::Id::init().next(),
                    kind: task::TaskDoneKind::ReadBlock(task::TaskDoneReadBlock {
                        block_bytes: hello_world_bytes().into_mut().unwrap(),
                        context: task::ReadBlockContext::Defrag {
                            space_key: SpaceKey { space_available: 53, serial: 4, },
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
        ScriptOp::Expect(ExpectOp::Idle),
        // defragmentation continue (delete task)
        ScriptOp::Expect(ExpectOp::InterpretTask {
            expect_offset: 77,
            expect_task: ExpectTask {
                block_id: block::Id::init().next(),
                kind: ExpectTaskKind::DeleteBlock(ExpectTaskDeleteBlock {
                    context: task::DeleteBlockContext::Defrag {
                        space_key: SpaceKey { space_available: 53, serial: 4, },
                    },
                }),
            },
        }),
        ScriptOp::Do(DoOp::TaskAccept { interpreter_context: "ictx07", }),
        ScriptOp::Expect(ExpectOp::PollRequestAndInterpreter {
            expect_context: "ictx07",
        }),
        // request user block read as well (expect instant cached response)
        ScriptOp::Do(DoOp::RequestAndInterpreterIncomingRequest {
            request: proto::Request::ReadBlock(proto::RequestReadBlock { block_id: block::Id::init().next(), context: "ectx05", }),
            interpreter_context: "ictx08",
        }),
        ScriptOp::Expect(ExpectOp::ReadBlockDone {
            expect_block_bytes: hello_world_bytes(),
            expect_context: "ectx05",
        }),
        ScriptOp::Expect(ExpectOp::PollRequestAndInterpreter {
            expect_context: "ictx08",
        }),
        ScriptOp::Do(DoOp::RequestAndInterpreterIncomingTaskDone {
            task_done: task::Done {
                current_offset: 77,
                task: task::TaskDone {
                    block_id: block::Id::init().next(),
                    kind: task::TaskDoneKind::DeleteBlock(task::TaskDoneDeleteBlock {
                        context: task::DeleteBlockContext::Defrag {
                            space_key: SpaceKey { space_available: 53, serial: 4, },
                        },
                    }),
                },
            },
        }),
        ScriptOp::Expect(ExpectOp::Idle),
        ScriptOp::Expect(ExpectOp::Idle),
        // defragmentation continue (write task)
        ScriptOp::Expect(ExpectOp::InterpretTask {
            expect_offset: 24,
            expect_task: ExpectTask {
                block_id: block::Id::init().next(),
                kind: ExpectTaskKind::WriteBlock(ExpectTaskWriteBlock {
                    block_bytes: hello_world_bytes(),
                    commit_type: task::CommitType::CommitAndEof,
                    context: task::WriteBlockContext::Defrag,
                }),
            },
        }),
        ScriptOp::Do(DoOp::TaskAccept { interpreter_context: "ictx09", }),
        ScriptOp::Expect(ExpectOp::PollRequestAndInterpreter {
            expect_context: "ictx09",
        }),
        ScriptOp::Do(DoOp::RequestAndInterpreterIncomingTaskDone {
            task_done: task::Done {
                current_offset: 77,
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
        // TODO: where is user write block ectx04 ??
    ];

    interpret(performer, script)
}
