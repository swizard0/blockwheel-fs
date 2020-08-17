use super::{
    task,
    proto,
    block,
    init,
    interpret,
    hello_world_bytes,
    ScriptOp,
    ExpectOp,
    DoOp,
    ExpectTask,
    ExpectTaskKind,
    ExpectTaskWriteBlock,
    ExpectTaskReadBlock,
    ExpectTaskDeleteBlock,
};

#[test]
fn script_basic() {
    let performer = init();
    let script = vec![
        ScriptOp::Expect(ExpectOp::PollRequest),
        ScriptOp::Do(DoOp::RequestIncomingRequest {
            request: proto::Request::LendBlock(proto::RequestLendBlock { context: "ectx00", }),
        }),
        ScriptOp::Expect(ExpectOp::LendBlockSuccess {
            expect_context: "ectx00",
        }),
        ScriptOp::Expect(ExpectOp::PollRequest),
        ScriptOp::Do(DoOp::RequestIncomingRequest {
            request: proto::Request::RepayBlock(proto::RequestRepayBlock { block_bytes: hello_world_bytes(), }),
        }),
        ScriptOp::Expect(ExpectOp::Idle),
        ScriptOp::Expect(ExpectOp::PollRequest),
        ScriptOp::Do(DoOp::RequestIncomingRequest {
            request: proto::Request::ReadBlock(proto::RequestReadBlock { block_id: block::Id::init(), context: "ectx01", }),
        }),
        ScriptOp::Expect(ExpectOp::ReadBlockNotFound {
            expect_context: "ectx01",
        }),
        ScriptOp::Expect(ExpectOp::PollRequest),
        ScriptOp::Do(DoOp::RequestIncomingRequest {
            request: proto::Request::WriteBlock(proto::RequestWriteBlock { block_bytes: hello_world_bytes(), context: "ectx02", }),
        }),
        ScriptOp::Expect(ExpectOp::Idle),
        ScriptOp::Expect(ExpectOp::InterpretTask {
            expect_offset: 24,
            expect_task: ExpectTask {
                block_id: block::Id::init(),
                kind: ExpectTaskKind::WriteBlock(ExpectTaskWriteBlock {
                    block_bytes: hello_world_bytes(),
                    commit_type: task::CommitType::CommitAndEof,
                    context: task::WriteBlockContext::External("ectx02"),
                }),
            },
        }),
        ScriptOp::Do(DoOp::TaskAccept { interpreter_context: "ictx00", }),
        ScriptOp::Expect(ExpectOp::PollRequestAndInterpreter {
            expect_context: "ictx00",
        }),
        ScriptOp::Do(DoOp::RequestAndInterpreterIncomingRequest {
            request: proto::Request::ReadBlock(proto::RequestReadBlock { block_id: block::Id::init(), context: "ectx03", }),
            interpreter_context: "ictx01",
        }),
        ScriptOp::Expect(ExpectOp::ReadBlockDone {
            expect_block_bytes: hello_world_bytes(),
            expect_context: "ectx03",
        }),
        ScriptOp::Expect(ExpectOp::PollRequestAndInterpreter {
            expect_context: "ictx01",
        }),
        ScriptOp::Do(DoOp::RequestAndInterpreterIncomingRequest {
            request: proto::Request::DeleteBlock(proto::RequestDeleteBlock { block_id: block::Id::init(), context: "ectx04", }),
            interpreter_context: "ictx02",
        }),
        ScriptOp::Expect(ExpectOp::Idle),
        ScriptOp::Expect(ExpectOp::PollRequestAndInterpreter {
            expect_context: "ictx02",
        }),
        ScriptOp::Do(DoOp::RequestAndInterpreterIncomingRequest {
            request: proto::Request::WriteBlock(proto::RequestWriteBlock { block_bytes: hello_world_bytes(), context: "ectx05", }),
            interpreter_context: "ictx03",
        }),
        ScriptOp::Expect(ExpectOp::Idle),
        ScriptOp::Expect(ExpectOp::PollRequestAndInterpreter {
            expect_context: "ictx03",
        }),
        ScriptOp::Do(DoOp::RequestAndInterpreterIncomingTaskDone {
            task_done: task::Done {
                current_offset: 77,
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
        ScriptOp::Expect(ExpectOp::InterpretTask {
            expect_offset: 77,
            expect_task: ExpectTask {
                block_id: block::Id::init().next(),
                kind: ExpectTaskKind::WriteBlock(ExpectTaskWriteBlock {
                    block_bytes: hello_world_bytes(),
                    commit_type: task::CommitType::CommitAndEof,
                    context: task::WriteBlockContext::External("ectx05"),
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
        ScriptOp::Expect(ExpectOp::InterpretTask {
            expect_offset: 24,
            expect_task: ExpectTask {
                block_id: block::Id::init(),
                kind: ExpectTaskKind::DeleteBlock(ExpectTaskDeleteBlock {
                    context: task::DeleteBlockContext::External("ectx04"),
                }),
            },
        }),
        ScriptOp::Do(DoOp::TaskAccept { interpreter_context: "ictx05", }),
        ScriptOp::Expect(ExpectOp::PollRequestAndInterpreter {
            expect_context: "ictx05",
        }),
        ScriptOp::Do(DoOp::RequestAndInterpreterIncomingRequest {
            request: proto::Request::ReadBlock(proto::RequestReadBlock { block_id: block::Id::init(), context: "ectx06", }),
            interpreter_context: "ictx06",
        }),
        ScriptOp::Expect(ExpectOp::Idle),
        ScriptOp::Expect(ExpectOp::PollRequestAndInterpreter {
            expect_context: "ictx06",
        }),
        ScriptOp::Do(DoOp::RequestAndInterpreterIncomingRequest {
            request: proto::Request::DeleteBlock(proto::RequestDeleteBlock { block_id: block::Id::init(), context: "ectx07", }),
            interpreter_context: "ictx07",
        }),
        ScriptOp::Expect(ExpectOp::Idle),
        ScriptOp::Expect(ExpectOp::PollRequestAndInterpreter {
            expect_context: "ictx07",
        }),
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
        ScriptOp::Expect(ExpectOp::ReadBlockNotFound {
            expect_context: "ectx06",
        }),
        ScriptOp::Expect(ExpectOp::DeleteBlockNotFound {
            expect_context: "ectx07",
        }),


//         ScriptOp::PerformerNext,
//         ScriptOp::ExpectPollRequest,
//         ScriptOp::RequestIncomingRequest {
//             request: proto::Request::WriteBlock(proto::RequestWriteBlock { block_bytes: hello_world_bytes(), context: "ctx0f", }),
//         },
//         ScriptOp::PerformerNext,
//         ScriptOp::ExpectInterpretTask {
//             expect_offset: 24,
//             expect_task: ExpectTask {
//                 block_id: block::Id::init().next().next(),
//                 kind: ExpectTaskKind::WriteBlock(ExpectTaskWriteBlock {
//                     block_bytes: hello_world_bytes(),
//                     commit_type: task::CommitType::CommitOnly,
//                     context: task::WriteBlockContext::External("ctx0f"),
//                 }),
//             },
//         },
//         ScriptOp::TaskAccepted { interpreter_context: "ctx10", },
//         ScriptOp::PerformerNext,
//         ScriptOp::ExpectPollRequestAndInterpreter {
//             expect_context: "ctx10",
//         },
//         ScriptOp::RequestAndInterpreterIncomingRequest {
//             request: proto::Request::WriteBlock(proto::RequestWriteBlock { block_bytes: hello_world_bytes(), context: "ctx11", }),
//             interpreter_context: "ctx12",
//         },
//         ScriptOp::ExpectWriteBlockNoSpaceLeft {
//             expect_context: "ctx11",
//         },
//         ScriptOp::PerformerNext,
//         ScriptOp::ExpectPollRequestAndInterpreter {
//             expect_context: "ctx12",
//         },
//         ScriptOp::RequestAndInterpreterIncomingRequest {
//             request: proto::Request::ReadBlock(proto::RequestReadBlock { block_id: block::Id::init().next(), context: "ctx13", }),
//             interpreter_context: "ctx14",
//         },
//         ScriptOp::PerformerNext,
//         ScriptOp::ExpectPollRequestAndInterpreter {
//             expect_context: "ctx14",
//         },
//         ScriptOp::RequestAndInterpreterIncomingTaskDone {
//             task_done: task::Done {
//                 current_offset: 24,
//                 task: task::TaskDone {
//                     block_id: block::Id::init().next().next(),
//                     kind: task::TaskDoneKind::WriteBlock(task::TaskDoneWriteBlock {
//                         context: task::WriteBlockContext::External("ctx11"),
//                     }),
//                 },
//             },
//         },
//         ScriptOp::ExpectWriteBlockDoneDone {
//             expect_block_id: block::Id::init().next().next(),
//             expect_context: "ctx11",
//         },
//         ScriptOp::PerformerNext,
//         ScriptOp::ExpectInterpretTask {
//             expect_offset: 77,
//             expect_task: ExpectTask {
//                 block_id: block::Id::init().next(),
//                 kind: ExpectTaskKind::ReadBlock(ExpectTaskReadBlock {
//                     block_header: storage::BlockHeader {
//                         block_id: block::Id::init().next(),
//                         block_size: 13,
//                         ..Default::default()
//                     },
//                     context: task::ReadBlockContext::External("ctx13"),
//                 }),
//             },
//         },
//         ScriptOp::TaskAccepted { interpreter_context: "ctx15", },
//         ScriptOp::PerformerNext,
//         ScriptOp::ExpectPollRequestAndInterpreter {
//             expect_context: "ctx15",
//         },
//         ScriptOp::RequestAndInterpreterIncomingTaskDone {
//             task_done: task::Done {
//                 current_offset: 77,
//                 task: task::TaskDone {
//                     block_id: block::Id::init().next(),
//                     kind: task::TaskDoneKind::ReadBlock(task::TaskDoneReadBlock {
//                         block_bytes: hello_world_bytes().into_mut().unwrap(),
//                         context: task::ReadBlockContext::External("ctx13"),
//                     }),
//                 },
//             },
//         },
//         ScriptOp::ExpectReadBlockDoneDone {
//             expect_block_bytes: hello_world_bytes(),
//             expect_context: "ctx13",
//         },
    ];

    interpret(performer, script)
}
