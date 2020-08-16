use super::{
    init,
    interpret,
    ScriptOp,
};

#[test]
fn script_basic() {
    let performer = init();
    let script = vec![
        ScriptOp::ExpectPollRequest,
//         ScriptOp::RequestIncomingRequest {
//             request: proto::Request::LendBlock(proto::RequestLendBlock { context: "ctx00", }),
//         },
//         ScriptOp::ExpectLendBlockSuccess {
//             expect_context: "ctx00",
//         },
//         ScriptOp::PerformerNext,
//         ScriptOp::ExpectPollRequest,
//         ScriptOp::RequestIncomingRequest {
//             request: proto::Request::RepayBlock(proto::RequestRepayBlock { block_bytes: hello_world_bytes(), }),
//         },
//         ScriptOp::PerformerNext,
//         ScriptOp::ExpectPollRequest,
//         ScriptOp::RequestIncomingRequest {
//             request: proto::Request::ReadBlock(proto::RequestReadBlock { block_id: block::Id::init(), context: "ctx01", }),
//         },
//         ScriptOp::ExpectReadBlockNotFound {
//             expect_context: "ctx01",
//         },
//         ScriptOp::PerformerNext,
//         ScriptOp::ExpectPollRequest,
//         ScriptOp::RequestIncomingRequest {
//             request: proto::Request::WriteBlock(proto::RequestWriteBlock { block_bytes: hello_world_bytes(), context: "ctx02", }),
//         },
//         ScriptOp::PerformerNext,
//         ScriptOp::ExpectInterpretTask {
//             expect_offset: 24,
//             expect_task: ExpectTask {
//                 block_id: block::Id::init(),
//                 kind: ExpectTaskKind::WriteBlock(ExpectTaskWriteBlock {
//                     block_bytes: hello_world_bytes(),
//                     commit_type: task::CommitType::CommitAndEof,
//                     context: task::WriteBlockContext::External("ctx02"),
//                 }),
//             },
//         },
//         ScriptOp::TaskAccepted { interpreter_context: "ctx03", },
//         ScriptOp::PerformerNext,
//         ScriptOp::ExpectPollRequestAndInterpreter {
//             expect_context: "ctx03",
//         },
//         ScriptOp::RequestAndInterpreterIncomingRequest {
//             request: proto::Request::ReadBlock(proto::RequestReadBlock { block_id: block::Id::init(), context: "ctx04", }),
//             interpreter_context: "ctx05",
//         },
//         ScriptOp::ExpectReadBlockCacheHit {
//             expect_block_bytes: hello_world_bytes(),
//             expect_context: "ctx04",
//         },
//         ScriptOp::PerformerNext,
//         ScriptOp::ExpectPollRequestAndInterpreter {
//             expect_context: "ctx05",
//         },
//         ScriptOp::RequestAndInterpreterIncomingRequest {
//             request: proto::Request::DeleteBlock(proto::RequestDeleteBlock { block_id: block::Id::init(), context: "ctx06", }),
//             interpreter_context: "ctx07",
//         },
//         ScriptOp::PerformerNext,
//         ScriptOp::ExpectPollRequestAndInterpreter {
//             expect_context: "ctx07",
//         },
//         ScriptOp::RequestAndInterpreterIncomingRequest {
//             request: proto::Request::WriteBlock(proto::RequestWriteBlock { block_bytes: hello_world_bytes(), context: "ctx08", }),
//             interpreter_context: "ctx09",
//         },
//         ScriptOp::PerformerNext,
//         ScriptOp::ExpectPollRequestAndInterpreter {
//             expect_context: "ctx09",
//         },
//         ScriptOp::RequestAndInterpreterIncomingTaskDone {
//             task_done: task::Done {
//                 current_offset: 77,
//                 task: task::TaskDone {
//                     block_id: block::Id::init(),
//                     kind: task::TaskDoneKind::WriteBlock(task::TaskDoneWriteBlock {
//                         context: task::WriteBlockContext::External("ctx02"),
//                     }),
//                 },
//             },
//         },
//         ScriptOp::ExpectWriteBlockDoneDone {
//             expect_block_id: block::Id::init(),
//             expect_context: "ctx02",
//         },
//         ScriptOp::PerformerNext,
//         ScriptOp::ExpectInterpretTask {
//             expect_offset: 77,
//             expect_task: ExpectTask {
//                 block_id: block::Id::init().next(),
//                 kind: ExpectTaskKind::WriteBlock(ExpectTaskWriteBlock {
//                     block_bytes: hello_world_bytes(),
//                     commit_type: task::CommitType::CommitAndEof,
//                     context: task::WriteBlockContext::External("ctx08"),
//                 }),
//             },
//         },
//         ScriptOp::TaskAccepted { interpreter_context: "ctx09", },
//         ScriptOp::PerformerNext,
//         ScriptOp::ExpectPollRequestAndInterpreter {
//             expect_context: "ctx09",
//         },
//         ScriptOp::RequestAndInterpreterIncomingTaskDone {
//             task_done: task::Done {
//                 current_offset: 130,
//                 task: task::TaskDone {
//                     block_id: block::Id::init().next(),
//                     kind: task::TaskDoneKind::WriteBlock(task::TaskDoneWriteBlock {
//                         context: task::WriteBlockContext::External("ctx08"),
//                     }),
//                 },
//             },
//         },
//         ScriptOp::ExpectWriteBlockDoneDone {
//             expect_block_id: block::Id::init().next(),
//             expect_context: "ctx08",
//         },
//         ScriptOp::PerformerNext,
//         ScriptOp::ExpectInterpretTask {
//             expect_offset: 24,
//             expect_task: ExpectTask {
//                 block_id: block::Id::init(),
//                 kind: ExpectTaskKind::DeleteBlock(ExpectTaskDeleteBlock {
//                     context: task::DeleteBlockContext::External("ctx06"),
//                 }),
//             },
//         },
//         ScriptOp::TaskAccepted { interpreter_context: "ctx0a", },
//         ScriptOp::PerformerNext,
//         ScriptOp::ExpectPollRequestAndInterpreter {
//             expect_context: "ctx0a",
//         },
//         ScriptOp::RequestAndInterpreterIncomingRequest {
//             request: proto::Request::ReadBlock(proto::RequestReadBlock { block_id: block::Id::init(), context: "ctx0b", }),
//             interpreter_context: "ctx0c",
//         },
//         ScriptOp::ExpectReadBlockNotFound { // here
//             expect_context: "ctx0b",
//         },
//         ScriptOp::PerformerNext,
//         ScriptOp::ExpectPollRequestAndInterpreter {
//             expect_context: "ctx0c",
//         },
//         ScriptOp::RequestAndInterpreterIncomingRequest {
//             request: proto::Request::DeleteBlock(proto::RequestDeleteBlock { block_id: block::Id::init(), context: "ctx0d", }),
//             interpreter_context: "ctx0e",
//         },
//         ScriptOp::ExpectDeleteBlockNotFound {
//             expect_context: "ctx0d",
//         },
//         ScriptOp::PerformerNext,
//         ScriptOp::ExpectPollRequestAndInterpreter {
//             expect_context: "ctx0e",
//         },
//         ScriptOp::RequestAndInterpreterIncomingTaskDone {
//             task_done: task::Done {
//                 current_offset: 24,
//                 task: task::TaskDone {
//                     block_id: block::Id::init(),
//                     kind: task::TaskDoneKind::DeleteBlock(task::TaskDoneDeleteBlock {
//                         context: task::DeleteBlockContext::External("ctx06"),
//                     }),
//                 },
//             },
//         },
//         ScriptOp::ExpectDeleteBlockDoneDone {
//             expect_context: "ctx06",
//         },
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
