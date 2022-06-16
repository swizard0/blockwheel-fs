
use crate::{
    job,
    wheel::{
        interpret,
        core::{
            performer,
        },
    },
    blockwheel_context::{
        Context,
    },
};

pub struct Common {
//    pub interpreter_pid: interpret::Pid<Context>,
}

pub struct RunJobArgs {
    pub common: Common,
    pub kont: Kont,
}

pub enum Kont {
    Init {
        performer: performer::Performer<Context>,
    },
}

pub struct RunJobDone {
}

pub type RunJobOutput = RunJobDone;

pub fn run_job(
    RunJobArgs {
        mut common,
        kont,
    }: RunJobArgs,
)
    -> RunJobOutput
{
    let mut op = match kont {
        Kont::Init { performer, } =>
            performer.next(),
    };

    loop {
        op = match op {

            performer::Op::Idle(performer) =>
                performer.next(),

            performer::Op::Query(performer::QueryOp::PollRequestAndInterpreter(poll)) => {

                todo!();
            },

            performer::Op::Query(performer::QueryOp::PollRequest(poll)) => {

                todo!();
            },

            performer::Op::Query(performer::QueryOp::InterpretTask(performer::InterpretTask { offset, task, next, })) => {

                todo!();
            },

            performer::Op::Query(performer::QueryOp::MakeIterBlocksStream(performer::MakeIterBlocksStream {
                blocks_total_count,
                blocks_total_size,
                iter_blocks_context: reply_tx,
                next,
            })) => {

                todo!();
            },

            performer::Op::Event(performer::Event {
                op: performer::EventOp::Info(
                    performer::TaskDoneOp { context: reply_tx, op: performer::InfoOp::Success { info, }, },
                ),
                performer,
            }) => {

                todo!();
            },

            performer::Op::Event(performer::Event {
                op: performer::EventOp::Flush(
                    performer::TaskDoneOp { context: reply_tx, op: performer::FlushOp::Flushed, },
                ),
                performer,
            }) => {

                todo!();
            },

            performer::Op::Event(performer::Event {
                op: performer::EventOp::WriteBlock(
                    performer::TaskDoneOp { context: reply_tx, op: performer::WriteBlockOp::NoSpaceLeft, },
                ),
                performer,
            }) => {

                todo!();
            },

            performer::Op::Event(performer::Event {
                op: performer::EventOp::WriteBlock(
                    performer::TaskDoneOp { context: reply_tx, op: performer::WriteBlockOp::Done { block_id, }, },
                ),
                performer,
            }) => {

                todo!();
            },

            performer::Op::Event(performer::Event {
                op: performer::EventOp::ReadBlock(
                    performer::TaskDoneOp { context: reply_tx, op: performer::ReadBlockOp::NotFound, },
                ),
                performer,
            }) => {

                todo!();
            },

            performer::Op::Event(performer::Event {
                op: performer::EventOp::ReadBlock(
                    performer::TaskDoneOp { context: reply_tx, op: performer::ReadBlockOp::Done { block_bytes, }, },
                ),
                performer,
            }) => {

                todo!();
            },

            performer::Op::Event(performer::Event {
                op: performer::EventOp::DeleteBlock(
                    performer::TaskDoneOp { context: reply_tx, op: performer::DeleteBlockOp::NotFound, },
                ),
                performer,
            }) => {

                todo!();
            },

            performer::Op::Event(performer::Event {
                op: performer::EventOp::DeleteBlock(
                    performer::TaskDoneOp { context: reply_tx, op: performer::DeleteBlockOp::Done { .. }, },
                ),
                performer,
            }) => {

                todo!();
            },

            performer::Op::Event(performer::Event {
                op: performer::EventOp::IterBlocksItem(
                    performer::IterBlocksItemOp {
                        block_id,
                        block_bytes,
                        iter_blocks_state: performer::IterBlocksState {
                            iter_blocks_stream_context: blocks_tx,
                            iter_blocks_cursor,
                        },
                    },
                ),
                performer,
            }) => {

                todo!();
            },

            performer::Op::Event(performer::Event {
                op: performer::EventOp::IterBlocksFinish(
                    performer::IterBlocksFinishOp {
                        iter_blocks_stream_context: blocks_tx,
                    },
                ),
                performer,
            }) => {

                todo!();
            },

            performer::Op::Event(performer::Event {
                op: performer::EventOp::PrepareInterpretTask(
                    performer::PrepareInterpretTaskOp {
                        block_id,
                        task: performer::PrepareInterpretTaskKind::WriteBlock(performer::PrepareInterpretTaskWriteBlock {
                            block_bytes,
                            context,
                        }),
                    },
                ),
                performer,
            }) => {

                todo!();
            },

            performer::Op::Event(performer::Event {
                op: performer::EventOp::PrepareInterpretTask(
                    performer::PrepareInterpretTaskOp {
                        block_id,
                        task: performer::PrepareInterpretTaskKind::DeleteBlock(performer::PrepareInterpretTaskDeleteBlock {
                            context,
                        }),
                    },
                ),
                performer,
            }) => {

                todo!();
            },

            performer::Op::Event(performer::Event {
                op: performer::EventOp::ProcessReadBlockTaskDone(
                    performer::ProcessReadBlockTaskDoneOp {
                        storage_layout,
                        block_header,
                        block_bytes,
                        pending_contexts,
                    },
                ),
                performer,
            }) => {

                todo!();
            },

        };
    }
}
