use futures::{
    channel::{
        mpsc,
        oneshot,
    },
    FutureExt,
};

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
    IterBlocks,
    IterBlocksItem,
};

pub struct Common {
    pub interpreter_pid: interpret::Pid<Context>,
}

pub struct RunJobArgs {
    pub common: Common,
    pub kont: Kont,
}

pub enum Kont {
    Init {
        performer: performer::Performer<Context>,
    },
    MakeIterBlocksStream {
        next: performer::MakeIterBlocksStreamNext<Context>,
        iter_blocks_tx: mpsc::Sender<IterBlocksItem>,
    },
}

pub enum RunJobDone {
    MakeIterBlocksStream {
        next: performer::MakeIterBlocksStreamNext<Context>,
        iter_blocks: IterBlocks,
        iter_blocks_tx: mpsc::Sender<IterBlocksItem>,
    },
}

#[derive(Debug)]
pub enum RunJobError {
    InterpreterCrash,
}

pub type RunJobOutput = Result<RunJobDone, RunJobError>;

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
        Kont::MakeIterBlocksStream { next, iter_blocks_tx, } =>
            next.stream_ready(iter_blocks_tx),
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
                let reply_rx = common.interpreter_pid.push_request(offset, task)
                    .map_err(|ero::NoProcError| RunJobError::InterpreterCrash)?;
                let performer = next.task_accepted(reply_rx.fuse());
                performer.next()
            },

            performer::Op::Query(performer::QueryOp::MakeIterBlocksStream(performer::MakeIterBlocksStream {
                blocks_total_count,
                blocks_total_size,
                iter_blocks_context: reply_tx,
                next,
            })) => {
                let (iter_blocks_tx, iter_blocks_rx) = mpsc::channel(0);
                let iter_blocks = IterBlocks {
                    blocks_total_count,
                    blocks_total_size,
                    blocks_rx: iter_blocks_rx,
                };
                return Ok(RunJobDone::MakeIterBlocksStream {
                    next,
                    iter_blocks,
                    iter_blocks_tx,
                });
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
