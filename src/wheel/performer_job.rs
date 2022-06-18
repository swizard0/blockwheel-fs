use futures::{
    channel::{
        mpsc,
        oneshot,
    },
    stream::{
        FuturesUnordered,
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
        IterTask,
    },
    blockwheel_context::{
        self,
        Context,
    },
    Info,
    Flushed,
    Deleted,
    IterBlocks,
    IterBlocksItem,
};

pub struct Env {
    pub interpreter_pid: interpret::Pid<Context>,
    pub(super) iter_tasks: FuturesUnordered<IterTask>,
}

pub struct RunJobArgs {
    pub env: Env,
    pub kont: Kont,
}

pub enum Kont {
    Next {
        performer: performer::Performer<Context>,
    },
}

pub struct RunJobDone {
    pub env: Env,
    pub done: Done,
}

pub enum Done {
    TaskDoneFlush {
        performer: performer::Performer<Context>,
        reply_tx: oneshot::Sender<Flushed>,
    },
}

#[derive(Debug)]
pub enum RunJobError {
    InterpreterCrash,
}

pub type RunJobOutput = Result<RunJobDone, RunJobError>;

pub fn run_job(
    RunJobArgs {
        mut env,
        kont,
    }: RunJobArgs,
)
    -> RunJobOutput
{
    let mut op = match kont {
        Kont::Next { performer, } =>
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
                let reply_rx = env.interpreter_pid.push_request(offset, task)
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
                if let Err(_send_error) = reply_tx.send(iter_blocks) {
                    log::warn!("Pid is gone during IterBlocks query result send");
                }
                next.stream_ready(iter_blocks_tx)
            },

            performer::Op::Event(performer::Event {
                op: performer::EventOp::Info(
                    performer::TaskDoneOp { context: reply_tx, op: performer::InfoOp::Success { info, }, },
                ),
                performer,
            }) => {
                if let Err(_send_error) = reply_tx.send(info) {
                    log::warn!("Pid is gone during Info query result send");
                }
                performer.next()

            },

            performer::Op::Event(performer::Event {
                op: performer::EventOp::Flush(
                    performer::TaskDoneOp { context: reply_tx, op: performer::FlushOp::Flushed, },
                ),
                performer,
            }) =>
                return Ok(RunJobDone { env, done: Done::TaskDoneFlush {
                    performer,
                    reply_tx,
                }}),

            performer::Op::Event(performer::Event {
                op: performer::EventOp::WriteBlock(
                    performer::TaskDoneOp { context: reply_tx, op: performer::WriteBlockOp::NoSpaceLeft, },
                ),
                performer,
            }) => {
                if let Err(_send_error) = reply_tx.send(Err(blockwheel_context::RequestWriteBlockError::NoSpaceLeft)) {
                    log::warn!("reply channel has been closed during WriteBlock result send");
                }
                performer.next()
            },

            performer::Op::Event(performer::Event {
                op: performer::EventOp::WriteBlock(
                    performer::TaskDoneOp { context: reply_tx, op: performer::WriteBlockOp::Done { block_id, }, },
                ),
                performer,
            }) => {
                if let Err(_send_error) = reply_tx.send(Ok(block_id)) {
                    log::warn!("client channel was closed before a block is actually written");
                }
                performer.next()
            },

            performer::Op::Event(performer::Event {
                op: performer::EventOp::ReadBlock(
                    performer::TaskDoneOp { context: reply_tx, op: performer::ReadBlockOp::NotFound, },
                ),
                performer,
            }) => {
                if let Err(_send_error) = reply_tx.send(Err(blockwheel_context::RequestReadBlockError::NotFound)) {
                    log::warn!("reply channel has been closed during ReadBlock result send");
                }
                performer.next()
            },

            performer::Op::Event(performer::Event {
                op: performer::EventOp::ReadBlock(
                    performer::TaskDoneOp { context: reply_tx, op: performer::ReadBlockOp::Done { block_bytes, }, },
                ),
                performer,
            }) => {
                if let Err(_send_error) = reply_tx.send(Ok(block_bytes)) {
                    log::warn!("client channel was closed before a block is actually read");
                }
                performer.next()
            },

            performer::Op::Event(performer::Event {
                op: performer::EventOp::DeleteBlock(
                    performer::TaskDoneOp { context: reply_tx, op: performer::DeleteBlockOp::NotFound, },
                ),
                performer,
            }) => {
                if let Err(_send_error) = reply_tx.send(Err(blockwheel_context::RequestDeleteBlockError::NotFound)) {
                    log::warn!("reply channel has been closed during DeleteBlock result send");
                }
                performer.next()
            },

            performer::Op::Event(performer::Event {
                op: performer::EventOp::DeleteBlock(
                    performer::TaskDoneOp { context: reply_tx, op: performer::DeleteBlockOp::Done { .. }, },
                ),
                performer,
            }) => {
                if let Err(_send_error) = reply_tx.send(Ok(Deleted)) {
                    log::warn!("client channel was closed before a block is actually deleted");
                }
                performer.next()
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
                env.iter_tasks.push(IterTask::Item {
                    blocks_tx,
                    item: IterBlocksItem::Block {
                        block_id,
                        block_bytes,
                    },
                    iter_blocks_cursor,
                });
                performer.next()
            },

            performer::Op::Event(performer::Event {
                op: performer::EventOp::IterBlocksFinish(
                    performer::IterBlocksFinishOp {
                        iter_blocks_stream_context: blocks_tx,
                    },
                ),
                performer,
            }) => {
                env.iter_tasks.push(IterTask::Finish { blocks_tx, });
                performer.next()
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
