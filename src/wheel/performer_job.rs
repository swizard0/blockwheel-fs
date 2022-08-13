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

use alloc_pool::{
    bytes::{
        Bytes,
        BytesMut,
    },
};

use crate::{
    proto,
    context,
    storage,
    wheel::{
        block,
        interpret,
        core::{
            task,
            performer,
        },
        IterTask,
    },
    blockwheel_context::{
        self,
        Context,
    },
    Flushed,
    Deleted,
    IterBlocks,
    IterBlocksItem,
    InterpretStats,
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
    Start {
        performer: performer::Performer<Context>,
    },
    PollRequestAndInterpreter {
        poll: performer::PollRequestAndInterpreter<Context>,
    },
    PollRequest {
        poll: performer::PollRequest<Context>,
    },






    PollRequestAndInterpreter {
        performer: performer::Performer<Context>,
    },
    PollRequestAndInterpreterIncomingRequest {
        next: performer::PollRequestAndInterpreterNext<Context>,
        request: proto::Request<Context>,
        fused_interpret_result_rx: <Context as context::Context>::Interpreter,
    },
    PollRequestAndInterpreterTaskDoneStats {
        next: performer::PollRequestAndInterpreterNext<Context>,
        task_done: task::Done<Context>,
        stats: InterpretStats,
    },
    PollRequestAndInterpreterIncomingIterBlocks {
        next: performer::PollRequestAndInterpreterNext<Context>,
        iter_block_state: performer::IterBlocksState<<Context as context::Context>::IterBlocksStream>,
        fused_interpret_result_rx: <Context as context::Context>::Interpreter,
    },
    PollRequestAndInterpreterPreparedWriteBlockDone {
        next: performer::PollRequestAndInterpreterNext<Context>,
        block_id: block::Id,
        write_block_bytes: task::WriteBlockBytes,
        context: task::WriteBlockContext<<Context as context::Context>::WriteBlock>,
        fused_interpret_result_rx: <Context as context::Context>::Interpreter,
    },
    PollRequestAndInterpreterProcessReadBlockDone {
        next: performer::PollRequestAndInterpreterNext<Context>,
        block_id: block::Id,
        block_bytes: Bytes,
        pending_contexts: task::queue::PendingReadContextBag,
        fused_interpret_result_rx: <Context as context::Context>::Interpreter,
    },
    PollRequestAndInterpreterPreparedDeleteBlockDone {
        next: performer::PollRequestAndInterpreterNext<Context>,
        block_id: block::Id,
        delete_block_bytes: BytesMut,
        context: task::DeleteBlockContext<<Context as context::Context>::DeleteBlock>,
        fused_interpret_result_rx: <Context as context::Context>::Interpreter,
    },
    PollRequestIncomingRequest {
        next: performer::PollRequestNext<Context>,
        request: proto::Request<Context>,
    },
    PollRequestIncomingIterBlocks {
        next: performer::PollRequestNext<Context>,
        iter_block_state: performer::IterBlocksState<<Context as context::Context>::IterBlocksStream>,
    },
    PollRequestPreparedWriteBlockDone {
        next: performer::PollRequestNext<Context>,
        block_id: block::Id,
        write_block_bytes: task::WriteBlockBytes,
        context: task::WriteBlockContext<<Context as context::Context>::WriteBlock>,
    },
    PollRequestProcessReadBlockDone {
        next: performer::PollRequestNext<Context>,
        block_id: block::Id,
        block_bytes: Bytes,
        pending_contexts: task::queue::PendingReadContextBag,
    },
    PollRequestPreparedDeleteBlockDone {
        next: performer::PollRequestNext<Context>,
        block_id: block::Id,
        delete_block_bytes: BytesMut,
        context: task::DeleteBlockContext<<Context as context::Context>::DeleteBlock>,
    },
}

pub struct RunJobDone {
    pub env: Env,
    pub done: Done,
}

pub enum Done {
    PollRequestAndInterpreter {
        poll: performer::PollRequestAndInterpreter<Context>,
    },
    PollRequest {
        poll: performer::PollRequest<Context>,
    },
    TaskDoneFlush {
        performer: performer::Performer<Context>,
        reply_tx: oneshot::Sender<Flushed>,
    },
    PrepareWriteBlock {
        performer: performer::Performer<Context>,
        block_id: block::Id,
        block_bytes: Bytes,
        context: task::WriteBlockContext<<Context as context::Context>::WriteBlock>,
    },
    PrepareDeleteBlock {
        performer: performer::Performer<Context>,
        block_id: block::Id,
        context: task::DeleteBlockContext<<Context as context::Context>::DeleteBlock>,
    },
    ProcessReadBlock {
        performer: performer::Performer<Context>,
        storage_layout: storage::Layout,
        block_header: storage::BlockHeader,
        block_bytes: Bytes,
        pending_contexts: task::queue::PendingReadContextBag,
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
        Kont::PollRequestAndInterpreterIncomingRequest { next, request, fused_interpret_result_rx, } =>
            next.incoming_request(request, fused_interpret_result_rx),
        Kont::PollRequestAndInterpreterTaskDoneStats { next, task_done, stats, } =>
            next.incoming_task_done_stats(task_done, stats),
        Kont::PollRequestAndInterpreterIncomingIterBlocks { next, iter_block_state, fused_interpret_result_rx, } =>
            next.incoming_iter_blocks(iter_block_state, fused_interpret_result_rx),
        Kont::PollRequestAndInterpreterPreparedWriteBlockDone { next, block_id, write_block_bytes, context, fused_interpret_result_rx, } =>
            next.prepared_write_block_done(
                block_id,
                write_block_bytes,
                context,
                fused_interpret_result_rx,
            ),
        Kont::PollRequestAndInterpreterProcessReadBlockDone { next, block_id, block_bytes, pending_contexts, fused_interpret_result_rx, } =>
            next.process_read_block_done(
                block_id,
                block_bytes,
                pending_contexts,
                fused_interpret_result_rx,
            ),
        Kont::PollRequestAndInterpreterPreparedDeleteBlockDone { next, block_id, delete_block_bytes, context, fused_interpret_result_rx, } =>
            next.prepared_delete_block_done(
                block_id,
                delete_block_bytes,
                context,
                fused_interpret_result_rx,
            ),
        Kont::PollRequestIncomingRequest { next, request, } =>
            next.incoming_request(request),
        Kont::PollRequestIncomingIterBlocks { next, iter_block_state, } =>
            next.incoming_iter_blocks(iter_block_state),
        Kont::PollRequestPreparedWriteBlockDone { next, block_id, write_block_bytes, context, } =>
            next.prepared_write_block_done(
                block_id,
                write_block_bytes,
                context,
            ),
        Kont::PollRequestProcessReadBlockDone { next, block_id, block_bytes, pending_contexts, } =>
            next.process_read_block_done(
                block_id,
                block_bytes,
                pending_contexts,
            ),
        Kont::PollRequestPreparedDeleteBlockDone { next, block_id, delete_block_bytes, context, } =>
            next.prepared_delete_block_done(
                block_id,
                delete_block_bytes,
                context,
            ),
    };

    loop {
        op = match op {

            performer::Op::Idle(performer) =>
                performer.next(),

            performer::Op::Query(performer::QueryOp::PollRequestAndInterpreter(poll)) =>
                return Ok(RunJobDone { env, done: Done::PollRequestAndInterpreter { poll, } }),

            performer::Op::Query(performer::QueryOp::PollRequest(poll)) =>
                return Ok(RunJobDone { env, done: Done::PollRequest { poll, } }),

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
            }) =>
                return Ok(RunJobDone { env, done: Done::PrepareWriteBlock {
                    performer,
                    block_id,
                    block_bytes,
                    context,
                }}),

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
            }) =>
                return Ok(RunJobDone { env, done: Done::PrepareDeleteBlock {
                    performer,
                    block_id,
                    context,
                }}),

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
            }) =>
                return Ok(RunJobDone { env, done: Done::ProcessReadBlock {
                    performer,
                    storage_layout,
                    block_header,
                    block_bytes,
                    pending_contexts,
                }}),

        };
    }
}
