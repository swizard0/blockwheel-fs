use futures::{
    channel::{
        mpsc,
        oneshot,
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
    pub incoming: Incoming,
    pub outgoing: Outgoing,
}

#[derive(Default)]
pub struct Incoming {
    pub incoming_request: Vec<proto::Request<Context>>,
    pub incoming_task_done_stats: Vec<IncomingTaskDoneStats>,
    pub incoming_iter_blocks: Vec<IncomingIterBlocks>,
    pub prepared_write_block_done: Vec<PreparedWriteBlockDone>,
    pub prepared_delete_block_done: Vec<PreparedDeleteBlockDone>,
    pub process_read_block_done: Vec<ProcessReadBlockDone>,
}

pub struct IncomingTaskDoneStats {
    pub task_done: task::Done<Context>,
    pub stats: InterpretStats,
}

pub struct IncomingIterBlocks {
    pub iter_block_state: performer::IterBlocksState<<Context as context::Context>::IterBlocksStream>,
}

pub struct PreparedWriteBlockDone {
    pub block_id: block::Id,
    pub write_block_bytes: task::WriteBlockBytes,
    pub context: task::WriteBlockContext<<Context as context::Context>::WriteBlock>,
}

pub struct ProcessReadBlockDone {
    pub block_id: block::Id,
    pub block_bytes: Bytes,
    pub pending_contexts: task::queue::PendingReadContextBag,
}

pub struct PreparedDeleteBlockDone {
    pub block_id: block::Id,
    pub delete_block_bytes: BytesMut,
    pub context: task::DeleteBlockContext<<Context as context::Context>::DeleteBlock>,
}

impl Incoming {
    pub fn is_empty(&self) -> bool {
        self.incoming_request.is_empty() &&
            self.incoming_task_done_stats.is_empty() &&
            self.incoming_iter_blocks.is_empty() &&
            self.prepared_write_block_done.is_empty() &&
            self.prepared_delete_block_done.is_empty() &&
            self.process_read_block_done.is_empty()
    }

    pub fn transfill_from(&mut self, from: &mut Self) {
        assert!(self.is_empty());
        std::mem::swap(self, from);
    }
}

#[derive(Default)]
pub struct Outgoing {
    pub interpret_task: Option<QueryInterpretTask>,
    pub iter_tasks: Vec<IterTask>,
    pub prepare_write_blocks: Vec<PrepareWriteBlock>,
    pub prepare_delete_blocks: Vec<PrepareDeleteBlock>,
    pub process_read_blocks: Vec<ProcessReadBlock>,
    pub task_done_flush: Option<TaskDoneFlush>,
}

pub struct QueryInterpretTask {
    pub fused_interpret_result_rx: <Context as context::Context>::Interpreter,
}

pub struct PrepareWriteBlock {
    pub block_id: block::Id,
    pub block_bytes: Bytes,
    pub context: task::WriteBlockContext<<Context as context::Context>::WriteBlock>,
}

pub struct PrepareDeleteBlock {
    pub block_id: block::Id,
    pub context: task::DeleteBlockContext<<Context as context::Context>::DeleteBlock>,
}

pub struct ProcessReadBlock {
    pub storage_layout: storage::Layout,
    pub block_header: storage::BlockHeader,
    pub block_bytes: Bytes,
    pub pending_contexts: task::queue::PendingReadContextBag,
}

pub struct TaskDoneFlush {
    pub reply_tx: oneshot::Sender<Flushed>,
}

pub struct JobArgs {
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
}

pub enum Done {
    Poll { env: Env, kont: Kont, },
}

#[derive(Debug)]
pub enum Error {
    InterpreterCrash,
}

pub type Output = Result<Done, Error>;

pub fn run_job(JobArgs { mut env, mut kont, }: JobArgs) -> Output {
    loop {
        let mut performer_op = match kont {
            Kont::Start { performer, } =>
                performer.next(),
            Kont::PollRequestAndInterpreter { poll, } =>
                if let Some(request) = env.incoming.incoming_request.pop() {
                    poll.next.incoming_request(request)
                } else if let Some(IncomingTaskDoneStats { task_done, stats, }) = env.incoming.incoming_task_done_stats.pop() {
                    poll.next.incoming_task_done_stats(task_done, stats)
                } else if let Some(IncomingIterBlocks { iter_block_state, }) = env.incoming.incoming_iter_blocks.pop() {
                    poll.next.incoming_iter_blocks(iter_block_state)
                } else if let Some(PreparedWriteBlockDone { block_id, write_block_bytes, context, }) = env.incoming.prepared_write_block_done.pop() {
                    poll.next.prepared_write_block_done(block_id, write_block_bytes, context)
                } else if let Some(ProcessReadBlockDone { block_id, block_bytes, pending_contexts, }) = env.incoming.process_read_block_done.pop() {
                    poll.next.process_read_block_done(block_id, block_bytes, pending_contexts)
                } else if let Some(PreparedDeleteBlockDone { block_id, delete_block_bytes, context, }) = env.incoming.prepared_delete_block_done.pop() {
                    poll.next.prepared_delete_block_done(block_id, delete_block_bytes, context)
                } else {
                    return Ok(Done::Poll { env, kont: Kont::PollRequestAndInterpreter { poll, }, });
                },
            Kont::PollRequest { poll, } =>
                if let Some(request) = env.incoming.incoming_request.pop() {
                    poll.next.incoming_request(request)
                } else if let Some(IncomingIterBlocks { iter_block_state, }) = env.incoming.incoming_iter_blocks.pop() {
                    poll.next.incoming_iter_blocks(iter_block_state)
                } else if let Some(PreparedWriteBlockDone { block_id, write_block_bytes, context, }) = env.incoming.prepared_write_block_done.pop() {
                    poll.next.prepared_write_block_done(block_id, write_block_bytes, context)
                } else if let Some(ProcessReadBlockDone { block_id, block_bytes, pending_contexts, }) = env.incoming.process_read_block_done.pop() {
                    poll.next.process_read_block_done(block_id, block_bytes, pending_contexts)
                } else if let Some(PreparedDeleteBlockDone { block_id, delete_block_bytes, context, }) = env.incoming.prepared_delete_block_done.pop() {
                    poll.next.prepared_delete_block_done(block_id, delete_block_bytes, context)
                } else {
                    return Ok(Done::Poll { env, kont: Kont::PollRequest { poll, }, });
                },
        };

        loop {
            performer_op = match performer_op {

                performer::Op::Idle(performer) =>
                    performer.next(),

                performer::Op::Query(performer::QueryOp::PollRequestAndInterpreter(poll)) => {
                    kont = Kont::PollRequestAndInterpreter { poll, };
                    break;
                },

                performer::Op::Query(performer::QueryOp::PollRequest(poll)) => {
                    kont = Kont::PollRequest { poll, };
                    break;
                },

                performer::Op::Query(performer::QueryOp::InterpretTask(performer::InterpretTask { offset, task, next, })) => {
                    let reply_rx = env.interpreter_pid.push_request(offset, task)
                        .map_err(|ero::NoProcError| Error::InterpreterCrash)?;
                    let performer = next.task_accepted();
                    assert!(env.outgoing.interpret_task.is_none());
                    env.outgoing.interpret_task = Some(QueryInterpretTask {
                        fused_interpret_result_rx: reply_rx.fuse(),
                    });
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
                }) => {
                    assert!(env.outgoing.task_done_flush.is_none());
                    env.outgoing.task_done_flush = Some(TaskDoneFlush { reply_tx, });
                    performer.next()
                },

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
                    env.outgoing.iter_tasks.push(IterTask::Item {
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
                    env.outgoing.iter_tasks.push(IterTask::Finish { blocks_tx, });
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
                    env.outgoing.prepare_write_blocks.push(PrepareWriteBlock {
                        block_id,
                        block_bytes,
                        context,
                    });
                    performer.next()
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
                    env.outgoing.prepare_delete_blocks.push(PrepareDeleteBlock {
                        block_id,
                        context,
                    });
                    performer.next()
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
                    env.outgoing.process_read_blocks.push(ProcessReadBlock {
                        storage_layout,
                        block_header,
                        block_bytes,
                        pending_contexts,
                    });
                    performer.next()
                },

            };
        }
    }
}
