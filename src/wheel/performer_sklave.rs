use alloc_pool::{
    bytes::{
        BytesPool,
    },
};

use futures::{
    channel::{
        mpsc,
    },
};

use crate::{
    job,
    proto,
    context,
    wheel::{
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

pub enum Order<C> where C: context::Context {
    Request(proto::Request<C>),
    TaskDoneStats(OrderTaskDoneStats<C>),
    DeviceSyncDone(OrderDeviceSyncDone<C>),
    IterBlocks(OrderIterBlocks),
    PreparedWriteBlockDone(OrderPreparedWriteBlockDone),
    ProcessReadBlockDone(OrderProcessReadBlockDone),
    PreparedDeleteBlockDone(OrderPreparedDeleteBlockDone),
    InterpreterError(interpret::RunError),
}

pub struct OrderTaskDoneStats<C> where C: context::Context {
    pub task_done: task::Done<C>,
    pub stats: InterpretStats,
}

pub struct OrderDeviceSyncDone<C> where C: context::Context {
    pub flush_context: C::Flush,
}

pub struct OrderIterBlocks {
    pub iter_blocks_state: performer::IterBlocksState<<Context as context::Context>::IterBlocksStream>,
}

pub struct OrderPreparedWriteBlockDone {
    pub output: interpret::BlockPrepareWriteJobOutput,
}

pub struct OrderProcessReadBlockDone {
    pub output: interpret::BlockProcessReadJobOutput,
}

pub struct OrderPreparedDeleteBlockDone {
    pub output: interpret::BlockPrepareDeleteJobOutput,
}

pub struct Env {
    pub interpreter_pid: interpret::Pid<Context>,
    pub blocks_pool: BytesPool,
    pub error_tx: mpsc::UnboundedSender<Error>,
}

pub struct Welt {
    pub env: Env,
    pub kont: Kont,
}

pub enum Kont {
    Taken,
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

pub type Meister = arbeitssklave::Meister<Welt, Order<Context>>;
pub type SklaveJob = arbeitssklave::SklaveJob<Welt, Order<Context>>;
pub type WriteBlockContext = task::WriteBlockContext<<Context as context::Context>::WriteBlock>;
pub type DeleteBlockContext = task::DeleteBlockContext<<Context as context::Context>::DeleteBlock>;

#[derive(Debug)]
pub enum Error {
    Edeltraud(edeltraud::SpawnError),
    Arbeitssklave(arbeitssklave::Error),
    WheelProcessLost,
    InterpreterProcessIsLost,
    InterpretBlockPrepareWrite(interpret::BlockPrepareWriteJobError),
    InterpretBlockPrepareDelete(interpret::BlockPrepareDeleteJobError),
    InterpretBlockProcessRead(interpret::BlockProcessReadJobError),
    InterpretRun(interpret::RunError),
}

pub fn run_job<P>(sklave_job: SklaveJob, thread_pool: &P) where P: edeltraud::ThreadPool<job::Job> {
    let error_tx = sklave_job.sklavenwelt.env.error_tx.clone();
    if let Err(error) = job(sklave_job, thread_pool) {
        log::debug!("terminated with an error: {error:?}");
        error_tx.unbounded_send(error).ok();
    }
}

fn job<P>(SklaveJob { mut sklave, mut sklavenwelt, }: SklaveJob, thread_pool: &P) -> Result<(), Error> where P: edeltraud::ThreadPool<job::Job> {
    loop {
        let mut incoming_order = None;
        let mut performer_op = loop {
            match (incoming_order, std::mem::replace(&mut sklavenwelt.kont, Kont::Taken)) {
                (_, Kont::Taken) =>
                    unreachable!(),
                (None, Kont::Start { performer, }) =>
                    break performer.next(),
                (Some(..), Kont::Start { .. }) =>
                    unreachable!(),
                (None, kont @ Kont::PollRequestAndInterpreter { .. }) => {
                    sklavenwelt.kont = kont;
                    match sklave.zu_ihren_diensten(sklavenwelt) {
                        Ok(arbeitssklave::Gehorsam::Machen { befehl, sklavenwelt: next_sklavenwelt, }) => {
                            sklavenwelt = next_sklavenwelt;
                            incoming_order = Some(befehl);
                        },
                        Ok(arbeitssklave::Gehorsam::Rasten) =>
                            return Ok(()),
                        Err(error) =>
                            return Err(Error::Arbeitssklave(error)),
                    }
                },
                (None, kont @ Kont::PollRequest { .. }) => {
                    sklavenwelt.kont = kont;
                    match sklave.zu_ihren_diensten(sklavenwelt) {
                        Ok(arbeitssklave::Gehorsam::Machen { befehl, sklavenwelt: next_sklavenwelt, }) => {
                            sklavenwelt = next_sklavenwelt;
                            incoming_order = Some(befehl);
                        },
                        Ok(arbeitssklave::Gehorsam::Rasten) =>
                            return Ok(()),
                        Err(error) =>
                            return Err(Error::Arbeitssklave(error)),
                    }
                },

                (Some(Order::Request(request)), Kont::PollRequestAndInterpreter { poll, }) =>
                    break poll.next.incoming_request(request),
                (Some(Order::TaskDoneStats(OrderTaskDoneStats { task_done, stats, })), Kont::PollRequestAndInterpreter { poll, }) =>
                    break poll.next.incoming_task_done_stats(task_done, stats),
                (Some(Order::IterBlocks(OrderIterBlocks { iter_blocks_state, })), Kont::PollRequestAndInterpreter { poll, }) =>
                    break poll.next.incoming_iter_blocks(iter_blocks_state),
                (
                    Some(Order::PreparedWriteBlockDone(OrderPreparedWriteBlockDone {
                        output: Ok(interpret::BlockPrepareWriteJobDone { block_id, write_block_bytes, context, }),
                    })),
                    Kont::PollRequestAndInterpreter { poll, },
                ) =>
                    break poll.next.prepared_write_block_done(block_id, write_block_bytes, context),
                (
                    Some(Order::ProcessReadBlockDone(OrderProcessReadBlockDone {
                        output: Ok(interpret::BlockProcessReadJobDone { block_id, block_bytes, pending_contexts, }),
                    })),
                    Kont::PollRequestAndInterpreter { poll, },
                ) =>
                    break poll.next.process_read_block_done(block_id, block_bytes, pending_contexts),
                (
                    Some(Order::PreparedDeleteBlockDone(OrderPreparedDeleteBlockDone {
                        output: Ok(interpret::BlockPrepareDeleteJobDone { block_id, delete_block_bytes, context, }),
                    })),
                    Kont::PollRequestAndInterpreter { poll, },
                ) =>
                    break poll.next.prepared_delete_block_done(block_id, delete_block_bytes, context),

                (Some(Order::Request(request)), Kont::PollRequest { poll, }) =>
                    break poll.next.incoming_request(request),
                (Some(Order::TaskDoneStats(..)), Kont::PollRequest { .. }) =>
                    unreachable!(),
                (Some(Order::IterBlocks(OrderIterBlocks { iter_blocks_state, })), Kont::PollRequest { poll, }) =>
                    break poll.next.incoming_iter_blocks(iter_blocks_state),
                (
                    Some(Order::PreparedWriteBlockDone(OrderPreparedWriteBlockDone {
                        output: Ok(interpret::BlockPrepareWriteJobDone { block_id, write_block_bytes, context, }),
                    })),
                    Kont::PollRequest { poll, },
                ) =>
                    break poll.next.prepared_write_block_done(block_id, write_block_bytes, context),
                (
                    Some(Order::ProcessReadBlockDone(OrderProcessReadBlockDone {
                        output: Ok(interpret::BlockProcessReadJobDone { block_id, block_bytes, pending_contexts, }),
                    })),
                    Kont::PollRequest { poll, },
                ) =>
                    break poll.next.process_read_block_done(block_id, block_bytes, pending_contexts),
                (
                    Some(Order::PreparedDeleteBlockDone(OrderPreparedDeleteBlockDone {
                        output: Ok(interpret::BlockPrepareDeleteJobDone { block_id, delete_block_bytes, context, }),
                    })),
                    Kont::PollRequest { poll, },
                ) =>
                    break poll.next.prepared_delete_block_done(block_id, delete_block_bytes, context),

                (Some(Order::DeviceSyncDone(OrderDeviceSyncDone { flush_context: done_tx, })), kont) => {
                    if let Err(_send_error) = done_tx.send(Flushed) {
                        return Err(Error::WheelProcessLost);
                    }
                    sklavenwelt.kont = kont;
                    incoming_order = None;
                },
                (Some(Order::PreparedWriteBlockDone(OrderPreparedWriteBlockDone { output: Err(error), })), _kont) =>
                    return Err(Error::InterpretBlockPrepareWrite(error)),
                (Some(Order::PreparedDeleteBlockDone(OrderPreparedDeleteBlockDone { output: Err(error), })), _kont) =>
                    return Err(Error::InterpretBlockPrepareDelete(error)),
                (Some(Order::ProcessReadBlockDone(OrderProcessReadBlockDone { output: Err(error), })), _kont) =>
                    return Err(Error::InterpretBlockProcessRead(error)),
                (Some(Order::InterpreterError(error)), _kont) =>
                    return Err(Error::InterpretRun(error)),
            }
        };

        loop {
            performer_op = match performer_op {

                performer::Op::Idle(performer) =>
                    performer.next(),

                performer::Op::Query(performer::QueryOp::PollRequestAndInterpreter(poll)) => {
                    sklavenwelt.kont = Kont::PollRequestAndInterpreter { poll, };
                    break;
                },

                performer::Op::Query(performer::QueryOp::PollRequest(poll)) => {
                    sklavenwelt.kont = Kont::PollRequest { poll, };
                    break;
                },

                performer::Op::Query(performer::QueryOp::InterpretTask(performer::InterpretTask { offset, task, next, })) => {
                    if let Err(ero::NoProcError) = sklavenwelt.env.interpreter_pid.push_request(offset, task) {
                        return Err(Error::InterpreterProcessIsLost);
                    }
                    let performer = next.task_accepted();
                    performer.next()
                },

                performer::Op::Query(performer::QueryOp::MakeIterBlocksStream(performer::MakeIterBlocksStream {
                    blocks_total_count,
                    blocks_total_size,
                    iter_blocks_context: blockwheel_context::IterBlocksContext {
                        reply_tx,
                        maybe_iter_task_tx: Some(iter_task_tx),
                    },
                    next,
                })) => {
                    let (blocks_tx, blocks_rx) = futures::channel::mpsc::channel(0);
                    let iter_blocks = IterBlocks {
                        blocks_total_count,
                        blocks_total_size,
                        blocks_rx,
                    };
                    if let Err(_send_error) = reply_tx.send(iter_blocks) {
                        log::warn!("Pid is gone during IterBlocks query result send");
                    }

                    let iter_blocks_stream = blockwheel_context::IterBlocksStreamContext {
                        blocks_tx,
                        iter_task_tx,
                    };
                    next.stream_ready(iter_blocks_stream)
                },

                performer::Op::Query(performer::QueryOp::MakeIterBlocksStream(performer::MakeIterBlocksStream {
                    iter_blocks_context: blockwheel_context::IterBlocksContext {
                        maybe_iter_task_tx: None,
                        ..
                    },
                    ..
                })) =>
                    unreachable!(),

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
                    if let Err(ero::NoProcError) = sklavenwelt.env.interpreter_pid.device_sync(reply_tx) {
                        return Err(Error::InterpreterProcessIsLost);
                    }
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
                                iter_blocks_stream_context: blockwheel_context::IterBlocksStreamContext {
                                    blocks_tx,
                                    iter_task_tx,
                                },
                                iter_blocks_cursor,
                            },
                        },
                    ),
                    performer,
                }) => {
                    let iter_task = IterTask::Item {
                        blocks_tx,
                        item: IterBlocksItem::Block {
                            block_id,
                            block_bytes,
                        },
                        iter_blocks_cursor,
                    };
                    if let Err(_send_error) = iter_task_tx.send(iter_task) {
                        return Err(Error::WheelProcessLost);
                    }
                    performer.next()
                },

                performer::Op::Event(performer::Event {
                    op: performer::EventOp::IterBlocksFinish(
                        performer::IterBlocksFinishOp {
                            iter_blocks_stream_context: blockwheel_context::IterBlocksStreamContext {
                                blocks_tx,
                                iter_task_tx,
                            },
                        },
                    ),
                    performer,
                }) => {
                    if let Err(_send_error) = iter_task_tx.send(IterTask::Finish { blocks_tx, }) {
                        return Err(Error::WheelProcessLost);
                    }
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
                    let job_args = interpret::BlockPrepareWriteJobArgs {
                        block_id,
                        block_bytes,
                        blocks_pool: sklavenwelt.env.blocks_pool.clone(),
                        context,
                        meister: sklave.meister()
                            .map_err(Error::Arbeitssklave)?,
                    };
                    edeltraud::job(thread_pool, job_args)
                        .map_err(Error::Edeltraud)?;
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
                    let job_args = interpret::BlockPrepareDeleteJobArgs {
                        block_id,
                        blocks_pool: sklavenwelt.env.blocks_pool.clone(),
                        context,
                        meister: sklave.meister()
                            .map_err(Error::Arbeitssklave)?,
                    };
                    edeltraud::job(thread_pool, job_args)
                        .map_err(Error::Edeltraud)?;
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
                    let job_args = interpret::BlockProcessReadJobArgs {
                        storage_layout,
                        block_header,
                        block_bytes,
                        pending_contexts,
                        meister: sklave.meister()
                            .map_err(Error::Arbeitssklave)?,
                    };
                    edeltraud::job(thread_pool, job_args)
                        .map_err(Error::Edeltraud)?;
                    performer.next()
                },

            };
        }
    }
}

use std::fmt;

impl<C> fmt::Debug for Order<C> where C: context::Context {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Order::Request(..) =>
                fmt.debug_tuple("Order::Request").finish(),
            Order::TaskDoneStats(..) =>
                fmt.debug_tuple("Order::TaskDoneStats").finish(),
            Order::DeviceSyncDone(..) =>
                fmt.debug_tuple("Order::DeviceSyncDone").finish(),
            Order::IterBlocks(..) =>
                fmt.debug_tuple("Order::IterBlocks").finish(),
            Order::PreparedWriteBlockDone(..) =>
                fmt.debug_tuple("Order::PreparedWriteBlockDone").finish(),
            Order::ProcessReadBlockDone(..) =>
                fmt.debug_tuple("Order::ProcessReadBlockDone").finish(),
            Order::PreparedDeleteBlockDone(..) =>
                fmt.debug_tuple("Order::PreparedDeleteBlockDone").finish(),
            Order::InterpreterError(error) =>
                fmt.debug_tuple("Order::InterpreterError").field(error).finish(),
        }
    }
}
