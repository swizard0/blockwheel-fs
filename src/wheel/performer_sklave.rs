use alloc_pool::{
    bytes::{
        Bytes,
        BytesMut,
    },
};

use crate::{
    job,
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

pub enum Order<C> where C: context::Context {
    Request(proto::Request<C>),
    TaskDoneStats(OrderTaskDoneStats<C>),
    DeviceSyncDone(OrderDeviceSyncDone<C>),
    IterBlocks(OrderIterBlocks),
    PreparedWriteBlockDone(OrderPreparedWriteBlockDone),
    ProcessReadBlockDone(OrderProcessReadBlockDone),
    PreparedDeleteBlockDone(OrderPreparedDeleteBlockDone),
}

pub struct OrderTaskDoneStats<C> where C: context::Context {
    pub task_done: task::Done<C>,
    pub stats: InterpretStats,
}

pub struct OrderDeviceSyncDone<C> where C: context::Context {
    pub flush_context: C::Flush,
}

pub struct OrderIterBlocks {
    pub iter_block_state: performer::IterBlocksState<<Context as context::Context>::IterBlocksStream>,
}

pub struct OrderPreparedWriteBlockDone {
    pub block_id: block::Id,
    pub write_block_bytes: task::WriteBlockBytes,
    pub context: task::WriteBlockContext<<Context as context::Context>::WriteBlock>,
}

pub struct OrderProcessReadBlockDone {
    pub block_id: block::Id,
    pub block_bytes: Bytes,
    pub pending_contexts: task::queue::PendingReadContextBag,
}

pub struct OrderPreparedDeleteBlockDone {
    pub block_id: block::Id,
    pub delete_block_bytes: BytesMut,
    pub context: task::DeleteBlockContext<<Context as context::Context>::DeleteBlock>,
}

pub struct Env {
    pub interpreter_pid: interpret::Pid<Context>,
}

pub struct Welt {
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

pub type Meister = arbeitssklave::Meister<Welt, Order<Context>>;
pub type Sklave = arbeitssklave::Sklave<Welt, Order<Context>>;
pub type SklaveJob = arbeitssklave::SklaveJob<Welt, Order<Context>>;

pub fn run_job<P>(SklaveJob { mut sklave, mut sklavenwelt, }: SklaveJob, thread_pool: &P) where P: edeltraud::ThreadPool<job::Job> {
    loop {
        let mut incoming_order = None;
        let mut performer_op = loop {
            match (incoming_order, sklavenwelt.kont) {
                (None, Kont::Start { performer, }) =>
                    break performer.next(),
                (Some(..), Kont::Start { .. }) =>
                    unreachable!(),
                (None, kont @ Kont::PollRequestAndInterpreter { .. }) =>
                    match sklave.obey(Welt { env: sklavenwelt.env, kont, }) {
                        Ok(arbeitssklave::Obey::Order { order, sklavenwelt: next_sklavenwelt, }) => {
                            incoming_order = Some(order);
                            sklavenwelt = next_sklavenwelt;
                        },
                        Ok(arbeitssklave::Obey::Rest) =>
                            return,
                        Err(error) => {
                            log::error!("recv error: {error:?}, terminating");
                            return;
                        },
                    },
                (None, kont @ Kont::PollRequest { .. }) =>
                    match sklave.obey(Welt { env: sklavenwelt.env, kont, }) {
                        Ok(arbeitssklave::Obey::Order { order, sklavenwelt: next_sklavenwelt, }) => {
                            incoming_order = Some(order);
                            sklavenwelt = next_sklavenwelt;
                        },
                        Ok(arbeitssklave::Obey::Rest) =>
                            return,
                        Err(error) => {
                            log::error!("recv error: {error:?}, terminating");
                            return;
                        },
                    },

                (Some(Order::Request(request)), Kont::PollRequestAndInterpreter { poll, }) =>
                    break poll.next.incoming_request(request),
                (Some(Order::TaskDoneStats(OrderTaskDoneStats { task_done, stats, })), Kont::PollRequestAndInterpreter { poll, }) =>
                    break poll.next.incoming_task_done_stats(task_done, stats),
                (Some(Order::IterBlocks(OrderIterBlocks { iter_block_state, })), Kont::PollRequestAndInterpreter { poll, }) =>
                    break poll.next.incoming_iter_blocks(iter_block_state),
                (
                    Some(Order::PreparedWriteBlockDone(OrderPreparedWriteBlockDone { block_id, write_block_bytes, context, })),
                    Kont::PollRequestAndInterpreter { poll, },
                ) =>
                    break poll.next.prepared_write_block_done(block_id, write_block_bytes, context),
                (
                    Some(Order::ProcessReadBlockDone(OrderProcessReadBlockDone { block_id, block_bytes, pending_contexts, })),
                    Kont::PollRequestAndInterpreter { poll, },
                ) =>
                    break poll.next.process_read_block_done(block_id, block_bytes, pending_contexts),
                (
                    Some(Order::PreparedDeleteBlockDone(OrderPreparedDeleteBlockDone { block_id, delete_block_bytes, context, })),
                    Kont::PollRequestAndInterpreter { poll, },
                ) =>
                    break poll.next.prepared_delete_block_done(block_id, delete_block_bytes, context),

                (Some(Order::Request(request)), Kont::PollRequest { poll, }) =>
                    break poll.next.incoming_request(request),
                (Some(Order::TaskDoneStats(..)), Kont::PollRequest { .. }) =>
                    unreachable!(),
                (Some(Order::IterBlocks(OrderIterBlocks { iter_block_state, })), Kont::PollRequest { poll, }) =>
                    break poll.next.incoming_iter_blocks(iter_block_state),
                (
                    Some(Order::PreparedWriteBlockDone(OrderPreparedWriteBlockDone { block_id, write_block_bytes, context, })),
                    Kont::PollRequest { poll, },
                ) =>
                    break poll.next.prepared_write_block_done(block_id, write_block_bytes, context),
                (
                    Some(Order::ProcessReadBlockDone(OrderProcessReadBlockDone { block_id, block_bytes, pending_contexts, })),
                    Kont::PollRequest { poll, },
                ) =>
                    break poll.next.process_read_block_done(block_id, block_bytes, pending_contexts),
                (
                    Some(Order::PreparedDeleteBlockDone(OrderPreparedDeleteBlockDone { block_id, delete_block_bytes, context, })),
                    Kont::PollRequest { poll, },
                ) =>
                    break poll.next.prepared_delete_block_done(block_id, delete_block_bytes, context),

                (Some(Order::DeviceSyncDone(OrderDeviceSyncDone { flush_context: done_tx, })), kont) => {
                    if let Err(_send_error) = done_tx.send(Flushed) {
                        log::error!("wheel flush done channel dropped, terminating");
                        return;
                    }
                    sklavenwelt.kont = kont;
                    incoming_order = None;
                },
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
                    if let Err(error) = sklavenwelt.env.interpreter_pid.push_request(offset, task) {
                        log::error!("interpreter request error: {error:?}");
                        return;
                    }
                    let performer = next.task_accepted();
                    performer.next()
                },

                performer::Op::Query(performer::QueryOp::MakeIterBlocksStream(performer::MakeIterBlocksStream {
                    blocks_total_count,
                    blocks_total_size,
                    iter_blocks_context: reply_tx,
                    next,
                })) => {

                    todo!()
                    // let (iter_blocks_tx, iter_blocks_rx) = mpsc::channel(0);
                    // let iter_blocks = IterBlocks {
                    //     blocks_total_count,
                    //     blocks_total_size,
                    //     blocks_rx: iter_blocks_rx,
                    // };
                    // if let Err(_send_error) = reply_tx.send(iter_blocks) {
                    //     log::warn!("Pid is gone during IterBlocks query result send");
                    // }
                    // next.stream_ready(iter_blocks_tx)
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
                    if let Err(error) = sklavenwelt.env.interpreter_pid.device_sync(reply_tx) {
                        log::error!("interpreter device sync error: {error:?}");
                        return;
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
                                iter_blocks_stream_context: blocks_tx,
                                iter_blocks_cursor,
                            },
                        },
                    ),
                    performer,
                }) => {

                    todo!()
                    // env.outgoing.iter_tasks.push(IterTask::Item {
                    //     blocks_tx,
                    //     item: IterBlocksItem::Block {
                    //         block_id,
                    //         block_bytes,
                    //     },
                    //     iter_blocks_cursor,
                    // });
                    // performer.next()
                },

                performer::Op::Event(performer::Event {
                    op: performer::EventOp::IterBlocksFinish(
                        performer::IterBlocksFinishOp {
                            iter_blocks_stream_context: blocks_tx,
                        },
                    ),
                    performer,
                }) => {

                    todo!()
                    // env.outgoing.iter_tasks.push(IterTask::Finish { blocks_tx, });
                    // performer.next()
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

                    todo!()
                    // env.outgoing.prepare_write_blocks.push(PrepareWriteBlock {
                    //     block_id,
                    //     block_bytes,
                    //     context,
                    // });
                    // performer.next()
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

                    todo!()
                    // env.outgoing.prepare_delete_blocks.push(PrepareDeleteBlock {
                    //     block_id,
                    //     context,
                    // });
                    // performer.next()
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

                    todo!()
                    // env.outgoing.process_read_blocks.push(ProcessReadBlock {
                    //     storage_layout,
                    //     block_header,
                    //     block_bytes,
                    //     pending_contexts,
                    // });
                    // performer.next()
                },

            };
        }
    }
}

// #[derive(Default)]
// pub struct Outgoing {
//     pub interpret_task: Option<QueryInterpretTask>,
//     pub iter_tasks: Vec<IterTask>,
//     pub prepare_write_blocks: Vec<PrepareWriteBlock>,
//     pub prepare_delete_blocks: Vec<PrepareDeleteBlock>,
//     pub process_read_blocks: Vec<ProcessReadBlock>,
//     pub task_done_flush: Option<TaskDoneFlush>,
// }

// pub struct QueryInterpretTask {
//     pub fused_interpret_result_rx: <Context as context::Context>::Interpreter,
// }

// pub struct PrepareWriteBlock {
//     pub block_id: block::Id,
//     pub block_bytes: Bytes,
//     pub context: task::WriteBlockContext<<Context as context::Context>::WriteBlock>,
// }

// pub struct PrepareDeleteBlock {
//     pub block_id: block::Id,
//     pub context: task::DeleteBlockContext<<Context as context::Context>::DeleteBlock>,
// }

// pub struct ProcessReadBlock {
//     pub storage_layout: storage::Layout,
//     pub block_header: storage::BlockHeader,
//     pub block_bytes: Bytes,
//     pub pending_contexts: task::queue::PendingReadContextBag,
// }

// pub struct TaskDoneFlush {
//     pub reply_tx: oneshot::Sender<Flushed>,
// }
