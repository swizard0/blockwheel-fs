use alloc_pool::{
    bytes::{
        BytesPool,
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
    },
    blockwheel_context::{
        Context,
    },
    Flushed,
    Deleted,
    AccessPolicy,
    InterpretStats,
    RequestWriteBlockError,
    RequestReadBlockError,
    RequestDeleteBlockError,
};

pub enum Order<A> where A: AccessPolicy {
    Bootstrap(OrderBootstrap<A>),
    Request(proto::Request<Context<A>>),
    TaskDoneStats(OrderTaskDoneStats<A>),
    DeviceSyncDone(OrderDeviceSyncDone<A>),
    PreparedWriteBlockDone(OrderPreparedWriteBlockDone<A>),
    ProcessReadBlockDone(OrderProcessReadBlockDone),
    PreparedDeleteBlockDone(OrderPreparedDeleteBlockDone<A>),
}

pub struct OrderBootstrap<A> where A: AccessPolicy {
    pub performer: performer::Performer<Context<A>>,
}

pub struct OrderTaskDoneStats<A> where A: AccessPolicy {
    pub task_done: task::Done<Context<A>>,
    pub stats: InterpretStats,
}

pub struct OrderDeviceSyncDone<A> where A: AccessPolicy {
    pub flush_context: <Context<A> as context::Context>::Flush,
}

pub struct OrderPreparedWriteBlockDone<A> where A: AccessPolicy {
    pub output: interpret::BlockPrepareWriteJobOutput<A>,
}

pub struct OrderProcessReadBlockDone {
    pub output: interpret::BlockProcessReadJobOutput,
}

pub struct OrderPreparedDeleteBlockDone<A> where A: AccessPolicy {
    pub output: interpret::BlockPrepareDeleteJobOutput<A>,
}

pub struct Env<A> where A: AccessPolicy {
    pub interpreter: interpret::Interpreter<A>,
    pub blocks_pool: BytesPool,
    pub incoming_orders: Vec<Order<A>>,
    pub delayed_orders: Vec<Order<A>>,
}

pub struct Welt<A> where A: AccessPolicy {
    pub env: Env<A>,
    pub kont: Kont<A>,
}

pub enum Kont<A> where A: AccessPolicy {
    Initialize,
    Start {
        performer: performer::Performer<Context<A>>,
    },
    PollRequestAndInterpreter {
        poll: performer::PollRequestAndInterpreter<Context<A>>,
    },
    PollRequest {
        poll: performer::PollRequest<Context<A>>,
    },
}

pub type Meister<A> = arbeitssklave::Meister<Welt<A>, Order<A>>;
pub type SklaveJob<A> = arbeitssklave::SklaveJob<Welt<A>, Order<A>>;
pub type WriteBlockContext<A> = task::WriteBlockContext<<Context<A> as context::Context>::WriteBlock>;
pub type DeleteBlockContext<A> = task::DeleteBlockContext<<Context<A> as context::Context>::DeleteBlock>;

#[derive(Debug)]
pub enum Error {
    Edeltraud(edeltraud::SpawnError),
    Arbeitssklave(arbeitssklave::Error),
    InterpretPushTask(interpret::Error),
    InterpretDeviceSync(interpret::Error),
    InterpretBlockPrepareWrite(interpret::BlockPrepareWriteJobError),
    InterpretBlockPrepareDelete(interpret::BlockPrepareDeleteJobError),
    InterpretBlockProcessRead(interpret::BlockProcessReadJobError),
}

pub fn run_job<A, P>(sklave_job: SklaveJob<A>, thread_pool: &P)
where A: AccessPolicy,
      P: edeltraud::ThreadPool<job::Job<A>>,
{
    if let Err(error) = job(sklave_job, thread_pool) {
        log::error!("terminated with an error: {error:?}");
    }
}

fn job<A, P>(mut sklave_job: SklaveJob<A>, thread_pool: &P) -> Result<(), Error>
where A: AccessPolicy,
      P: edeltraud::ThreadPool<job::Job<A>>,
{
    loop {
        let mut performer_op = 'op: loop {
            let kont = match std::mem::replace(&mut sklave_job.sklavenwelt_mut().kont, Kont::Initialize) {
                Kont::Start { performer, } =>
                    break 'op performer.next(),
                other_kont =>
                    other_kont,
            };

            {
                let sklavenwelt = sklave_job.sklavenwelt_mut();
                sklavenwelt.env
                    .incoming_orders
                    .extend(sklavenwelt.env.delayed_orders.drain(..));
            }
            while let Some(incoming_order) = sklave_job.sklavenwelt_mut().env.incoming_orders.pop() {
                match incoming_order {
                    Order::Bootstrap(OrderBootstrap { performer, }) => {
                        match kont {
                            Kont::Initialize => {
                                sklave_job.sklavenwelt_mut().kont = Kont::Start { performer, };
                                let env = &mut sklave_job.sklavenwelt_mut().env;
                                env.delayed_orders.extend(env.incoming_orders.drain(..));
                                continue 'op;
                            },
                            Kont::Start { .. } | Kont::PollRequestAndInterpreter { .. } | Kont::PollRequest { .. } =>
                                panic!("unexpected Order::Bootstrap without Kont::Initialize"),
                        }
                    },
                    Order::DeviceSyncDone(OrderDeviceSyncDone { flush_context: rueckkopplung, }) => {
                        if let Err(commit_error) = rueckkopplung.commit(Flushed) {
                            log::warn!("rueckkopplung is gone during Flush query result send: {commit_error:?}");
                        }
                    },
                    Order::Request(request) =>
                        match kont {
                            Kont::PollRequestAndInterpreter { poll, } =>
                                break 'op poll.next.incoming_request(request),
                            Kont::PollRequest { poll, } =>
                                break 'op poll.next.incoming_request(request),
                            Kont::Initialize | Kont::Start { .. } =>
                                sklave_job.sklavenwelt_mut().env.delayed_orders.push(Order::Request(request)),
                        },
                    Order::TaskDoneStats(OrderTaskDoneStats { task_done, stats, }) =>
                        match kont {
                            Kont::PollRequestAndInterpreter { poll, } =>
                                break 'op poll.next.incoming_task_done_stats(task_done, stats),
                            Kont::PollRequest { .. } =>
                                panic!("unexpected Order::TaskDoneStats without poll Kont::PollRequestAndInterpreter"),
                            Kont::Initialize | Kont::Start { .. } =>
                                sklave_job.sklavenwelt_mut().env.delayed_orders.push(Order::TaskDoneStats(OrderTaskDoneStats { task_done, stats, })),
                        },
                    Order::PreparedWriteBlockDone(OrderPreparedWriteBlockDone {
                        output: Ok(interpret::BlockPrepareWriteJobDone { block_id, write_block_bytes, context, }),
                    }) =>
                        match kont {
                            Kont::PollRequestAndInterpreter { poll, } =>
                                break 'op poll.next.prepared_write_block_done(block_id, write_block_bytes, context),
                            Kont::PollRequest { poll, } =>
                                break 'op poll.next.prepared_write_block_done(block_id, write_block_bytes, context),
                            Kont::Initialize | Kont::Start { .. } =>
                                sklave_job.sklavenwelt_mut().env.delayed_orders.push(
                                    Order::PreparedWriteBlockDone(OrderPreparedWriteBlockDone {
                                        output: Ok(interpret::BlockPrepareWriteJobDone { block_id, write_block_bytes, context, }),
                                    }),
                                ),
                        },
                    Order::PreparedWriteBlockDone(OrderPreparedWriteBlockDone { output: Err(error), }) =>
                        return Err(Error::InterpretBlockPrepareWrite(error)),
                    Order::ProcessReadBlockDone(OrderProcessReadBlockDone {
                        output: Ok(interpret::BlockProcessReadJobDone { block_id, block_bytes, pending_contexts, }),
                    }) =>
                        match kont {
                            Kont::PollRequestAndInterpreter { poll, } =>
                                break 'op poll.next.process_read_block_done(block_id, block_bytes, pending_contexts),
                            Kont::PollRequest { poll, } =>
                                break 'op poll.next.process_read_block_done(block_id, block_bytes, pending_contexts),
                            Kont::Initialize | Kont::Start { .. } =>
                                sklave_job.sklavenwelt_mut().env.delayed_orders.push(
                                    Order::ProcessReadBlockDone(OrderProcessReadBlockDone {
                                        output: Ok(interpret::BlockProcessReadJobDone { block_id, block_bytes, pending_contexts, }),
                                    }),
                                ),
                        },
                    Order::ProcessReadBlockDone(OrderProcessReadBlockDone { output: Err(error), }) =>
                        return Err(Error::InterpretBlockProcessRead(error)),
                    Order::PreparedDeleteBlockDone(OrderPreparedDeleteBlockDone {
                        output: Ok(interpret::BlockPrepareDeleteJobDone { block_id, delete_block_bytes, context, }),
                    }) =>
                        match kont {
                            Kont::PollRequestAndInterpreter { poll, } =>
                                break 'op poll.next.prepared_delete_block_done(block_id, delete_block_bytes, context),
                            Kont::PollRequest { poll, } =>
                                break 'op poll.next.prepared_delete_block_done(block_id, delete_block_bytes, context),
                            Kont::Initialize | Kont::Start { .. } =>
                                sklave_job.sklavenwelt_mut().env.delayed_orders.push(
                                    Order::PreparedDeleteBlockDone(OrderPreparedDeleteBlockDone {
                                        output: Ok(interpret::BlockPrepareDeleteJobDone { block_id, delete_block_bytes, context, }),
                                    }),
                                ),
                        },
                    Order::PreparedDeleteBlockDone(OrderPreparedDeleteBlockDone { output: Err(error), }) =>
                        return Err(Error::InterpretBlockPrepareDelete(error)),
                }
            }
            sklave_job.sklavenwelt_mut().kont = kont;

            match sklave_job.zu_ihren_diensten() {
                Ok(arbeitssklave::Gehorsam::Machen { mut befehle, }) =>
                    loop {
                        match befehle.befehl() {
                            arbeitssklave::SklavenBefehl::Mehr { befehl, mut mehr_befehle, } => {
                                mehr_befehle
                                    .sklavenwelt_mut()
                                    .env
                                    .delayed_orders
                                    .push(befehl);
                                befehle = mehr_befehle;
                            },
                            arbeitssklave::SklavenBefehl::Ende { sklave_job: next_sklave_job, } => {
                                sklave_job = next_sklave_job;
                                break;
                            },
                        }
                    },
                Ok(arbeitssklave::Gehorsam::Rasten) =>
                    return Ok(()),
                Err(error) =>
                    return Err(Error::Arbeitssklave(error)),
            }
        };

        loop {
            performer_op = match performer_op {

                performer::Op::Idle(performer) =>
                    performer.next(),

                performer::Op::Query(performer::QueryOp::PollRequestAndInterpreter(poll)) => {
                    sklave_job.sklavenwelt_mut().kont = Kont::PollRequestAndInterpreter { poll, };
                    break;
                },

                performer::Op::Query(performer::QueryOp::PollRequest(poll)) => {
                    sklave_job.sklavenwelt_mut().kont = Kont::PollRequest { poll, };
                    break;
                },

                performer::Op::Query(performer::QueryOp::InterpretTask(performer::InterpretTask { offset, task, next, })) => {
                    sklave_job.sklavenwelt().env.interpreter.push_task(offset, task)
                        .map_err(Error::InterpretPushTask)?;
                    let performer = next.task_accepted();
                    performer.next()
                },

                performer::Op::Event(performer::Event {
                    op: performer::EventOp::Info(
                        performer::TaskDoneOp { context: rueckkopplung, op: performer::InfoOp::Success { info, }, },
                    ),
                    performer,
                }) => {
                    if let Err(commit_error) = rueckkopplung.commit(info) {
                        log::warn!("rueckkopplung is gone during Info query result send: {commit_error:?}");
                    }
                    performer.next()
                },

                performer::Op::Event(performer::Event {
                    op: performer::EventOp::Flush(
                        performer::TaskDoneOp { context: rueckkopplung, op: performer::FlushOp::Flushed, },
                    ),
                    performer,
                }) => {
                    sklave_job.sklavenwelt().env.interpreter.device_sync(rueckkopplung)
                        .map_err(Error::InterpretDeviceSync)?;
                    performer.next()
                },
                performer::Op::Event(performer::Event {
                    op: performer::EventOp::WriteBlock(
                        performer::TaskDoneOp { context: rueckkopplung, op: performer::WriteBlockOp::NoSpaceLeft, },
                    ),
                    performer,
                }) => {
                    if let Err(commit_error) = rueckkopplung.commit(Err(RequestWriteBlockError::NoSpaceLeft)) {
                        log::warn!("rueckkopplung is gone during WriteBlock result send: {commit_error:?}");
                    }
                    performer.next()
                },

                performer::Op::Event(performer::Event {
                    op: performer::EventOp::WriteBlock(
                        performer::TaskDoneOp { context: rueckkopplung, op: performer::WriteBlockOp::Done { block_id, }, },
                    ),
                    performer,
                }) => {
                    if let Err(commit_error) = rueckkopplung.commit(Ok(block_id)) {
                        log::warn!("rueckkopplung is gone during WriteBlock result send: {commit_error:?}");
                    }
                    performer.next()
                },

                performer::Op::Event(performer::Event {
                    op: performer::EventOp::ReadBlock(
                        performer::TaskDoneOp { context: rueckkopplung, op: performer::ReadBlockOp::NotFound, },
                    ),
                    performer,
                }) => {
                    if let Err(commit_error) = rueckkopplung.commit(Err(RequestReadBlockError::NotFound)) {
                        log::warn!("rueckkopplung is gone during ReadBlock result send: {commit_error:?}");
                    }
                    performer.next()
                },

                performer::Op::Event(performer::Event {
                    op: performer::EventOp::ReadBlock(
                        performer::TaskDoneOp { context: rueckkopplung, op: performer::ReadBlockOp::Done { block_bytes, }, },
                    ),
                    performer,
                }) => {
                    if let Err(commit_error) = rueckkopplung.commit(Ok(block_bytes)) {
                        log::warn!("rueckkopplung is gone during ReadBlock result send: {commit_error:?}");
                    }
                    performer.next()
                },

                performer::Op::Event(performer::Event {
                    op: performer::EventOp::DeleteBlock(
                        performer::TaskDoneOp { context: rueckkopplung, op: performer::DeleteBlockOp::NotFound, },
                    ),
                    performer,
                }) => {
                    if let Err(commit_error) = rueckkopplung.commit(Err(RequestDeleteBlockError::NotFound)) {
                        log::warn!("rueckkopplung is gone during DeleteBlock result send: {commit_error:?}");
                    }
                    performer.next()
                },

                performer::Op::Event(performer::Event {
                    op: performer::EventOp::DeleteBlock(
                        performer::TaskDoneOp { context: rueckkopplung, op: performer::DeleteBlockOp::Done { .. }, },
                    ),
                    performer,
                }) => {
                    if let Err(commit_error) = rueckkopplung.commit(Ok(Deleted)) {
                        log::warn!("rueckkopplung is gone during DeleteBlock result send: {commit_error:?}");
                    }
                    performer.next()
                },

                performer::Op::Event(performer::Event {
                    op: performer::EventOp::IterBlocksInit(
                        performer::IterBlocksInitOp {
                            iter_blocks,
                            iter_blocks_init_context: rueckkopplung,
                        },
                    ),
                    performer,
                }) => {
                    if let Err(commit_error) = rueckkopplung.commit(iter_blocks) {
                        log::warn!("rueckkopplung is gone during IterBlocksInit result send: {commit_error:?}");
                    }
                    performer.next()
                },

                performer::Op::Event(performer::Event {
                    op: performer::EventOp::IterBlocksNext(
                        performer::IterBlocksNextOp {
                            item,
                            iter_blocks_next_context: rueckkopplung,
                        },
                    ),
                    performer,
                }) => {
                    if let Err(commit_error) = rueckkopplung.commit(item) {
                        log::warn!("rueckkopplung is gone during IterBlocksNext result send: {commit_error:?}");
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
                        blocks_pool: sklave_job.sklavenwelt().env.blocks_pool.clone(),
                        context,
                        meister: sklave_job.meister(),
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
                        blocks_pool: sklave_job.sklavenwelt().env.blocks_pool.clone(),
                        context,
                        meister: sklave_job.meister(),
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
                        meister: sklave_job.meister(),
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

impl<A> fmt::Debug for Order<A> where A: AccessPolicy {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Order::Bootstrap(..) =>
                fmt.debug_tuple("Order::Bootstrap").finish(),
            Order::Request(..) =>
                fmt.debug_tuple("Order::Request").finish(),
            Order::TaskDoneStats(..) =>
                fmt.debug_tuple("Order::TaskDoneStats").finish(),
            Order::DeviceSyncDone(..) =>
                fmt.debug_tuple("Order::DeviceSyncDone").finish(),
            Order::PreparedWriteBlockDone(..) =>
                fmt.debug_tuple("Order::PreparedWriteBlockDone").finish(),
            Order::ProcessReadBlockDone(..) =>
                fmt.debug_tuple("Order::ProcessReadBlockDone").finish(),
            Order::PreparedDeleteBlockDone(..) =>
                fmt.debug_tuple("Order::PreparedDeleteBlockDone").finish(),
        }
    }
}
