use alloc_pool::{
    bytes::{
        BytesPool,
    },
};

use arbeitssklave::{
    komm::{
        Echo,
    },
};

use crate::{
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
    EchoPolicy,
    InterpretStats,
    RequestWriteBlockError,
    RequestReadBlockError,
    RequestDeleteBlockError,
};

pub enum Order<E> where E: EchoPolicy {
    Bootstrap(OrderBootstrap<E>),
    Request(proto::Request<Context<E>>),
    TaskDoneStats(OrderTaskDoneStats<E>),
    DeviceSyncDone(OrderDeviceSyncDone<E>),
    PreparedWriteBlockDone(OrderPreparedWriteBlockDone<E>),
    ProcessReadBlockDone(OrderProcessReadBlockDone),
    PreparedDeleteBlockDone(OrderPreparedDeleteBlockDone<E>),
}

pub struct OrderBootstrap<E> where E: EchoPolicy {
    pub performer: performer::Performer<Context<E>>,
}

pub struct OrderTaskDoneStats<E> where E: EchoPolicy {
    pub task_done: task::Done<Context<E>>,
    pub stats: InterpretStats,
}

pub struct OrderDeviceSyncDone<E> where E: EchoPolicy {
    pub flush_context: <Context<E> as context::Context>::Flush,
}

pub struct OrderPreparedWriteBlockDone<E> where E: EchoPolicy {
    pub output: interpret::BlockPrepareWriteJobOutput<E>,
}

pub struct OrderProcessReadBlockDone {
    pub output: interpret::BlockProcessReadJobOutput,
}

pub struct OrderPreparedDeleteBlockDone<E> where E: EchoPolicy {
    pub output: interpret::BlockPrepareDeleteJobOutput<E>,
}

pub struct Env<E> where E: EchoPolicy {
    pub interpreter: interpret::Interpreter<E>,
    pub blocks_pool: BytesPool,
    pub incoming_orders: Vec<Order<E>>,
    pub delayed_orders: Vec<Order<E>>,
}

pub struct Welt<E> where E: EchoPolicy {
    pub env: Env<E>,
    pub kont: Kont<E>,
}

pub enum Kont<E> where E: EchoPolicy {
    Initialize,
    Start {
        performer: performer::Performer<Context<E>>,
    },
    PollRequestAndInterpreter {
        poll: performer::PollRequestAndInterpreter<Context<E>>,
    },
    PollRequest {
        poll: performer::PollRequest<Context<E>>,
    },
}

pub type Meister<E> = arbeitssklave::Meister<Welt<E>, Order<E>>;
pub type SklaveJob<E> = arbeitssklave::SklaveJob<Welt<E>, Order<E>>;
pub type WriteBlockContext<E> = task::WriteBlockContext<<Context<E> as context::Context>::WriteBlock>;
pub type DeleteBlockContext<E> = task::DeleteBlockContext<<Context<E> as context::Context>::DeleteBlock>;

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

pub fn run_job<E, J>(sklave_job: SklaveJob<E>, thread_pool: &edeltraud::Handle<J>)
where E: EchoPolicy,
      J: From<interpret::BlockPrepareWriteJobArgs<E>>,
      J: From<interpret::BlockPrepareDeleteJobArgs<E>>,
      J: From<interpret::BlockProcessReadJobArgs<E>>,
{
    if let Err(error) = job(sklave_job, thread_pool) {
        log::error!("terminated with an error: {error:?}");
    }
}

fn job<E, J>(mut sklave_job: SklaveJob<E>, thread_pool: &edeltraud::Handle<J>) -> Result<(), Error>
where E: EchoPolicy,
      J: From<interpret::BlockPrepareWriteJobArgs<E>>,
      J: From<interpret::BlockPrepareDeleteJobArgs<E>>,
      J: From<interpret::BlockProcessReadJobArgs<E>>,
{
    loop {
        let mut performer_op = 'op: loop {
            let kont = match std::mem::replace(&mut sklave_job.kont, Kont::Initialize) {
                Kont::Start { performer, } =>
                    break 'op performer.next(),
                other_kont =>
                    other_kont,
            };

            let sklavenwelt = &mut *sklave_job;
            sklavenwelt.env
                .incoming_orders
                .append(&mut sklavenwelt.env.delayed_orders);
            while let Some(incoming_order) = sklavenwelt.env.incoming_orders.pop() {
                match incoming_order {
                    Order::Bootstrap(OrderBootstrap { performer, }) => {
                        match kont {
                            Kont::Initialize => {
                                sklavenwelt.kont = Kont::Start { performer, };
                                let env = &mut sklavenwelt.env;
                                env.delayed_orders.append(&mut env.incoming_orders);
                                continue 'op;
                            },
                            Kont::Start { .. } | Kont::PollRequestAndInterpreter { .. } | Kont::PollRequest { .. } =>
                                panic!("unexpected Order::Bootstrap without Kont::Initialize"),
                        }
                    },
                    Order::DeviceSyncDone(OrderDeviceSyncDone { flush_context: echo, }) => {
                        if let Err(commit_error) = echo.commit_echo(Flushed) {
                            log::warn!("echo is gone during Flush query result send: {commit_error:?}");
                        }
                    },
                    Order::Request(request) =>
                        match kont {
                            Kont::PollRequestAndInterpreter { poll, } =>
                                break 'op poll.next.incoming_request(request),
                            Kont::PollRequest { poll, } =>
                                break 'op poll.next.incoming_request(request),
                            Kont::Initialize | Kont::Start { .. } =>
                                sklavenwelt.env.delayed_orders.push(Order::Request(request)),
                        },
                    Order::TaskDoneStats(OrderTaskDoneStats { task_done, stats, }) =>
                        match kont {
                            Kont::PollRequestAndInterpreter { poll, } =>
                                break 'op poll.next.incoming_task_done_stats(task_done, stats),
                            Kont::PollRequest { .. } =>
                                panic!("unexpected Order::TaskDoneStats without poll Kont::PollRequestAndInterpreter"),
                            Kont::Initialize | Kont::Start { .. } =>
                                sklavenwelt.env.delayed_orders.push(Order::TaskDoneStats(OrderTaskDoneStats { task_done, stats, })),
                        },
                    Order::PreparedWriteBlockDone(OrderPreparedWriteBlockDone {
                        output: Ok(interpret::BlockPrepareWriteJobDone {
                            block_id,
                            write_block_bytes,
                            context,
                        }),
                    }) =>
                        match kont {
                            Kont::PollRequestAndInterpreter { poll, } =>
                                break 'op poll.next.prepared_write_block_done(block_id, write_block_bytes, context),
                            Kont::PollRequest { poll, } =>
                                break 'op poll.next.prepared_write_block_done(block_id, write_block_bytes, context),
                            Kont::Initialize | Kont::Start { .. } =>
                                sklavenwelt.env.delayed_orders.push(
                                    Order::PreparedWriteBlockDone(OrderPreparedWriteBlockDone {
                                        output: Ok(interpret::BlockPrepareWriteJobDone { block_id, write_block_bytes, context, }),
                                    }),
                                ),
                        },
                    Order::PreparedWriteBlockDone(OrderPreparedWriteBlockDone {
                        output: Err(error),
                    }) =>
                        return Err(Error::InterpretBlockPrepareWrite(error)),
                    Order::ProcessReadBlockDone(OrderProcessReadBlockDone {
                        output: Ok(interpret::BlockProcessReadJobDone {
                            block_id,
                            block_bytes,
                            pending_contexts,
                        }),
                    }) =>
                        match kont {
                            Kont::PollRequestAndInterpreter { poll, } =>
                                break 'op poll.next.process_read_block_done(block_id, block_bytes, pending_contexts),
                            Kont::PollRequest { poll, } =>
                                break 'op poll.next.process_read_block_done(block_id, block_bytes, pending_contexts),
                            Kont::Initialize | Kont::Start { .. } =>
                                sklavenwelt.env.delayed_orders.push(
                                    Order::ProcessReadBlockDone(OrderProcessReadBlockDone {
                                        output: Ok(interpret::BlockProcessReadJobDone { block_id, block_bytes, pending_contexts, }),
                                    }),
                                ),
                        },
                    Order::ProcessReadBlockDone(OrderProcessReadBlockDone {
                        output: Err(error),
                    }) =>
                        return Err(Error::InterpretBlockProcessRead(error)),
                    Order::PreparedDeleteBlockDone(OrderPreparedDeleteBlockDone {
                        output: Ok(interpret::BlockPrepareDeleteJobDone {
                            block_id,
                            delete_block_bytes,
                            context,
                        }),
                    }) =>
                        match kont {
                            Kont::PollRequestAndInterpreter { poll, } =>
                                break 'op poll.next.prepared_delete_block_done(block_id, delete_block_bytes, context),
                            Kont::PollRequest { poll, } =>
                                break 'op poll.next.prepared_delete_block_done(block_id, delete_block_bytes, context),
                            Kont::Initialize | Kont::Start { .. } =>
                                sklavenwelt.env.delayed_orders.push(
                                    Order::PreparedDeleteBlockDone(OrderPreparedDeleteBlockDone {
                                        output: Ok(interpret::BlockPrepareDeleteJobDone { block_id, delete_block_bytes, context, }),
                                    }),
                                ),
                        },
                    Order::PreparedDeleteBlockDone(OrderPreparedDeleteBlockDone { output: Err(error), }) =>
                        return Err(Error::InterpretBlockPrepareDelete(error)),
                }
            }

            sklavenwelt.kont = kont;

            match sklave_job.zu_ihren_diensten() {
                Ok(arbeitssklave::Gehorsam::Machen {
                    mut befehle,
                }) =>
                    loop {
                        match befehle.befehl() {
                            arbeitssklave::SklavenBefehl::Mehr {
                                befehl,
                                mut mehr_befehle,
                            } => {
                                mehr_befehle
                                    .env
                                    .delayed_orders
                                    .push(befehl);
                                befehle = mehr_befehle;
                            },
                            arbeitssklave::SklavenBefehl::Ende {
                                sklave_job: next_sklave_job,
                            } => {
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

                performer::Op::Query(performer::QueryOp::PollRequestAndInterpreter(
                    poll,
                )) => {
                    sklave_job.kont = Kont::PollRequestAndInterpreter { poll, };
                    break;
                },

                performer::Op::Query(performer::QueryOp::PollRequest(poll)) => {
                    sklave_job.kont = Kont::PollRequest { poll, };
                    break;
                },

                performer::Op::Query(performer::QueryOp::InterpretTask(performer::InterpretTask {
                    offset,
                    task,
                    next,
                })) => {
                    sklave_job.env.interpreter.push_task(offset, task)
                        .map_err(Error::InterpretPushTask)?;
                    let performer = next.task_accepted();
                    performer.next()
                },

                performer::Op::Event(performer::Event {
                    op: performer::EventOp::Info(
                        performer::TaskDoneOp {
                            context: echo,
                            op: performer::InfoOp::Success { info, },
                        },
                    ),
                    performer,
                }) => {
                    if let Err(commit_error) = echo.commit_echo(info) {
                        log::warn!("echo is gone during Info query result send: {commit_error:?}");
                    }
                    performer.next()
                },

                performer::Op::Event(performer::Event {
                    op: performer::EventOp::Flush(
                        performer::TaskDoneOp {
                            context: echo,
                            op: performer::FlushOp::Flushed,
                        },
                    ),
                    performer,
                }) => {
                    sklave_job.env.interpreter.device_sync(echo)
                        .map_err(Error::InterpretDeviceSync)?;
                    performer.next()
                },
                performer::Op::Event(performer::Event {
                    op: performer::EventOp::WriteBlock(
                        performer::TaskDoneOp {
                            context: echo,
                            op: performer::WriteBlockOp::NoSpaceLeft,
                        },
                    ),
                    performer,
                }) => {
                    if let Err(commit_error) = echo.commit_echo(Err(RequestWriteBlockError::NoSpaceLeft)) {
                        log::warn!("echo is gone during WriteBlock result send: {commit_error:?}");
                    }
                    performer.next()
                },

                performer::Op::Event(performer::Event {
                    op: performer::EventOp::WriteBlock(
                        performer::TaskDoneOp {
                            context: echo,
                            op: performer::WriteBlockOp::Done { block_id, },
                        },
                    ),
                    performer,
                }) => {
                    if let Err(commit_error) = echo.commit_echo(Ok(block_id)) {
                        log::warn!("echo is gone during WriteBlock result send: {commit_error:?}");
                    }
                    performer.next()
                },

                performer::Op::Event(performer::Event {
                    op: performer::EventOp::ReadBlock(
                        performer::TaskDoneOp {
                            context: echo,
                            op: performer::ReadBlockOp::NotFound,
                        },
                    ),
                    performer,
                }) => {
                    if let Err(commit_error) = echo.commit_echo(Err(RequestReadBlockError::NotFound)) {
                        log::warn!("echo is gone during ReadBlock result send: {commit_error:?}");
                    }
                    performer.next()
                },

                performer::Op::Event(performer::Event {
                    op: performer::EventOp::ReadBlock(
                        performer::TaskDoneOp {
                            context: echo,
                            op: performer::ReadBlockOp::Done { block_bytes, },
                        },
                    ),
                    performer,
                }) => {
                    if let Err(commit_error) = echo.commit_echo(Ok(block_bytes)) {
                        log::warn!("echo is gone during ReadBlock result send: {commit_error:?}");
                    }
                    performer.next()
                },

                performer::Op::Event(performer::Event {
                    op: performer::EventOp::DeleteBlock(
                        performer::TaskDoneOp {
                            context: echo,
                            op: performer::DeleteBlockOp::NotFound,
                        },
                    ),
                    performer,
                }) => {
                    if let Err(commit_error) = echo.commit_echo(Err(RequestDeleteBlockError::NotFound)) {
                        log::warn!("echo is gone during DeleteBlock result send: {commit_error:?}");
                    }
                    performer.next()
                },

                performer::Op::Event(performer::Event {
                    op: performer::EventOp::DeleteBlock(
                        performer::TaskDoneOp {
                            context: echo,
                            op: performer::DeleteBlockOp::Done { .. },
                        },
                    ),
                    performer,
                }) => {
                    if let Err(commit_error) = echo.commit_echo(Ok(Deleted)) {
                        log::warn!("echo is gone during DeleteBlock result send: {commit_error:?}");
                    }
                    performer.next()
                },

                performer::Op::Event(performer::Event {
                    op: performer::EventOp::IterBlocksInit(
                        performer::IterBlocksInitOp {
                            iter_blocks,
                            iter_blocks_init_context: echo,
                        },
                    ),
                    performer,
                }) => {
                    if let Err(commit_error) = echo.commit_echo(iter_blocks) {
                        log::warn!("echo is gone during IterBlocksInit result send: {commit_error:?}");
                    }
                    performer.next()
                },

                performer::Op::Event(performer::Event {
                    op: performer::EventOp::IterBlocksNext(
                        performer::IterBlocksNextOp {
                            item,
                            iter_blocks_next_context: echo,
                        },
                    ),
                    performer,
                }) => {
                    if let Err(commit_error) = echo.commit_echo(item) {
                        log::warn!("echo is gone during IterBlocksNext result send: {commit_error:?}");
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
                        blocks_pool: sklave_job.env.blocks_pool.clone(),
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
                        blocks_pool: sklave_job.env.blocks_pool.clone(),
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

impl<E> fmt::Debug for Order<E> where E: EchoPolicy {
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
