use std::{
    mem,
    fmt,
};

use super::{
    lru,
    task,
    pool,
    proto,
    block,
    schema,
    defrag,
    context::Context,
};

#[cfg(test)]
mod tests;

struct Inner<C> where C: Context {
    schema: schema::Schema,
    lru_cache: lru::Cache,
    blocks_pool: pool::Blocks,
    tasks_queue: task::Queue<C>,
    defrag_queues: Option<defrag::Queues<C::WriteBlock>>,
    bg_task: BackgroundTask<C::Interpreter>,
}

impl<C> fmt::Debug for Inner<C> where C: Context {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt.write_str("<inner>")
    }
}

struct BackgroundTask<C> {
    current_offset: u64,
    state: BackgroundTaskState<C>,
}

enum BackgroundTaskState<C> {
    Idle,
    InProgress { interpreter_context: C, },
}

#[derive(Debug)]
pub struct Performer<C> where C: Context {
    inner: Inner<C>,
}

#[derive(Debug)]
pub enum Op<C> where C: Context {
    PollRequestAndInterpreter(PollRequestAndInterpreter<C>),
    PollRequest(PollRequest<C>),
    InterpretTask(InterpretTask<C>),
}

impl<C> Performer<C> where C: Context {
    pub fn new(
        schema: schema::Schema,
        lru_cache: lru::Cache,
        defrag_queues: Option<defrag::Queues<C::WriteBlock>>,
    )
        -> Performer<C>
    {
        Performer {
            inner: Inner::new(schema, lru_cache, defrag_queues),
        }
    }

    pub fn next(mut self) -> Op<C> {
        match mem::replace(&mut self.inner.bg_task.state, BackgroundTaskState::Idle) {
            BackgroundTaskState::Idle =>
                if let Some((offset, task_kind)) = self.inner.tasks_queue.pop(self.inner.bg_task.current_offset) {
                    Op::InterpretTask(InterpretTask {
                        offset, task_kind,
                        next: InterpretTaskNext {
                            inner: self.inner,
                        },
                    })
                } else {
                    Op::PollRequest(PollRequest {
                        next: PollRequestNext {
                            inner: self.inner,
                        },
                    })
                },
            BackgroundTaskState::InProgress { interpreter_context, } =>
                Op::PollRequestAndInterpreter(PollRequestAndInterpreter {
                    interpreter_context,
                    next: PollRequestAndInterpreterNext {
                        inner: self.inner,
                    },
                }),
        }
    }
}

pub struct PollRequestAndInterpreter<C> where C: Context {
    pub interpreter_context: C::Interpreter,
    pub next: PollRequestAndInterpreterNext<C>,
}

impl<C> fmt::Debug for PollRequestAndInterpreter<C> where C: Context {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt.debug_struct("PollRequestAndInterpreter")
            .field("interpreter_context", &"<..>")
            .field("next", &"PollRequestAndInterpreterNext { inner: <inner>, }")
            .finish()
    }
}

#[derive(Debug)]
pub struct PollRequestAndInterpreterNext<C> where C: Context {
    inner: Inner<C>,
}

enum RequestOrInterpreterIncoming<C> where C: Context {
    Request(proto::Request<C>),
    Interpreter(task::Done<C>),
}

impl<C> PollRequestAndInterpreterNext<C> where C: Context {
    pub fn incoming_request(mut self, request: proto::Request<C>, interpreter_context: C::Interpreter) -> PerformOp<C> {
        self.inner.bg_task.state = BackgroundTaskState::InProgress { interpreter_context, };
        self.inner.next(RequestOrInterpreterIncoming::Request(request))
    }

    pub fn incoming_task_done(self, task_done: task::Done<C>) -> PerformOp<C> {
        self.inner.next(RequestOrInterpreterIncoming::Interpreter(task_done))
    }
}

#[derive(Debug)]
pub struct PollRequest<C> where C: Context {
    pub next: PollRequestNext<C>,
}

#[derive(Debug)]
pub struct PollRequestNext<C> where C: Context {
    inner: Inner<C>,
}

impl<C> PollRequestNext<C> where C: Context {
    pub fn incoming_request(self, request: proto::Request<C>) -> PerformOp<C> {
        self.inner.next(RequestOrInterpreterIncoming::Request(request))
    }
}

#[derive(Debug)]
pub struct InterpretTask<C> where C: Context {
    pub offset: u64,
    pub task_kind: task::TaskKind<C>,
    pub next: InterpretTaskNext<C>,
}

#[derive(Debug)]
pub struct InterpretTaskNext<C> where C: Context {
    inner: Inner<C>,
}

impl<C> InterpretTaskNext<C> where C: Context {
    pub fn task_accepted(mut self, interpreter_context: C::Interpreter) -> Performer<C> {
        self.inner.bg_task.state = BackgroundTaskState::InProgress { interpreter_context, };
        Performer { inner: self.inner, }
    }
}

#[derive(Debug)]
pub enum PerformOp<C> where C: Context {
    Idle(Performer<C>),
    LendBlock(LendBlockOp<C>),
    WriteBlock(WriteBlockOp<C>),
    ReadBlock(ReadBlockOp<C>),
    DeleteBlock(DeleteBlockOp<C>),
    WriteBlockDone(WriteBlockDoneOp<C>),
    ReadBlockDone(ReadBlockDoneOp<C>),
    DeleteBlockDone(DeleteBlockDoneOp<C>),
}

#[derive(Debug)]
pub enum LendBlockOp<C> where C: Context {
    Success {
        block_bytes: block::BytesMut,
        context: C::LendBlock,
        performer: Performer<C>,
    },
}

#[derive(Debug)]
pub enum WriteBlockOp<C> where C: Context {
    NoSpaceLeft {
        context: C::WriteBlock,
        performer: Performer<C>,
    },
}

#[derive(Debug)]
pub enum ReadBlockOp<C> where C: Context {
    CacheHit {
        context: C::ReadBlock,
        block_bytes: block::Bytes,
        performer: Performer<C>,
    },
    NotFound {
        context: C::ReadBlock,
        performer: Performer<C>,
    },
}

#[derive(Debug)]
pub enum DeleteBlockOp<C> where C: Context {
    NotFound {
        context: C::DeleteBlock,
        performer: Performer<C>,
    },
}

#[derive(Debug)]
pub enum WriteBlockDoneOp<C> where C: Context {
    Done {
        block_id: block::Id,
        context: C::WriteBlock,
        performer: Performer<C>,
    },
}

#[derive(Debug)]
pub enum ReadBlockDoneOp<C> where C: Context {
    Done {
        block_bytes: block::Bytes,
        context: C::ReadBlock,
        performer: Performer<C>,
    },
}

#[derive(Debug)]
pub enum DeleteBlockDoneOp<C> where C: Context {
    Done {
        context: C::DeleteBlock,
        performer: Performer<C>,
    },
}

impl<C> Inner<C> where C: Context {
    fn new(
        schema: schema::Schema,
        lru_cache: lru::Cache,
        defrag_queues: Option<defrag::Queues<C::WriteBlock>>,
    )
        -> Inner<C>
    {
        Inner {
            schema,
            lru_cache,
            blocks_pool: pool::Blocks::new(),
            tasks_queue: task::Queue::new(),
            defrag_queues,
            bg_task: BackgroundTask {
                current_offset: 0,
                state: BackgroundTaskState::Idle,
            },
        }
    }

    fn next(mut self, incoming: RequestOrInterpreterIncoming<C>) -> PerformOp<C> {
        match incoming {

            RequestOrInterpreterIncoming::Request(proto::Request::LendBlock(proto::RequestLendBlock { context, })) => {
                let block_bytes = self.blocks_pool.lend();
                PerformOp::LendBlock(LendBlockOp::Success {
                    block_bytes, context,
                    performer: Performer { inner: self, },
                })
            },

            RequestOrInterpreterIncoming::Request(proto::Request::RepayBlock(proto::RequestRepayBlock { block_bytes, })) => {
                self.blocks_pool.repay(block_bytes);
                PerformOp::Idle(Performer { inner: self, })
            },

            RequestOrInterpreterIncoming::Request(proto::Request::WriteBlock(request_write_block)) =>
                match self.schema.process_write_block_request(&request_write_block.block_bytes) {

                    schema::WriteBlockOp::Perform(schema::WriteBlockPerform { defrag_op, task_op, }) => {
                        match (defrag_op, self.defrag_queues.as_mut()) {
                            (schema::DefragOp::Queue { free_space_offset, space_key, }, Some(defrag::Queues { tasks, .. })) =>
                                tasks.push(free_space_offset, space_key),
                            (schema::DefragOp::None, _) | (_, None) =>
                                (),
                        }
                        self.lru_cache.insert(
                            task_op.block_id.clone(),
                            request_write_block.block_bytes.clone(),
                        );
                        self.tasks_queue.push(
                            self.bg_task.current_offset,
                            task_op.block_offset,
                            task::TaskKind::WriteBlock(
                                task::WriteBlock {
                                    block_id: task_op.block_id,
                                    block_bytes: request_write_block.block_bytes,
                                    commit_type: match task_op.commit_type {
                                        schema::WriteBlockTaskCommitType::CommitOnly =>
                                            task::CommitType::CommitOnly,
                                        schema::WriteBlockTaskCommitType::CommitAndEof =>
                                            task::CommitType::CommitAndEof,
                                    },
                                    context: task::WriteBlockContext::External(
                                        request_write_block.context,
                                    ),
                                },
                            ),
                        );
                        PerformOp::Idle(Performer { inner: self, })
                    },

                    schema::WriteBlockOp::QueuePendingDefrag => {
                        log::debug!(
                            "cannot directly allocate {} bytes in process_write_block_request: moving to pending defrag queue",
                            request_write_block.block_bytes.len(),
                        );
                        if let Some(defrag::Queues { pending, .. }) = self.defrag_queues.as_mut() {
                            pending.push(request_write_block);
                        }
                        PerformOp::Idle(Performer { inner: self, })
                    },

                    schema::WriteBlockOp::ReplyNoSpaceLeft =>
                        PerformOp::WriteBlock(WriteBlockOp::NoSpaceLeft {
                            context: request_write_block.context,
                            performer: Performer { inner: self, },
                        }),
                },

            RequestOrInterpreterIncoming::Request(proto::Request::ReadBlock(request_read_block)) =>
                if let Some(block_bytes) = self.lru_cache.get(&request_read_block.block_id) {
                    PerformOp::ReadBlock(ReadBlockOp::CacheHit {
                        block_bytes: block_bytes.clone(),
                        context: request_read_block.context,
                        performer: Performer { inner: self, },
                    })
                } else {
                    match self.schema.process_read_block_request(&request_read_block.block_id) {

                        schema::ReadBlockOp::Perform(schema::ReadBlockPerform { block_offset, block_header, }) => {
                            let block_bytes = self.blocks_pool.lend();
                            self.tasks_queue.push(
                                self.bg_task.current_offset,
                                block_offset,
                                task::TaskKind::ReadBlock(task::ReadBlock {
                                    block_header: block_header,
                                    block_bytes,
                                    context: task::ReadBlockContext::External(
                                        request_read_block.context,
                                    ),
                                }),
                            );
                            PerformOp::Idle(Performer { inner: self, })
                        },

                        schema::ReadBlockOp::NotFound =>
                            PerformOp::ReadBlock(ReadBlockOp::NotFound {
                                context: request_read_block.context,
                                performer: Performer { inner: self, },
                            }),

                    }
                },

            RequestOrInterpreterIncoming::Request(proto::Request::DeleteBlock(request_delete_block)) => {
                self.lru_cache.invalidate(&request_delete_block.block_id);
                match self.schema.process_delete_block_request(&request_delete_block.block_id) {

                    schema::DeleteBlockOp::Perform(schema::DeleteBlockPerform { block_offset, }) => {
                        self.tasks_queue.push(
                            self.bg_task.current_offset,
                            block_offset,
                            task::TaskKind::MarkTombstone(task::MarkTombstone {
                                block_id: request_delete_block.block_id,
                                context: task::MarkTombstoneContext::External(
                                    request_delete_block.context,
                                ),
                            }),
                        );
                        PerformOp::Idle(Performer { inner: self, })
                    },

                    schema::DeleteBlockOp::NotFound =>
                        PerformOp::DeleteBlock(DeleteBlockOp::NotFound {
                            context: request_delete_block.context,
                            performer: Performer { inner: self, },
                        }),

                }
            },

            RequestOrInterpreterIncoming::Interpreter(
                task::Done { current_offset, task: task::TaskDone::WriteBlock(write_block), },
            ) => {
                self.bg_task = BackgroundTask { current_offset, state: BackgroundTaskState::Idle, };
                match write_block.context {
                    task::WriteBlockContext::External(context) =>
                        PerformOp::WriteBlockDone(WriteBlockDoneOp::Done {
                            block_id: write_block.block_id,
                            context,
                            performer: Performer { inner: self, },
                        }),
                }
            },

            RequestOrInterpreterIncoming::Interpreter(
                task::Done { current_offset, task: task::TaskDone::ReadBlock(read_block), },
            ) => {
                let block_bytes = read_block.block_bytes.freeze();
                self.lru_cache.insert(read_block.block_id.clone(), block_bytes.clone());
                self.bg_task = BackgroundTask { current_offset, state: BackgroundTaskState::Idle, };
                match read_block.context {
                    task::ReadBlockContext::External(context) =>
                        PerformOp::ReadBlockDone(ReadBlockDoneOp::Done {
                            block_bytes,
                            context,
                            performer: Performer { inner: self, },
                        }),
                }
            },

            RequestOrInterpreterIncoming::Interpreter(
                task::Done { current_offset, task: task::TaskDone::MarkTombstone(mark_tombstone), },
            ) => {
                match self.schema.process_tombstone_written(mark_tombstone.block_id) {
                    schema::TombstoneWrittenOp::Perform(schema::TombstoneWrittenPerform { defrag_op, }) => {
                        match (defrag_op, self.defrag_queues.as_mut()) {
                            (schema::DefragOp::Queue { free_space_offset, space_key, }, Some(defrag::Queues { tasks, .. })) =>
                                tasks.push(free_space_offset, space_key),
                            (schema::DefragOp::None, _) | (_, None) =>
                                (),
                        }
                    },
                }
                self.bg_task = BackgroundTask { current_offset, state: BackgroundTaskState::Idle, };
                match mark_tombstone.context {
                    task::MarkTombstoneContext::External(context) =>
                        PerformOp::DeleteBlockDone(DeleteBlockDoneOp::Done {
                            context,
                            performer: Performer { inner: self, },
                        }),
                }
            },

        }
    }
}
