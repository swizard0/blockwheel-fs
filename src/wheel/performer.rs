use std::mem;

use futures::{
    channel::{
        oneshot,
    },
};

use super::{
    lru,
    task,
    pool,
    proto,
    block,
    schema,
    defrag,
};

struct Inner<IC> {
    schema: schema::Schema,
    lru_cache: lru::Cache,
    blocks_pool: pool::Blocks,
    tasks_queue: task::Queue,
    defrag_pending_queue: defrag::PendingQueue,
    defrag_task_queue: defrag::TaskQueue,
    bg_task: BackgroundTask<IC>,
}

struct BackgroundTask<IC> {
    current_offset: u64,
    state: BackgroundTaskState<IC>,
}

enum BackgroundTaskState<IC> {
    Idle,
    InProgress { interpreter_context: IC, },
}

pub struct Performer<IC> {
    inner: Inner<IC>,
}

pub enum Op<IC> {
    PollRequestAndInterpreter(PollRequestAndInterpreter<IC>),
    PollRequest(PollRequest<IC>),
    InterpretTask(InterpretTask<IC>),
}

impl<IC> Performer<IC> {
    pub fn new(schema: schema::Schema, lru_bytes_limit: usize) -> Performer<IC> {
        Performer {
            inner: Inner::new(schema, lru_bytes_limit),
        }
    }

    pub fn next(mut self) -> Op<IC> {
        match mem::replace(&mut self.inner.bg_task.state, BackgroundTaskState::Idle) {
            BackgroundTaskState::Idle =>
                if let Some((offset, task_kind)) = self.inner.tasks_queue.pop() {
                    Op::InterpretTask(InterpretTask {
                        offset, task_kind,
                        next: InterpretTaskNext {
                            inner: self.inner,
                        },
                    })
                } else {
                    Op::PollRequest(PollRequest {
                        inner: self.inner,
                    })
                },
            BackgroundTaskState::InProgress { interpreter_context, } =>
                Op::PollRequestAndInterpreter(PollRequestAndInterpreter {
                    interpreter_context,
                    inner: self.inner,
                }),
        }
    }
}

pub struct PollRequestAndInterpreter<IC> {
    pub interpreter_context: IC,
    inner: Inner<IC>,
}

pub enum RequestOrInterpreterIncoming {
    Request(proto::Request),
    Interpreter(task::Done),
}

impl<IC> PollRequestAndInterpreter<IC> {
    pub fn next(self, incoming: RequestOrInterpreterIncoming) -> PerformOp<IC> {
        self.inner.next(incoming)
    }
}

pub struct PollRequest<IC> {
    inner: Inner<IC>,
}

impl<IC> PollRequest<IC> {
    pub fn next(self, incoming: proto::Request) -> PerformOp<IC> {
        self.inner.next(RequestOrInterpreterIncoming::Request(incoming))
    }
}

pub struct InterpretTask<IC> {
    pub offset: u64,
    pub task_kind: task::TaskKind,
    pub next: InterpretTaskNext<IC>,
}

pub struct InterpretTaskNext<IC> {
    inner: Inner<IC>,
}

impl<IC> InterpretTaskNext<IC> {
    pub fn task_accepted(mut self, interpreter_context: IC) -> Performer<IC> {
        self.inner.bg_task.state = BackgroundTaskState::InProgress { interpreter_context, };
        Performer { inner: self.inner, }
    }
}

pub enum PerformOp<IC> {
    Idle(Performer<IC>),
    LendBlock(LendBlockOp<IC>),
    WriteBlock(WriteBlockOp<IC>),
    ReadBlock(ReadBlockOp<IC>),
}

pub enum LendBlockOp<IC> {
    Success {
        block_bytes: block::BytesMut,
        reply_tx: oneshot::Sender<block::BytesMut>,
        performer: Performer<IC>,
    },
}

pub enum WriteBlockOp<IC> {
    NoSpaceLeft {
        reply_tx: oneshot::Sender<Result<block::Id, proto::RequestWriteBlockError>>,
        performer: Performer<IC>,
    },
}

pub enum ReadBlockOp<IC> {
    CacheHit {
        reply_tx: oneshot::Sender<Result<block::Bytes, proto::RequestReadBlockError>>,
        block_bytes: block::Bytes,
        performer: Performer<IC>,
    },
    NotFound {
        reply_tx: oneshot::Sender<Result<block::Bytes, proto::RequestReadBlockError>>,
        performer: Performer<IC>,
    },
}

impl<IC> Inner<IC> {
    fn new(schema: schema::Schema, lru_bytes_limit: usize) -> Inner<IC> {
        Inner {
            schema,
            lru_cache: lru::Cache::new(lru_bytes_limit),
            blocks_pool: pool::Blocks::new(),
            tasks_queue: task::Queue::new(),
            defrag_pending_queue: defrag::PendingQueue::new(),
            defrag_task_queue: defrag::TaskQueue::new(),
            bg_task: BackgroundTask {
                current_offset: 0,
                state: BackgroundTaskState::Idle,
            },
        }
    }

    fn next(mut self, incoming: RequestOrInterpreterIncoming) -> PerformOp<IC> {
        match incoming {

            RequestOrInterpreterIncoming::Request(proto::Request::LendBlock(proto::RequestLendBlock { reply_tx, })) => {
                let block_bytes = self.blocks_pool.lend();
                PerformOp::LendBlock(LendBlockOp::Success {
                    block_bytes, reply_tx,
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
                        match defrag_op {
                            schema::DefragOp::None =>
                                (),
                            schema::DefragOp::Queue { free_space_offset, space_key, } =>
                                self.defrag_task_queue.push(free_space_offset, space_key),
                        }
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
                                        task::WriteBlockContextExternal {
                                            reply_tx: request_write_block.reply_tx,
                                        },
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
                        self.defrag_pending_queue.push(request_write_block);
                        PerformOp::Idle(Performer { inner: self, })
                    },

                    schema::WriteBlockOp::ReplyNoSpaceLeft =>
                        PerformOp::WriteBlock(WriteBlockOp::NoSpaceLeft {
                            reply_tx: request_write_block.reply_tx,
                            performer: Performer { inner: self, },
                        }),
                },

            RequestOrInterpreterIncoming::Request(proto::Request::ReadBlock(request_read_block)) =>
                if let Some(block_bytes) = self.lru_cache.get(&request_read_block.block_id) {
                    PerformOp::ReadBlock(ReadBlockOp::CacheHit {
                        block_bytes: block_bytes.clone(),
                        reply_tx: request_read_block.reply_tx,
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
                                        task::ReadBlockContextExternal {
                                            reply_tx: request_read_block.reply_tx,
                                        },
                                    ),
                                }),
                            );
                            PerformOp::Idle(Performer { inner: self, })
                        },

                        schema::ReadBlockOp::NotFound =>
                            PerformOp::ReadBlock(ReadBlockOp::NotFound {
                                reply_tx: request_read_block.reply_tx,
                                performer: Performer { inner: self, },
                            }),

                    }
                },

            // Source::Pid(Some(proto::Request::DeleteBlock(request_delete_block))) => {
            //     lru_cache.invalidate(&request_delete_block.block_id);
            //     match schema.process_delete_block_request(&request_delete_block.block_id) {

            //         schema::DeleteBlockOp::Perform(schema::DeleteBlockPerform { block_offset, }) => {
            //             tasks_queue.push(
            //                 bg_task.current_offset,
            //                 block_offset,
            //                 task::TaskKind::MarkTombstone(task::MarkTombstone {
            //                     block_id: request_delete_block.block_id,
            //                     context: task::MarkTombstoneContext::External(
            //                         task::MarkTombstoneContextExternal {
            //                             reply_tx: request_delete_block.reply_tx,
            //                         },
            //                     ),
            //                 }),
            //             );
            //         },

            //         schema::DeleteBlockOp::NotFound => {
            //             if let Err(_send_error) = request_delete_block.reply_tx.send(Err(proto::RequestDeleteBlockError::NotFound)) {
            //                 log::warn!("reply channel has been closed during DeleteBlock result send");
            //             }
            //         },

            //     }
            // },

            // Source::InterpreterDone(Ok(task::Done { current_offset, task: task::TaskDone::WriteBlock(write_block), })) => {
            //     bg_task = BackgroundTask { current_offset, state: BackgroundTaskState::Idle, };
            //     match write_block.context {
            //         task::WriteBlockContext::External(context) =>
            //             if let Err(_send_error) = context.reply_tx.send(Ok(write_block.block_id)) {
            //                 log::warn!("client channel was closed before a block is actually written");
            //             },
            //     }
            // },

            // Source::InterpreterDone(Ok(task::Done { current_offset, task: task::TaskDone::ReadBlock(read_block), })) => {
            //     let block_bytes = read_block.block_bytes.freeze();
            //     lru_cache.insert(read_block.block_id.clone(), block_bytes.clone());
            //     bg_task = BackgroundTask { current_offset, state: BackgroundTaskState::Idle, };
            //     match read_block.context {
            //         task::ReadBlockContext::External(context) =>
            //             if let Err(_send_error) = context.reply_tx.send(Ok(block_bytes)) {
            //                 log::warn!("client channel was closed before a block is actually read");
            //             },
            //     }
            // },

            // Source::InterpreterDone(Ok(task::Done { current_offset, task: task::TaskDone::MarkTombstone(mark_tombstone), })) => {
            //     match schema.process_tombstone_written(mark_tombstone.block_id) {
            //         schema::TombstoneWrittenOp::Perform(schema::TombstoneWrittenPerform { defrag_op, }) => {
            //             match defrag_op {
            //                 schema::DefragOp::None =>
            //                     (),
            //                 schema::DefragOp::Queue { free_space_offset, space_key, } =>
            //                     defrag_task_queue.push(free_space_offset, space_key),
            //             }
            //         },
            //     }
            //     bg_task = BackgroundTask { current_offset, state: BackgroundTaskState::Idle, };
            //     match mark_tombstone.context {
            //         task::MarkTombstoneContext::External(context) =>
            //             if let Err(_send_error) = context.reply_tx.send(Ok(proto::Deleted)) {
            //                 log::warn!("client channel was closed before a block is actually deleted");
            //             },
            //     }
            // },

            // Source::InterpreterDone(Err(oneshot::Canceled)) => {
            //     log::debug!("interpreter reply channel closed: shutting down");
            //     return Ok(());
            // },

            // Source::InterpreterError(Ok(ErrorSeverity::Recoverable { state: (), })) =>
            //     return Err(ErrorSeverity::Recoverable { state, }),

            // Source::InterpreterError(Ok(ErrorSeverity::Fatal(error))) =>
            //     return Err(ErrorSeverity::Fatal(error)),

            // Source::InterpreterError(Err(oneshot::Canceled)) => {
            //     log::debug!("interpreter error channel closed: shutting down");
            //     return Ok(());
            // },

            RequestOrInterpreterIncoming::Request(..) =>
                unimplemented!(),
            RequestOrInterpreterIncoming::Interpreter(..) =>
                unimplemented!(),
        }
    }
}
