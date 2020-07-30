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
    tasks_queue: task::queue::Queue<C>,
    defrag_queues: Option<defrag::Queues<C::WriteBlock>>,
    defrag_state: DefragState,
    bg_task: BackgroundTask<C::Interpreter>,
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
        self.inner.next_defrag();
        self.inner.next_bg_task()
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
    pub task: task::Task<C>,
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
    TaskDone(TaskDone<C>),
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
pub enum TaskDone<C> where C: Context {
    WriteBlockDone(WriteBlockDoneOp<C>),
    ReadBlockDone(ReadBlockDoneOp<C>),
    DeleteBlockDone(DeleteBlockDoneOp<C>),
}

#[derive(Debug)]
pub struct WriteBlockDoneOp<C> where C: Context {
    pub block_id: block::Id,
    pub context: C::WriteBlock,
    pub performer: Performer<C>,
}

#[derive(Debug)]
pub struct ReadBlockDoneOp<C> where C: Context {
    pub block_bytes: block::Bytes,
    pub context: C::ReadBlock,
    pub next: TaskReadBlockDoneNext<C>,
}

#[derive(Debug)]
pub struct DeleteBlockDoneOp<C> where C: Context {
    pub context: C::DeleteBlock,
    pub next: TaskDeleteBlockDoneNext<C>,
}

#[derive(Debug)]
pub struct TaskReadBlockDoneNext<C> where C: Context {
    block_id: block::Id,
    block_bytes: block::Bytes,
    inner: Inner<C>,
}

#[derive(Debug)]
pub enum TaskReadBlockDoneLoop<C> where C: Context {
    Done(Performer<C>),
    More(ReadBlockDoneOp<C>),
}

impl<C> TaskReadBlockDoneNext<C> where C: Context {
    pub fn step(mut self) -> TaskReadBlockDoneLoop<C> {
        match self.inner.schema.process_read_block_task_done(&self.block_id) {
            schema::ReadBlockTaskDoneOp::Perform(schema::ReadBlockTaskDonePerform { block_offset, tasks_head, }) => {
                if let Some(read_block) = self.inner.tasks_queue.pop_read_task(tasks_head) {
                    self.inner.blocks_pool.repay(read_block.block_bytes.freeze());
                    match read_block.context {
                        task::ReadBlockContext::External(context) =>
                            return TaskReadBlockDoneLoop::More(ReadBlockDoneOp {
                                block_bytes: self.block_bytes.clone(),
                                context,
                                next: self,
                            }),
                    }
                }

                if let Some(task_kind) = self.inner.tasks_queue.pop_task(tasks_head) {
                    self.inner.tasks_queue.push(
                        self.inner.bg_task.current_offset,
                        block_offset,
                        task::Task {
                            block_id: self.block_id.clone(),
                            kind: task_kind,
                        },
                        tasks_head,
                    );
                }

                TaskReadBlockDoneLoop::Done(Performer { inner: self.inner, })
            }
        }
    }
}

#[derive(Debug)]
pub struct TaskDeleteBlockDoneNext<C> where C: Context {
    block_id: block::Id,
    inner: Inner<C>,
}

#[derive(Debug)]
pub enum TaskDeleteBlockDoneLoop<C> where C: Context {
    Done(Performer<C>),
    More(DeleteBlockDoneOp<C>),
}

impl<C> TaskDeleteBlockDoneNext<C> where C: Context {
    pub fn step(mut self) -> TaskDeleteBlockDoneLoop<C> {
        if let Some(tasks_head) = self.inner.schema.block_tasks_head(&self.block_id) {
            if let Some(delete_block) = self.inner.tasks_queue.pop_delete_task(tasks_head) {
                match delete_block.context {
                    task::DeleteBlockContext::External(context) =>
                        return TaskDeleteBlockDoneLoop::More(DeleteBlockDoneOp {
                            context,
                            next: self,
                        }),
                }
            }
        }

        TaskDeleteBlockDoneLoop::Done(Performer { inner: self.inner, })
    }
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

enum DefragState {
    Idle,
    InProgress { task: DefragTask, },
}

struct DefragTask {
    block_id: block::Id,
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
            tasks_queue: task::queue::Queue::new(),
            defrag_queues,
            defrag_state: DefragState::Idle,
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

                    schema::WriteBlockOp::Perform(schema::WriteBlockPerform { defrag_op, task_op, tasks_head, }) => {
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
                            task::Task {
                                block_id: task_op.block_id,
                                kind: task::TaskKind::WriteBlock(
                                    task::WriteBlock {
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
                            },
                            tasks_head,
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
                match self.schema.process_read_block_request(&request_read_block.block_id) {

                    schema::ReadBlockOp::Perform(schema::ReadBlockPerform { block_offset, block_header, tasks_head, }) =>
                        if let Some(block_bytes) = self.lru_cache.get(&request_read_block.block_id) {
                            PerformOp::ReadBlock(ReadBlockOp::CacheHit {
                                block_bytes: block_bytes.clone(),
                                context: request_read_block.context,
                                performer: Performer { inner: self, },
                            })
                        } else {
                            let block_bytes = self.blocks_pool.lend();
                            self.tasks_queue.push(
                                self.bg_task.current_offset,
                                block_offset,
                                task::Task {
                                    block_id: request_read_block.block_id.clone(),
                                    kind: task::TaskKind::ReadBlock(task::ReadBlock {
                                        block_header: block_header.clone(),
                                        block_bytes,
                                        context: task::ReadBlockContext::External(
                                            request_read_block.context,
                                        ),
                                    }),
                                },
                                tasks_head,
                            );
                            PerformOp::Idle(Performer { inner: self, })
                        },

                    schema::ReadBlockOp::NotFound =>
                        PerformOp::ReadBlock(ReadBlockOp::NotFound {
                            context: request_read_block.context,
                            performer: Performer { inner: self, },
                        }),

                },

            RequestOrInterpreterIncoming::Request(proto::Request::DeleteBlock(request_delete_block)) =>
                match self.schema.process_delete_block_request(&request_delete_block.block_id) {

                    schema::DeleteBlockOp::Perform(schema::DeleteBlockPerform { block_offset, tasks_head, }) => {
                        self.lru_cache.invalidate(&request_delete_block.block_id);
                        self.tasks_queue.push(
                            self.bg_task.current_offset,
                            block_offset,
                            task::Task {
                                block_id: request_delete_block.block_id,
                                kind: task::TaskKind::DeleteBlock(task::DeleteBlock {
                                    context: task::DeleteBlockContext::External(
                                        request_delete_block.context,
                                    ),
                                }),
                            },
                            tasks_head,
                        );
                        PerformOp::Idle(Performer { inner: self, })
                    },

                    schema::DeleteBlockOp::NotFound =>
                        PerformOp::DeleteBlock(DeleteBlockOp::NotFound {
                            context: request_delete_block.context,
                            performer: Performer { inner: self, },
                        }),

                },

            RequestOrInterpreterIncoming::Interpreter(
                task::Done { current_offset, task: task::TaskDone { block_id, kind: task::TaskDoneKind::WriteBlock(write_block), }, },
            ) => {
                self.bg_task = BackgroundTask { current_offset, state: BackgroundTaskState::Idle, };
                match self.schema.process_write_block_task_done(&block_id) {
                    schema::WriteBlockTaskDoneOp::Perform(schema::WriteBlockTaskDonePerform { block_offset, tasks_head, }) =>
                        if let Some(task_kind) = self.tasks_queue.pop_task(tasks_head) {
                            self.tasks_queue.push(
                                self.bg_task.current_offset,
                                block_offset,
                                task::Task {
                                    block_id: block_id.clone(),
                                    kind: task_kind,
                                },
                                tasks_head,
                            );
                        },
                }
                match write_block.context {
                    task::WriteBlockContext::External(context) =>
                        PerformOp::TaskDone(TaskDone::WriteBlockDone(WriteBlockDoneOp {
                            block_id,
                            context,
                            performer: Performer { inner: self, },
                        })),
                }
            },

            RequestOrInterpreterIncoming::Interpreter(
                task::Done { current_offset, task: task::TaskDone { block_id, kind: task::TaskDoneKind::ReadBlock(read_block), }, },
            ) => {
                self.bg_task = BackgroundTask { current_offset, state: BackgroundTaskState::Idle, };
                let block_bytes = read_block.block_bytes.freeze();
                self.lru_cache.insert(block_id.clone(), block_bytes.clone());
                match read_block.context {
                    task::ReadBlockContext::External(context) =>
                        PerformOp::TaskDone(TaskDone::ReadBlockDone(ReadBlockDoneOp {
                            block_bytes: block_bytes.clone(),
                            context,
                            next: TaskReadBlockDoneNext {
                                block_id,
                                block_bytes,
                                inner: self,
                            },
                        })),
                }
            },

            RequestOrInterpreterIncoming::Interpreter(
                task::Done { current_offset, task: task::TaskDone { block_id, kind: task::TaskDoneKind::DeleteBlock(delete_block), }, },
            ) => {
                self.bg_task = BackgroundTask { current_offset, state: BackgroundTaskState::Idle, };
                match self.schema.process_delete_block_task_done(block_id.clone()) {
                    schema::DeleteBlockTaskDoneOp::Perform(schema::DeleteBlockTaskDonePerform { defrag_op, }) => {
                        match (defrag_op, self.defrag_queues.as_mut()) {
                            (schema::DefragOp::Queue { free_space_offset, space_key, }, Some(defrag::Queues { tasks, .. })) =>
                                tasks.push(free_space_offset, space_key),
                            (schema::DefragOp::None, _) | (_, None) =>
                                (),
                        }
                    },
                }
                match delete_block.context {
                    task::DeleteBlockContext::External(context) =>
                        PerformOp::TaskDone(TaskDone::DeleteBlockDone(DeleteBlockDoneOp {
                            context,
                            next: TaskDeleteBlockDoneNext {
                                block_id,
                                inner: self,
                            },
                        })),
                }
            },

        }
    }

    fn next_defrag(&mut self) {
        // if let Some(defrag::Queues { tasks, .. }) = self.defrag_queues.as_mut() {
        //     match mem::replace(&mut self.defrag_state, DefragState::Idle) {
        //         DefragState::Idle =>
        //             while let Some((offset, space_key)) = tasks.pop() {

        //                 unimplemented!()
        //             },
        //         DefragState::InProgress { task, } =>
        //             unimplemented!(),
        //     }
        // }
    }

    fn next_bg_task(mut self) -> Op<C> {
        match mem::replace(&mut self.bg_task.state, BackgroundTaskState::Idle) {
            BackgroundTaskState::Idle =>
                if let Some((offset, block_id)) = self.tasks_queue.pop_block_id(self.bg_task.current_offset) {
                    let tasks_head = self.schema.block_tasks_head(&block_id).unwrap();
                    let task_kind = self.tasks_queue.pop_task(tasks_head).unwrap();
                    Op::InterpretTask(InterpretTask {
                        offset,
                        task: task::Task { block_id, kind: task_kind, },
                        next: InterpretTaskNext {
                            inner: self,
                        },
                    })
                } else {
                    Op::PollRequest(PollRequest {
                        next: PollRequestNext {
                            inner: self,
                        },
                    })
                },
            BackgroundTaskState::InProgress { interpreter_context, } =>
                Op::PollRequestAndInterpreter(PollRequestAndInterpreter {
                    interpreter_context,
                    next: PollRequestAndInterpreterNext {
                        inner: self,
                    },
                }),
        }
    }
}
