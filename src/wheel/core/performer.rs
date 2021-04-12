use std::mem;

use alloc_pool::bytes::{
    Bytes,
    BytesMut,
};

use crate::{
    Info,
    InterpretStats,
    proto,
    storage,
    context::Context,
    wheel::{
        lru,
        core::{
            task,
            block,
            schema,
            defrag,
            SpaceKey,
            BlockGet,
            BlockEntry,
            BlockEntryGet,
        },
    },
};

#[cfg(test)]
mod tests;

struct Inner<C> where C: Context {
    schema: schema::Schema,
    lru_cache: lru::Cache,
    defrag: Option<Defrag<C::WriteBlock>>,
    freed_space_key: Option<SpaceKey>,
    bg_task: BackgroundTask<C::Interpreter>,
    tasks_queue: task::queue::Queue<C>,
    done_task: DoneTask<C>,
    interpret_stats: InterpretStats,
}

struct Defrag<C> {
    queues: defrag::Queues<C>,
    in_progress_tasks_count: usize,
    in_progress_tasks_limit: usize,
}

enum DoneTask<C> where C: Context {
    None,
    ReadBlockRaw {
        block_header: storage::BlockHeader,
        block_bytes: Bytes,
        maybe_context: Option<task::ReadBlockContext<C>>,
        pending_contexts: task::queue::PendingReadContextBag,
    },
    ReadBlockProcessed {
        block_id: block::Id,
        maybe_block_bytes: Option<Bytes>,
        pending_contexts: task::queue::PendingReadContextBag,
    },
    DeleteBlockRegular {
        block_id: block::Id,
        block_entry: BlockEntry,
        freed_space_key: SpaceKey,
    },
}

pub struct Performer<C> where C: Context {
    inner: Inner<C>,
}

pub enum Op<C> where C: Context {
    Idle(Performer<C>),
    Query(QueryOp<C>),
    Event(Event<C>),
}

pub enum QueryOp<C> where C: Context {
    PollRequestAndInterpreter(PollRequestAndInterpreter<C>),
    PollRequest(PollRequest<C>),
    InterpretTask(InterpretTask<C>),
    MakeIterBlocksStream(MakeIterBlocksStream<C>),
}

pub struct MakeIterBlocksStream<C> where C: Context {
    pub blocks_total_count: usize,
    pub blocks_total_size: usize,
    pub iter_blocks_context: C::IterBlocks,
    pub next: MakeIterBlocksStreamNext<C>,
}

pub struct PollRequestAndInterpreter<C> where C: Context {
    pub interpreter_context: C::Interpreter,
    pub next: PollRequestAndInterpreterNext<C>,
}

pub struct PollRequest<C> where C: Context {
    pub next: PollRequestNext<C>,
}

pub struct Event<C> where C: Context {
    pub op: EventOp<C>,
    pub performer: Performer<C>,
}

pub enum EventOp<C> where C: Context {
    Info(TaskDoneOp<C::Info, InfoOp>),
    Flush(TaskDoneOp<C::Flush, FlushOp>),
    WriteBlock(TaskDoneOp<C::WriteBlock, WriteBlockOp>),
    ReadBlock(TaskDoneOp<C::ReadBlock, ReadBlockOp>),
    DeleteBlock(TaskDoneOp<C::DeleteBlock, DeleteBlockOp>),
    IterBlocksItem(IterBlocksItemOp<C::IterBlocksStream>),
    IterBlocksFinish(IterBlocksFinishOp<C::IterBlocksStream>),
    PrepareInterpretTask(PrepareInterpretTaskOp<C>),
    ProcessReadBlockTaskDone(ProcessReadBlockTaskDoneOp),
}

pub struct TaskDoneOp<C, O> {
    pub context: C,
    pub op: O,
}

pub enum InfoOp {
    Success { info: Info, },
}

pub enum FlushOp {
    Flushed,
}

pub enum WriteBlockOp {
    NoSpaceLeft,
    Done { block_id: block::Id, },
}

pub enum ReadBlockOp {
    NotFound,
    Done { block_bytes: Bytes, },
}

pub enum DeleteBlockOp {
    NotFound,
    Done { block_id: block::Id, },
}

pub struct IterBlocksItemOp<C> {
    pub block_id: block::Id,
    pub block_bytes: Bytes,
    pub iter_blocks_state: IterBlocksState<C>,
}

pub struct PrepareInterpretTaskOp<C> where C: Context {
    pub block_id: block::Id,
    pub task: PrepareInterpretTaskKind<C>,
}

pub enum PrepareInterpretTaskKind<C> where C: Context {
    WriteBlock(PrepareInterpretTaskWriteBlock<C::WriteBlock>),
    DeleteBlock(PrepareInterpretTaskDeleteBlock<C::DeleteBlock>),
}

pub struct PrepareInterpretTaskWriteBlock<C> {
    pub block_bytes: Bytes,
    pub context: task::WriteBlockContext<C>,
}

pub struct PrepareInterpretTaskDeleteBlock<C> {
    pub context: task::DeleteBlockContext<C>,
}

pub struct ProcessReadBlockTaskDoneOp {
    pub storage_layout: storage::Layout,
    pub block_header: storage::BlockHeader,
    pub block_bytes: Bytes,
    pub pending_contexts: task::queue::PendingReadContextBag,
}

#[derive(Debug)]
pub struct IterBlocksState<C> {
    pub iter_blocks_stream_context: C,
    pub iter_blocks_cursor: IterBlocksCursor,
}

#[derive(Debug)]
pub struct IterBlocksCursor {
    block_id: block::Id,
}

pub struct IterBlocksFinishOp<C> {
    pub iter_blocks_stream_context: C,
}

pub struct InterpretTask<C> where C: Context {
    pub offset: u64,
    pub task: task::Task<C>,
    pub next: InterpretTaskNext<C>,
}

pub struct InterpretTaskNext<C> where C: Context {
    inner: Inner<C>,
}

pub struct PollRequestAndInterpreterNext<C> where C: Context {
    inner: Inner<C>,
}

pub struct PollRequestNext<C> where C: Context {
    inner: Inner<C>,
}

pub struct MakeIterBlocksStreamNext<C> where C: Context {
    inner: Inner<C>,
}

pub struct DefragConfig<C> {
    queues: defrag::Queues<C>,
    in_progress_tasks_limit: usize,
}

impl<C> DefragConfig<C> {
    pub fn new(in_progress_tasks_limit: usize) -> DefragConfig<C> {
        DefragConfig {
            queues: defrag::Queues::new(),
            in_progress_tasks_limit,
        }
    }
}

#[derive(Debug)]
pub enum BuilderError {
    StorageLayoutCalculate(storage::LayoutError),
}

pub struct PerformerBuilderInit<C> where C: Context {
    lru_cache: lru::Cache,
    defrag: Option<Defrag<C::WriteBlock>>,
    storage_layout: storage::Layout,
    work_block: Vec<u8>,
}

impl<C> PerformerBuilderInit<C> where C: Context {
    pub fn new(
        lru_cache: lru::Cache,
        defrag_queues: Option<DefragConfig<C::WriteBlock>>,
        work_block_size_bytes: usize,
    )
        -> Result<PerformerBuilderInit<C>, BuilderError>
    {
        let mut work_block = Vec::with_capacity(work_block_size_bytes);
        let storage_layout = storage::Layout::calculate(&mut work_block)
            .map_err(BuilderError::StorageLayoutCalculate)?;

        Ok(PerformerBuilderInit {
            lru_cache,
            defrag: defrag_queues
                .map(|config| Defrag {
                    queues: config.queues,
                    in_progress_tasks_count: 0,
                    in_progress_tasks_limit: config.in_progress_tasks_limit,
                }),
            storage_layout,
            work_block,
        })
    }

    pub fn storage_layout(&self) -> &storage::Layout {
        &self.storage_layout
    }

    pub fn work_block_cleared(&mut self) -> &mut Vec<u8> {
        self.work_block.clear();
        self.work_block()
    }

    pub fn work_block(&mut self) -> &mut Vec<u8> {
        &mut self.work_block
    }

    pub fn start_fill(self) -> (PerformerBuilder<C>, Vec<u8>) {
        let schema_builder = schema::Builder::new(self.storage_layout);
        (
            PerformerBuilder {
                schema_builder,
                lru_cache: self.lru_cache,
                defrag: self.defrag,
            },
            self.work_block,
        )
    }
}

pub struct PerformerBuilder<C> where C: Context {
    schema_builder: schema::Builder,
    lru_cache: lru::Cache,
    defrag: Option<Defrag<C::WriteBlock>>,
}

impl<C> PerformerBuilder<C> where C: Context {
    pub fn push_block(&mut self, offset: u64, block_header: storage::BlockHeader) {
        let defrag_op = self.schema_builder.push_block(offset, block_header);
        if let Some(Defrag { queues: defrag::Queues { tasks, .. }, .. }) = self.defrag.as_mut() {
            match defrag_op {
                schema::DefragOp::Queue { defrag_gaps, moving_block_id, } =>
                    tasks.push(defrag_gaps, moving_block_id),
                schema::DefragOp::None =>
                    (),
            }
        }
    }

    pub fn storage_layout(&self) -> &storage::Layout {
        self.schema_builder.storage_layout()
    }

    pub fn finish(mut self, size_bytes_total: usize) -> Performer<C> {
        let (defrag_op, schema) = self.schema_builder.finish(size_bytes_total);
        if let Some(Defrag { queues: defrag::Queues { tasks, .. }, .. }) = self.defrag.as_mut() {
            match defrag_op {
                schema::DefragOp::Queue { defrag_gaps, moving_block_id, } =>
                    tasks.push(defrag_gaps, moving_block_id),
                schema::DefragOp::None =>
                    (),
            }
        }

        Performer {
            inner: Inner::new(
                schema,
                self.lru_cache,
                self.defrag,
            ),
        }
    }
}

impl<C> Performer<C> where C: Context {
    pub fn next(self) -> Op<C> {
        self.inner.incoming_poke()
    }

    #[cfg(test)]
    pub fn decompose(self) -> schema::Schema {
        self.inner.schema
    }
}

impl<C> PollRequestAndInterpreterNext<C> where C: Context {
    pub fn incoming_request(mut self, request: proto::Request<C>, interpreter_context: C::Interpreter) -> Op<C> {
        self.inner.rollback_bg_task_state(interpreter_context);
        self.inner.incoming_request(request)
    }

    #[cfg(test)]
    pub fn incoming_task_done(self, task_done: task::Done<C>) -> Op<C> {
        self.inner.incoming_interpreter(task_done)
    }

    pub fn incoming_task_done_stats(mut self, task_done: task::Done<C>, stats: InterpretStats) -> Op<C> {
        self.inner.interpret_stats = stats;
        self.inner.incoming_interpreter(task_done)
    }

    pub fn prepared_write_block_done(
        mut self,
        block_id: block::Id,
        write_block_bytes: BytesMut,
        context: task::WriteBlockContext<C::WriteBlock>,
        interpreter_context: C::Interpreter,
    )
        -> Op<C>
    {
        self.inner.rollback_bg_task_state(interpreter_context);
        self.inner.prepared_write_block_done(block_id, write_block_bytes, context)
    }

    pub fn prepared_delete_block_done(
        mut self,
        block_id: block::Id,
        delete_block_bytes: BytesMut,
        context: task::DeleteBlockContext<C::DeleteBlock>,
        interpreter_context: C::Interpreter,
    )
        -> Op<C>
    {
        self.inner.rollback_bg_task_state(interpreter_context);
        self.inner.prepared_delete_block_done(block_id, delete_block_bytes, context)
    }

    pub fn process_read_block_done(
        mut self,
        block_id: block::Id,
        block_bytes: Bytes,
        pending_contexts: task::queue::PendingReadContextBag,
        interpreter_context: C::Interpreter,
    )
        -> Op<C>
    {
        self.inner.rollback_bg_task_state(interpreter_context);
        self.inner.process_read_block_done(block_id, block_bytes, pending_contexts)
    }

    pub fn incoming_iter_blocks(
        mut self,
        iter_blocks_state: IterBlocksState<C::IterBlocksStream>,
        interpreter_context: C::Interpreter,
    )
        -> Op<C>
    {
        self.inner.rollback_bg_task_state(interpreter_context);
        self.inner.iter_blocks_stream_next(
            iter_blocks_state.iter_blocks_cursor.block_id,
            iter_blocks_state.iter_blocks_stream_context,
        )
    }
}

impl<C> PollRequestNext<C> where C: Context {
    pub fn incoming_request(self, request: proto::Request<C>) -> Op<C> {
        self.inner.incoming_request(request)
    }

    pub fn incoming_iter_blocks(self, iter_blocks_state: IterBlocksState<C::IterBlocksStream>) -> Op<C> {
        self.inner.iter_blocks_stream_next(
            iter_blocks_state.iter_blocks_cursor.block_id,
            iter_blocks_state.iter_blocks_stream_context,
        )
    }

    pub fn prepared_write_block_done(
        self,
        block_id: block::Id,
        write_block_bytes: BytesMut,
        context: task::WriteBlockContext<C::WriteBlock>,
    )
        -> Op<C>
    {
        self.inner.prepared_write_block_done(block_id, write_block_bytes, context)
    }

    pub fn prepared_delete_block_done(
        self,
        block_id: block::Id,
        delete_block_bytes: BytesMut,
        context: task::DeleteBlockContext<C::DeleteBlock>,
    )
        -> Op<C>
    {
        self.inner.prepared_delete_block_done(block_id, delete_block_bytes, context)
    }

    pub fn process_read_block_done(
        self,
        block_id: block::Id,
        block_bytes: Bytes,
        pending_contexts: task::queue::PendingReadContextBag,
    )
        -> Op<C>
    {
        self.inner.process_read_block_done(block_id, block_bytes, pending_contexts)
    }
}

impl<C> InterpretTaskNext<C> where C: Context {
    pub fn task_accepted(mut self, interpreter_context: C::Interpreter) -> Performer<C> {
        self.inner.bg_task.state = match self.inner.bg_task.state {
            BackgroundTaskState::Await { block_id, } =>
                BackgroundTaskState::InProgress { block_id, interpreter_context, },
            BackgroundTaskState::Idle | BackgroundTaskState::InProgress { .. } =>
                unreachable!(),
        };
        Performer { inner: self.inner, }
    }
}

impl<C> MakeIterBlocksStreamNext<C> where C: Context {
    pub fn stream_ready(self, iter_blocks_stream_context: C::IterBlocksStream) -> Op<C> {
        self.inner.iter_blocks_stream_ready(iter_blocks_stream_context)
    }
}


struct BackgroundTask<C> {
    current_offset: u64,
    state: BackgroundTaskState<C>,
}

enum BackgroundTaskState<C> {
    Idle,
    InProgress {
        block_id: block::Id,
        interpreter_context: C,
    },
    Await {
        block_id: block::Id,
    }
}

impl<C> Inner<C> where C: Context {
    fn new(
        schema: schema::Schema,
        lru_cache: lru::Cache,
        defrag: Option<Defrag<C::WriteBlock>>,
    )
        -> Inner<C>
    {
        Inner {
            schema,
            lru_cache,
            tasks_queue: task::queue::Queue::new(),
            defrag,
            freed_space_key: None,
            bg_task: BackgroundTask {
                current_offset: 0,
                state: BackgroundTaskState::Idle,
            },
            done_task: DoneTask::None,
            interpret_stats: InterpretStats::default(),
        }
    }

    fn rollback_bg_task_state(&mut self, interpreter_context: C::Interpreter) {
        self.bg_task.state = match mem::replace(&mut self.bg_task.state, BackgroundTaskState::Idle) {
            BackgroundTaskState::Await { block_id, } =>
                BackgroundTaskState::InProgress { block_id, interpreter_context, },
            BackgroundTaskState::Idle | BackgroundTaskState::InProgress { .. } =>
                unreachable!(),
        };
    }

    fn incoming_poke(mut self) -> Op<C> {
        match mem::replace(&mut self.done_task, DoneTask::None) {

            DoneTask::None =>
                (),

            DoneTask::ReadBlockRaw { block_header, block_bytes, maybe_context: None, pending_contexts, } => {
                self.tasks_queue
                    .focus_block_id(block_header.block_id.clone())
                    .enqueue(self.schema.block_get());
                if !pending_contexts.is_empty() {
                    return Op::Event(Event {
                        op: EventOp::ProcessReadBlockTaskDone(ProcessReadBlockTaskDoneOp {
                            storage_layout: self.schema.storage_layout().clone(),
                            block_header,
                            block_bytes,
                            pending_contexts,
                        }),
                        performer: Performer { inner: self, },
                    });
                }
            }

            DoneTask::ReadBlockRaw { block_header, block_bytes, maybe_context: Some(context), mut pending_contexts, } => {
                let maybe_op = match self.schema.process_read_block_task_done(&block_header.block_id) {
                    schema::ReadBlockTaskDoneOp::NotFound =>
                        unreachable!(),
                    schema::ReadBlockTaskDoneOp::Perform(schema::ReadBlockTaskDonePerform) =>
                        match context {

                            context @ task::ReadBlockContext::Process(..) => {
                                self.tasks_queue.push_pending_read_context(
                                    task::ReadBlock {
                                        block_header: block_header.clone(),
                                        context,
                                    },
                                    &mut pending_contexts,
                                );
                                None
                            },

                            task::ReadBlockContext::Defrag(task::ReadBlockDefragContext { defrag_gaps, }) =>
                                if defrag_gaps.is_still_relevant(&block_header.block_id, self.schema.block_get()) {
                                    Some(EventOp::PrepareInterpretTask(PrepareInterpretTaskOp {
                                        block_id: block_header.block_id.clone(),
                                        task: PrepareInterpretTaskKind::DeleteBlock(PrepareInterpretTaskDeleteBlock {
                                            context: task::DeleteBlockContext::Defrag {
                                                defrag_gaps,
                                                block_bytes: block_bytes.clone(),
                                            },
                                        }),
                                    }))
                                } else {
                                    // cancel defrag read task
                                    cancel_defrag_task(self.defrag.as_mut().unwrap());
                                    None
                                },

                        },
                };

                let mut lens = self.tasks_queue.focus_block_id(block_header.block_id.clone());
                assert!(lens.pop_write_task(self.schema.block_get()).is_none());
                let maybe_context = lens.pop_read_task(self.schema.block_get())
                    .map(|read_block| read_block.context);
                self.done_task = DoneTask::ReadBlockRaw {
                    block_header,
                    block_bytes,
                    maybe_context,
                    pending_contexts,
                };

                return if let Some(op) = maybe_op {
                    Op::Event(Event { op, performer: Performer { inner: self, }, })
                } else {
                    Op::Idle(Performer { inner: self, })
                }
            },

            DoneTask::ReadBlockProcessed { block_id, maybe_block_bytes, mut pending_contexts, } => {
                if let Some(read_block) = self.tasks_queue.pop_pending_read_context(&mut pending_contexts) {
                    let maybe_op = match (maybe_block_bytes.clone(), read_block.context) {
                        (None, task::ReadBlockContext::Process(task::ReadBlockProcessContext::External(context))) =>
                            Some(EventOp::ReadBlock(TaskDoneOp { context, op: ReadBlockOp::NotFound, })),

                        (Some(block_bytes), task::ReadBlockContext::Process(task::ReadBlockProcessContext::External(context))) =>
                            Some(EventOp::ReadBlock(TaskDoneOp { context, op: ReadBlockOp::Done { block_bytes, }, }, )),

                        (
                            None,
                            task::ReadBlockContext::Process(
                                task::ReadBlockProcessContext::IterBlocks { iter_blocks_stream_context, next_block_id, },
                            ),
                        ) => {
                            // skip this block, proceed with the next one
                            self.iter_blocks_stream_next_op(next_block_id, iter_blocks_stream_context)
                        },

                        (
                            Some(block_bytes),
                            task::ReadBlockContext::Process(
                                task::ReadBlockProcessContext::IterBlocks { iter_blocks_stream_context, next_block_id, },
                            ),
                        ) =>
                            Some(EventOp::IterBlocksItem(IterBlocksItemOp {
                                block_id: block_id.clone(),
                                block_bytes,
                                iter_blocks_state: IterBlocksState {
                                    iter_blocks_stream_context,
                                    iter_blocks_cursor: IterBlocksCursor {
                                        block_id: next_block_id,
                                    },
                                },
                            })),

                        (_, task::ReadBlockContext::Defrag(..)) =>
                            unreachable!(),
                    };

                    self.done_task = DoneTask::ReadBlockProcessed {
                        block_id,
                        maybe_block_bytes,
                        pending_contexts,
                    };

                    return if let Some(op) = maybe_op {
                        Op::Event(Event { op, performer: Performer { inner: self, }, })
                    } else {
                        Op::Idle(Performer { inner: self, })
                    };
                }
            },

            DoneTask::DeleteBlockRegular { block_id, mut block_entry, freed_space_key, } => {
                let mut lens = self.tasks_queue.focus_block_id(block_id.clone());
                let mut block_get = BlockEntryGet::new(&mut block_entry);
                while let Some(write_block) = lens.pop_write_task(&mut block_get) {
                    match write_block.context {
                        task::WriteBlockContext::External(..) =>
                            unreachable!(),
                        task::WriteBlockContext::Defrag { .. } => {
                            // cancel defrag write task
                            cancel_defrag_task(self.defrag.as_mut().unwrap());
                        },
                    }
                }
                while let Some(read_block) = lens.pop_read_task(&mut block_get) {
                    match read_block.context {
                        task::ReadBlockContext::Process(task::ReadBlockProcessContext::External(context)) => {
                            self.done_task = DoneTask::DeleteBlockRegular {
                                block_id: block_id.clone(),
                                block_entry,
                                freed_space_key,
                            };
                            return Op::Event(Event {
                                op: EventOp::ReadBlock(TaskDoneOp {
                                    context,
                                    op: ReadBlockOp::NotFound,
                                }),
                                performer: Performer { inner: self, },
                            });
                        },
                        task::ReadBlockContext::Process(task::ReadBlockProcessContext::IterBlocks {
                            iter_blocks_stream_context,
                            next_block_id,
                        }) => {
                            // skip this block, proceed with the next one
                            return self.iter_blocks_stream_next(next_block_id, iter_blocks_stream_context);
                        },
                        task::ReadBlockContext::Defrag { .. } => {
                            // cancel defrag read task
                            cancel_defrag_task(self.defrag.as_mut().unwrap());
                        },
                    }
                }
                while let Some(delete_block) = lens.pop_delete_task(&mut block_get) {
                    match delete_block.context {
                        task::DeleteBlockContext::External(context) => {
                            self.done_task = DoneTask::DeleteBlockRegular {
                                block_id: block_id.clone(),
                                block_entry,
                                freed_space_key,
                            };
                            return Op::Event(Event {
                                op: EventOp::DeleteBlock(TaskDoneOp {
                                    context,
                                    op: DeleteBlockOp::NotFound,
                                }),
                                performer: Performer { inner: self, },
                            });
                        },
                        task::DeleteBlockContext::Defrag { .. } => {
                            // cancel defrag delete task
                            cancel_defrag_task(self.defrag.as_mut().unwrap());
                        },
                    }
                }
                self.freed_space_key = Some(freed_space_key);
            },
        }

        if let Some(space_key) = self.freed_space_key.take() {
            if let Some(defrag) = self.defrag.as_mut() {
                if let Some(request_write_block) = defrag.queues.pending.pop_at_most(space_key.space_available()) {

                    match self.schema.process_write_block_request(&request_write_block.block_bytes, Some(defrag.queues.pending.pending_bytes())) {
                        schema::WriteBlockOp::Perform(write_block_perform) => {
                            self.freed_space_key = write_block_perform.right_space_key;

                            match write_block_perform.defrag_op {
                                schema::DefragOp::Queue { defrag_gaps, moving_block_id, } =>
                                    defrag.queues.tasks.push(defrag_gaps, moving_block_id),
                                schema::DefragOp::None =>
                                    (),
                            }

                            return Op::Event(Event {
                                op: EventOp::PrepareInterpretTask(PrepareInterpretTaskOp {
                                    block_id: write_block_perform.task_op.block_id,
                                    task: PrepareInterpretTaskKind::WriteBlock(PrepareInterpretTaskWriteBlock {
                                        block_bytes: request_write_block.block_bytes,
                                        context: task::WriteBlockContext::External(
                                            request_write_block.context,
                                        ),
                                    }),
                                }),
                                performer: Performer { inner: self, },
                            });
                        },
                        schema::WriteBlockOp::QueuePendingDefrag { space_required, } => {
                            defrag.queues.pending.push(request_write_block, space_required);
                        },
                        schema::WriteBlockOp::ReplyNoSpaceLeft =>
                            unreachable!(),
                    }

                }
            }
        }

        if let Some(defrag) = self.defrag.as_mut() {
            loop {
                if defrag.in_progress_tasks_count >= defrag.in_progress_tasks_limit {
                    break;
                }
                if let Some((defrag_gaps, moving_block_id)) = defrag.queues.tasks.pop(self.schema.block_get()) {
                    let mut block_get = self.schema.block_get();
                    let block_entry = block_get.by_id(&moving_block_id).unwrap();
                    let mut lens = self.tasks_queue.focus_block_id(block_entry.header.block_id.clone());
                    lens.push_task(
                        task::Task {
                            block_id: block_entry.header.block_id.clone(),
                            kind: task::TaskKind::ReadBlock(task::ReadBlock {
                                block_header: block_entry.header.clone(),
                                context: task::ReadBlockContext::Defrag(task::ReadBlockDefragContext {
                                    defrag_gaps,
                                }),
                            }),
                        },
                        self.schema.block_get(),
                    );
                    lens.enqueue(self.schema.block_get());
                    defrag.in_progress_tasks_count += 1;
                } else {
                    break;
                }
            }
        }

        if self.tasks_queue.is_empty_tasks() && self.defrag.as_ref().map_or(true, |defrag| defrag.in_progress_tasks_count == 0) {
            if let Some(task::Flush { context, }) = self.tasks_queue.pop_flush() {
                return Op::Event(Event {
                    op: EventOp::Flush(TaskDoneOp { context, op: FlushOp::Flushed, }),
                    performer: Performer { inner: self, },
                });
            }
        }

        match mem::replace(&mut self.bg_task.state, BackgroundTaskState::Idle) {
            BackgroundTaskState::Idle =>
                self.maybe_run_background_task(),
            BackgroundTaskState::InProgress { block_id, interpreter_context, } => {
                self.bg_task.state = BackgroundTaskState::Await { block_id, };
                Op::Query(QueryOp::PollRequestAndInterpreter(PollRequestAndInterpreter {
                    interpreter_context,
                    next: PollRequestAndInterpreterNext {
                        inner: self,
                    },
                }))
            },
            BackgroundTaskState::Await { .. } =>
                unreachable!(),
        }
    }

    fn incoming_request(self, incoming: proto::Request<C>) -> Op<C> {
        match incoming {
            proto::Request::Info(request_info) =>
                self.incoming_request_info(request_info),
            proto::Request::Flush(request_flush) =>
                self.incoming_request_flush(request_flush),
            proto::Request::WriteBlock(request_write_block) =>
                self.incoming_request_write_block(request_write_block),
            proto::Request::ReadBlock(request_read_block) =>
                self.incoming_request_read_block(request_read_block),
            proto::Request::DeleteBlock(request_delete_block) =>
                self.incoming_request_delete_block(request_delete_block),
            proto::Request::IterBlocks(request_iter_blocks) =>
                self.incoming_request_iter_blocks(request_iter_blocks),
        }
    }

    fn incoming_request_info(self, proto::RequestInfo { context, }: proto::RequestInfo<C::Info>) -> Op<C> {
        let mut info = self.schema.info();
        info.interpret_stats = self.interpret_stats;
        if let Some(defrag) = self.defrag.as_ref() {
            info.defrag_write_pending_bytes = defrag.queues.pending.pending_bytes();
            assert!(
                info.bytes_free >= info.defrag_write_pending_bytes,
                "assertion failed: info.bytes_free = {} >= info.defrag_write_pending_bytes = {}",
                info.bytes_free,
                info.defrag_write_pending_bytes,
            );
            info.bytes_free -= info.defrag_write_pending_bytes;
        }

        Op::Event(Event {
            op: EventOp::Info(TaskDoneOp { context, op: InfoOp::Success { info, }, }),
            performer: Performer { inner: self, },
        })
    }

    fn incoming_request_flush(mut self, proto::RequestFlush { context, }: proto::RequestFlush<C::Flush>) -> Op<C> {
        self.tasks_queue.push_flush(task::Flush { context, });
        Op::Idle(Performer { inner: self, })
    }

    fn incoming_request_write_block(mut self, request_write_block: proto::RequestWriteBlock<C::WriteBlock>) -> Op<C> {
        let defrag_pending_bytes = self.defrag
            .as_ref()
            .map(|defrag| defrag.queues.pending.pending_bytes());
        match self.schema.process_write_block_request(&request_write_block.block_bytes, defrag_pending_bytes) {

            schema::WriteBlockOp::Perform(schema::WriteBlockPerform { defrag_op, task_op, .. }) => {
                if let Some(Defrag { queues: defrag::Queues { tasks, .. }, .. }) = self.defrag.as_mut() {
                    match defrag_op {
                        schema::DefragOp::Queue { defrag_gaps, moving_block_id, } =>
                            tasks.push(defrag_gaps, moving_block_id),
                        schema::DefragOp::None =>
                            (),
                    }
                }

                Op::Event(Event {
                    op: EventOp::PrepareInterpretTask(PrepareInterpretTaskOp {
                        block_id: task_op.block_id,
                        task: PrepareInterpretTaskKind::WriteBlock(PrepareInterpretTaskWriteBlock {
                            block_bytes: request_write_block.block_bytes,
                            context: task::WriteBlockContext::External(
                                request_write_block.context,
                            ),
                        }),
                    }),
                    performer: Performer { inner: self, },
                })
            },

            schema::WriteBlockOp::QueuePendingDefrag { space_required, } => {
                log::debug!(
                    "cannot directly allocate {} ({}) bytes in process_write_block_request: moving to pending defrag queue",
                    request_write_block.block_bytes.len(),
                    space_required,
                );
                if let Some(Defrag { queues: defrag::Queues { pending, .. }, .. }) = self.defrag.as_mut() {
                    pending.push(request_write_block, space_required);
                    Op::Idle(Performer { inner: self, })
                } else {
                    Op::Event(Event {
                        op: EventOp::WriteBlock(TaskDoneOp {
                            context: request_write_block.context,
                            op: WriteBlockOp::NoSpaceLeft,
                        }),
                        performer: Performer { inner: self, },
                    })
                }
            },

            schema::WriteBlockOp::ReplyNoSpaceLeft =>
               Op::Event(Event {
                    op: EventOp::WriteBlock(TaskDoneOp {
                        context: request_write_block.context,
                        op: WriteBlockOp::NoSpaceLeft,
                    }),
                    performer: Performer { inner: self, },
                }),

        }
    }

    fn incoming_request_read_block(mut self, request_read_block: proto::RequestReadBlock<C::ReadBlock>) -> Op<C> {
        match self.schema.process_read_block_request(&request_read_block.block_id) {

            schema::ReadBlockOp::Perform(schema::ReadBlockPerform { block_header, }) => {
                if let Some(block_bytes) = self.lru_cache.get(&request_read_block.block_id) {
                    Op::Event(Event {
                        op: EventOp::ReadBlock(TaskDoneOp {
                            context: request_read_block.context,
                            op: ReadBlockOp::Done {
                                block_bytes: block_bytes.clone(),
                            },
                        }),
                        performer: Performer { inner: self, },
                    })
                } else {
                    let mut lens = self.tasks_queue.focus_block_id(request_read_block.block_id.clone());
                    lens.push_task(
                        task::Task {
                            block_id: request_read_block.block_id,
                            kind: task::TaskKind::ReadBlock(task::ReadBlock {
                                block_header: block_header.clone(),
                                context: task::ReadBlockContext::Process(
                                    task::ReadBlockProcessContext::External(
                                        request_read_block.context,
                                    ),
                                ),
                            }),
                        },
                        self.schema.block_get(),
                    );
                    lens.enqueue(self.schema.block_get());

                    Op::Idle(Performer { inner: self, })
                }
            },

            schema::ReadBlockOp::NotFound =>
                Op::Event(Event {
                    op: EventOp::ReadBlock(TaskDoneOp {
                        context: request_read_block.context,
                        op: ReadBlockOp::NotFound,
                    }),
                    performer: Performer { inner: self, },
                }),

        }
    }

    fn incoming_request_delete_block(mut self, request_delete_block: proto::RequestDeleteBlock<C::DeleteBlock>) -> Op<C> {
        match self.schema.process_delete_block_request(&request_delete_block.block_id) {

            schema::DeleteBlockOp::Perform(schema::DeleteBlockPerform) =>
                Op::Event(Event {
                    op: EventOp::PrepareInterpretTask(PrepareInterpretTaskOp {
                        block_id: request_delete_block.block_id,
                        task: PrepareInterpretTaskKind::DeleteBlock(PrepareInterpretTaskDeleteBlock {
                            context: task::DeleteBlockContext::External(
                                request_delete_block.context,
                            ),
                        }),
                    }),
                    performer: Performer { inner: self, },
                }),

            schema::DeleteBlockOp::NotFound =>
                Op::Event(Event {
                    op: EventOp::DeleteBlock(TaskDoneOp {
                        context: request_delete_block.context,
                        op: DeleteBlockOp::NotFound,
                    }),
                    performer: Performer { inner: self, },
                }),

        }
    }

    fn incoming_request_iter_blocks(self, request_iter_blocks: proto::RequestIterBlocks<C::IterBlocks>) -> Op<C> {
        let info = self.schema.info();
        Op::Query(QueryOp::MakeIterBlocksStream(MakeIterBlocksStream {
            blocks_total_count: info.blocks_count,
            blocks_total_size: info.data_bytes_used,
            iter_blocks_context: request_iter_blocks.context,
            next: MakeIterBlocksStreamNext {
                inner: self,
            },
        }))
    }

    fn prepared_write_block_done(
        mut self,
        block_id: block::Id,
        write_block_bytes: BytesMut,
        context: task::WriteBlockContext<C::WriteBlock>,
    )
        -> Op<C>
    {
        let mut block_get = self.schema.block_get();
        let mut lens = self.tasks_queue.focus_block_id(block_id.clone());
        lens.push_task(
            task::Task {
                block_id,
                kind: task::TaskKind::WriteBlock(task::WriteBlock {
                    write_block_bytes: write_block_bytes.freeze(),
                    commit: task::Commit::None,
                    context,
                }),
            },
            &mut block_get,
        );
        lens.enqueue(block_get);
        Op::Idle(Performer { inner: self, })
    }

    fn prepared_delete_block_done(
        mut self,
        block_id: block::Id,
        delete_block_bytes: BytesMut,
        context: task::DeleteBlockContext<C::DeleteBlock>,
    )
        -> Op<C>
    {
        let mut block_get = self.schema.block_get();
        match (block_get.by_id(&block_id), context) {
            (None, task::DeleteBlockContext::Defrag { .. }) => {
                // block has been deleted already during defrag
                cancel_defrag_task(self.defrag.as_mut().unwrap());
                Op::Idle(Performer { inner: self, })
            },
            (None, task::DeleteBlockContext::External(context)) => {
                // block has been deleted already during request
                Op::Event(Event {
                    op: EventOp::DeleteBlock(TaskDoneOp {
                        context,
                        op: DeleteBlockOp::NotFound,
                    }),
                    performer: Performer { inner: self, },
                })
            },
            (Some(block_entry), context) => {
                if let task::DeleteBlockContext::Defrag { defrag_gaps, .. } = &context {
                    let mut block_entry_get = BlockEntryGet::new(block_entry);
                    if !defrag_gaps.is_still_relevant(&block_id, &mut block_entry_get) {
                        // block has been moved somewhere during defrag
                        cancel_defrag_task(self.defrag.as_mut().unwrap());
                        return Op::Idle(Performer { inner: self, })
                    }
                }

                let mut lens = self.tasks_queue.focus_block_id(block_id.clone());
                lens.push_task(
                    task::Task {
                        block_id,
                        kind: task::TaskKind::DeleteBlock(task::DeleteBlock {
                            delete_block_bytes: delete_block_bytes.freeze(),
                            commit: task::Commit::None,
                            context,
                        }),
                    },
                    block_get,
                );
                lens.enqueue(self.schema.block_get());
                Op::Idle(Performer { inner: self, })
            },
        }
    }

    fn process_read_block_done(
        mut self,
        block_id: block::Id,
        block_bytes: Bytes,
        pending_contexts: task::queue::PendingReadContextBag,
    )
        -> Op<C>
    {
        match self.schema.process_read_block_task_done(&block_id) {
            schema::ReadBlockTaskDoneOp::NotFound =>
                self.done_task = DoneTask::ReadBlockProcessed { block_id, maybe_block_bytes: None, pending_contexts, },
            schema::ReadBlockTaskDoneOp::Perform(schema::ReadBlockTaskDonePerform) => {
                self.lru_cache.insert(block_id.clone(), block_bytes.clone());
                self.done_task = DoneTask::ReadBlockProcessed {
                    block_id, maybe_block_bytes: Some(block_bytes),
                    pending_contexts,
                };
            },
        }
        Op::Idle(Performer { inner: self, })
    }

    fn incoming_interpreter(mut self, incoming: task::Done<C>) -> Op<C> {
        match incoming {

            task::Done { current_offset, task: task::TaskDone { block_id, kind: task::TaskDoneKind::WriteBlock(write_block), }, } => {
                self.bg_task = BackgroundTask { current_offset, state: BackgroundTaskState::Idle, };
                let mut lens = self.tasks_queue.focus_block_id(block_id.clone());
                lens.finish(self.schema.block_get());
                lens.enqueue(self.schema.block_get());
                match write_block.context {
                    task::WriteBlockContext::External(context) =>
                        Op::Event(Event {
                            op: EventOp::WriteBlock(TaskDoneOp {
                                context,
                                op: WriteBlockOp::Done { block_id, },
                            }),
                            performer: Performer { inner: self, },
                        }),
                    task::WriteBlockContext::Defrag => {
                        let defrag = self.defrag.as_mut().unwrap();
                        assert!(defrag.in_progress_tasks_count > 0);
                        defrag.in_progress_tasks_count -= 1;
                        Op::Idle(Performer { inner: self, })
                    },
                }
            },

            task::Done { current_offset, task: task::TaskDone { block_id, kind: task::TaskDoneKind::ReadBlock(read_block), }, } => {
                self.bg_task = BackgroundTask { current_offset, state: BackgroundTaskState::Idle, };
                let mut block_get = self.schema.block_get();
                let block_header = block_get.by_id(&block_id)
                    .unwrap()
                    .header
                    .clone();
                self.tasks_queue.focus_block_id(block_id.clone())
                    .finish(block_get);
                self.done_task = DoneTask::ReadBlockRaw {
                    block_header,
                    block_bytes: read_block.block_bytes.freeze(),
                    maybe_context: Some(read_block.context),
                    pending_contexts: task::queue::PendingReadContextBag::default(),
                };
                Op::Idle(Performer { inner: self, })
            },

            task::Done { current_offset, task: task::TaskDone { block_id, kind: task::TaskDoneKind::DeleteBlock(delete_block), }, } => {
                self.bg_task = BackgroundTask { current_offset, state: BackgroundTaskState::Idle, };
                self.tasks_queue.focus_block_id(block_id.clone())
                    .finish(self.schema.block_get());
                match delete_block.context {
                    task::DeleteBlockContext::External(context) => {
                        self.lru_cache.invalidate(&block_id);
                        match self.schema.process_delete_block_task_done(block_id.clone()) {
                            schema::DeleteBlockTaskDoneOp::Perform(schema::DeleteBlockTaskDonePerform {
                                defrag_op,
                                block_entry,
                                freed_space_key,
                            }) => {
                                if let Some(Defrag { queues: defrag::Queues { tasks, .. }, .. }) = self.defrag.as_mut() {
                                    match defrag_op {
                                        schema::DefragOp::Queue { defrag_gaps, moving_block_id, } =>
                                            tasks.push(defrag_gaps, moving_block_id),
                                        schema::DefragOp::None =>
                                            (),
                                    }
                                }
                                self.done_task = DoneTask::DeleteBlockRegular {
                                    block_id: block_id.clone(),
                                    block_entry,
                                    freed_space_key,
                                };
                                Op::Event(Event {
                                    op: EventOp::DeleteBlock(TaskDoneOp { context, op: DeleteBlockOp::Done { block_id, }, }),
                                    performer: Performer { inner: self, },
                                })
                            },
                        }
                    },
                    task::DeleteBlockContext::Defrag { block_bytes, .. } =>
                        match self.schema.process_delete_block_task_done_defrag(block_id.clone()) {
                            schema::DeleteBlockTaskDoneDefragOp::Perform(task_op) => {
                                if let Some(Defrag { queues: defrag::Queues { tasks, .. }, .. }) = self.defrag.as_mut() {
                                    match task_op.defrag_op {
                                        schema::DefragOp::Queue { defrag_gaps, moving_block_id, } =>
                                            tasks.push(defrag_gaps, moving_block_id),
                                        schema::DefragOp::None =>
                                            (),
                                    }
                                }
                                let mut lens = self.tasks_queue.focus_block_id(block_id.clone());
                                let mut block_get = self.schema.block_get();
                                lens.push_task(
                                    task::Task {
                                        block_id: block_id.clone(),
                                        kind: task::TaskKind::WriteBlock(task::WriteBlock {
                                            write_block_bytes: block_bytes.clone(),
                                            commit: task::Commit::None,
                                            context: task::WriteBlockContext::Defrag,
                                        }),
                                    },
                                    &mut block_get,
                                );
                                if let Some(read_block) = lens.pop_read_task(&mut block_get) {
                                    self.done_task = DoneTask::ReadBlockRaw {
                                        block_header: block_get.by_id(&block_id).unwrap().header.clone(),
                                        block_bytes,
                                        maybe_context: Some(read_block.context),
                                        pending_contexts: task::queue::PendingReadContextBag::default(),
                                    };
                                }
                                lens.enqueue(&mut block_get);
                                self.freed_space_key = task_op.freed_space_key;
                                Op::Idle(Performer { inner: self, })
                            },
                        },
                }
            },

        }
    }

    fn iter_blocks_stream_ready(self, iter_blocks_stream_context: C::IterBlocksStream) -> Op<C> {
        self.iter_blocks_stream_next(block::Id::init(), iter_blocks_stream_context)
    }

    fn iter_blocks_stream_next(mut self, block_id_from: block::Id, iter_blocks_stream_context: C::IterBlocksStream) -> Op<C> {
        if let Some(op) = self.iter_blocks_stream_next_op(block_id_from, iter_blocks_stream_context) {
            Op::Event(Event { op, performer: Performer { inner: self, }, })
        } else {
            Op::Idle(Performer { inner: self, })
        }
    }

    fn iter_blocks_stream_next_op(&mut self, block_id_from: block::Id, iter_blocks_stream_context: C::IterBlocksStream) -> Option<EventOp<C>> {
        match self.schema.next_block_id_from(block_id_from) {
            None =>
                Some(EventOp::IterBlocksFinish(IterBlocksFinishOp {
                    iter_blocks_stream_context,
                })),

            Some(block_id) =>
                match self.schema.process_read_block_request(&block_id) {

                    schema::ReadBlockOp::Perform(schema::ReadBlockPerform { block_header, }) =>
                        if let Some(block_bytes) = self.lru_cache.get(&block_id) {
                            Some(EventOp::IterBlocksItem(IterBlocksItemOp {
                                block_id: block_id.clone(),
                                block_bytes: block_bytes.clone(),
                                iter_blocks_state: IterBlocksState {
                                    iter_blocks_stream_context,
                                    iter_blocks_cursor: IterBlocksCursor {
                                        block_id: block_id.next(),
                                    },
                                },
                            }))
                        } else {
                            let mut lens = self.tasks_queue.focus_block_id(block_id.clone());
                            lens.push_task(
                                task::Task {
                                    block_id: block_id.clone(),
                                    kind: task::TaskKind::ReadBlock(task::ReadBlock {
                                        block_header: block_header.clone(),
                                        context: task::ReadBlockContext::Process(task::ReadBlockProcessContext::IterBlocks {
                                            iter_blocks_stream_context,
                                            next_block_id: block_id.next(),
                                        }),
                                    }),
                                },
                                self.schema.block_get(),
                            );
                            lens.enqueue(self.schema.block_get());
                            None
                        },

                    schema::ReadBlockOp::NotFound =>
                        unreachable!(),

                },
        }
    }

    fn maybe_run_background_task(mut self) -> Op<C> {
        loop {
            if let Some((offset, mut lens)) = self.tasks_queue.next_trigger(self.bg_task.current_offset, self.schema.block_get()) {
                let mut task_kind = match lens.pop_task(self.schema.block_get()) {
                    Some(task_kind) => task_kind,
                    None => panic!("empty task queue unexpected for block {:?} @ {}", lens.block_id(), offset),
                };
                match &task_kind {
                    task::TaskKind::WriteBlock(..) |
                    task::TaskKind::ReadBlock(..) |
                    task::TaskKind::DeleteBlock(task::DeleteBlock {
                        context: task::DeleteBlockContext::External(..),
                        ..
                    }) =>
                        (),
                    task::TaskKind::DeleteBlock(task::DeleteBlock {
                        context: task::DeleteBlockContext::Defrag { defrag_gaps, .. },
                        ..
                    }) =>
                        if !defrag_gaps.is_still_relevant(lens.block_id(), self.schema.block_get()) {
                            cancel_defrag_task(self.defrag.as_mut().unwrap());
                            lens.finish(self.schema.block_get());
                            lens.enqueue(self.schema.block_get());
                            continue;
                        },
                }

                match &mut task_kind {
                    task::TaskKind::WriteBlock(task::WriteBlock { commit, .. }) |
                    task::TaskKind::DeleteBlock(task::DeleteBlock { commit, .. }) =>
                        if self.schema.is_last_block(&lens.block_id()) {
                            *commit = task::Commit::WithTerminator;
                        },
                    task::TaskKind::ReadBlock(..) =>
                        (),
                };

                self.bg_task.state = BackgroundTaskState::Await {
                    block_id: lens.block_id().clone(),
                };
                return Op::Query(QueryOp::InterpretTask(InterpretTask {
                    offset,
                    task: task::Task {
                        block_id: lens.block_id().clone(),
                        kind: task_kind,
                    },
                    next: InterpretTaskNext {
                        inner: self,
                    },
                }));
            } else {
                return Op::Query(QueryOp::PollRequest(PollRequest {
                    next: PollRequestNext {
                        inner: self,
                    },
                }));
            }
        }
    }
}

fn cancel_defrag_task<C>(defrag: &mut Defrag<C>) {
    assert!(defrag.in_progress_tasks_count > 0);
    defrag.in_progress_tasks_count -= 1;
}
