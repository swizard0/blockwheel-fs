
use super::{
    lru,
    task,
    pool,
    proto,
    defrag,
};

struct Inner<IC> {
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

impl<IC> Inner<IC> {
    fn new(total_bytes_limit: usize) -> Inner<IC> {
        Inner {
            lru_cache: lru::Cache::new(total_bytes_limit),
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
    pub fn new(total_bytes_limit: usize) -> Performer<IC> {
        Performer {
            inner: Inner::new(total_bytes_limit),
        }
    }

    pub fn next(self) -> Op<IC> {

        unimplemented!()
    }
}

pub struct PollRequestAndInterpreter<IC> {
    pub interpret_task: IC,
    inner: Inner<IC>,
}

pub enum RequestOrInterpreterIncoming {
    Request(proto::Request),
    Interpreter(task::Done),
}

impl<IC> PollRequestAndInterpreter<IC> {
    pub fn next(self, incoming: RequestOrInterpreterIncoming) -> Performer<IC> {

        unimplemented!()
    }
}

pub struct PollRequest<IC> {
    inner: Inner<IC>,
}

impl<IC> PollRequest<IC> {
    pub fn next(self, incoming: proto::Request) -> Performer<IC> {

        unimplemented!()
    }
}

pub struct InterpretTask<IC> {
    pub offset: u64,
    pub task: task::TaskKind,
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
