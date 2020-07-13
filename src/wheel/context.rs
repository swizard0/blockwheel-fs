use std::fmt::Debug;

pub trait Context {
    type LendBlock: Debug;
    type WriteBlock: Debug;
    type ReadBlock: Debug;
    type DeleteBlock: Debug;
    type Interpreter;
}

pub mod blockwheel {
    use futures::{
        channel::{
            oneshot,
        },
        future,
    };

    use super::super::{
        block,
        task,
        Deleted,
    };

    pub struct Context;

    impl super::Context for Context {
        type LendBlock = oneshot::Sender<block::BytesMut>;
        type WriteBlock = oneshot::Sender<Result<block::Id, RequestWriteBlockError>>;
        type ReadBlock = oneshot::Sender<Result<block::Bytes, RequestReadBlockError>>;
        type DeleteBlock = oneshot::Sender<Result<Deleted, RequestDeleteBlockError>>;
        type Interpreter = future::Fuse<oneshot::Receiver<task::Done<Self>>>;
    }

    #[derive(Clone, PartialEq, Eq, Debug)]
    pub enum RequestWriteBlockError {
        NoSpaceLeft,
    }

    #[derive(Clone, PartialEq, Eq, Debug)]
    pub enum RequestReadBlockError {
        NotFound,
    }

    #[derive(Clone, PartialEq, Eq, Debug)]
    pub enum RequestDeleteBlockError {
        NotFound,
    }
}
