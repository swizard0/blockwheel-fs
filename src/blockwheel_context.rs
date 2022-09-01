use std::{
    marker::{
        PhantomData,
    },
};

use alloc_pool::{
    bytes::{
        Bytes,
    },
};

use arbeitssklave::{
    komm,
};

use crate::{
    block,
    context,
    ReplyPolicy,
};

#[derive(Debug)]
pub struct Context<B, R> {
    _marker: PhantomData<(B, R)>,
}

impl<B, R> Context<B, R> {
    pub fn new() -> Self {
        Self { _marker: PhantomData, }
    }
}

impl<B, R> context::Context for Context<B, R> where R: ReplyPolicy<B> {
    type Info = komm::Rueckkopplung<B, R::Info>;
    type Flush = komm::Rueckkopplung<Flushed, R::Flush>;
    type WriteBlock = komm::Rueckkopplung<Result<block::Id, RequestWriteBlockError>, R::WriteBlock>;
    type ReadBlock = komm::Rueckkopplung<Result<Bytes, RequestReadBlockError>, R::ReadBlock>;
    type DeleteBlock = komm::Rueckkopplung<Result<Deleted, RequestDeleteBlockError>, R::DeleteBlock>;
    type IterBlocksInit = komm::Rueckkopplung<IterBlocks, R::IterBlocksInit>;
    type IterBlocksNext = komm::Rueckkopplung<IterBlocksItem, R::IterBlocksNext>;
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
