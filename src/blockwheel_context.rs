use std::{
    marker::{
        PhantomData,
    },
};

use crate::{
    context,
    EchoPolicy,
};

#[derive(Debug)]
pub struct Context<E> {
    _marker: PhantomData<E>,
}

impl<E> Default for Context<E> {
    fn default() -> Self {
        Self::new()
    }
}

impl<E> Context<E> {
    pub fn new() -> Self {
        Self { _marker: PhantomData, }
    }
}

impl<E> context::Context for Context<E> where E: EchoPolicy {
    type Info = E::Info;
    type Flush = E::Flush;
    type WriteBlock = E::WriteBlock;
    type ReadBlock = E::ReadBlock;
    type DeleteBlock = E::DeleteBlock;
    type IterBlocksInit = E::IterBlocksInit;
    type IterBlocksNext = E::IterBlocksNext;
}
