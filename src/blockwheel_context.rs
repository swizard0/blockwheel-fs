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
    AccessPolicy,
};

#[derive(Debug)]
pub struct Context<A> {
    _marker: PhantomData<A>,
}

impl<A> Context<A> {
    pub fn new() -> Self {
        Self { _marker: PhantomData, }
    }
}

impl<A> context::Context for Context<A> where A: AccessPolicy {
    type Info = komm::Rueckkopplung<A::Order, A::Info>;
    type Flush = komm::Rueckkopplung<A::Order, A::Flush>;
    type WriteBlock = komm::Rueckkopplung<A::Order, A::WriteBlock>;
    type ReadBlock = komm::Rueckkopplung<A::Order, A::ReadBlock>;
    type DeleteBlock = komm::Rueckkopplung<A::Order, A::DeleteBlock>;
    type IterBlocksInit = komm::Rueckkopplung<A::Order, A::IterBlocksInit>;
    type IterBlocksNext = komm::Rueckkopplung<A::Order, A::IterBlocksNext>;
}
