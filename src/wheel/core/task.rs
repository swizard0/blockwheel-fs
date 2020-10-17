use super::{
    block,
    storage,
    DefragGaps,
};

use crate::context::Context;

pub mod queue;

#[derive(Debug)]
pub struct Task<C> where C: Context {
    pub block_id: block::Id,
    pub kind: TaskKind<C>,
}

#[derive(Debug)]
pub enum TaskKind<C> where C: Context {
    WriteBlock(WriteBlock<C::WriteBlock>),
    ReadBlock(ReadBlock<C::ReadBlock>),
    DeleteBlock(DeleteBlock<C::DeleteBlock>),
}

#[derive(Debug)]
pub struct WriteBlock<C> {
    pub block_bytes: block::Bytes,
    pub context: WriteBlockContext<C>,
}

#[derive(Clone, PartialEq, Debug)]
pub enum WriteBlockContext<C> {
    External(C),
    Defrag,
}

#[derive(Debug)]
pub struct ReadBlock<C> {
    pub block_header: storage::BlockHeader,
    pub block_bytes: block::BytesMut,
    pub context: ReadBlockContext<C>,
}

#[derive(Clone, PartialEq, Debug)]
pub enum ReadBlockContext<C> {
    External(C),
    Defrag { defrag_gaps: DefragGaps, },
}

#[derive(Debug)]
pub struct DeleteBlock<C> {
    pub context: DeleteBlockContext<C>,
}

#[derive(Clone, PartialEq, Debug)]
pub enum DeleteBlockContext<C> {
    External(C),
    Defrag {
        defrag_gaps: DefragGaps,
        block_bytes: block::Bytes,
    },
}

#[derive(Debug)]
pub struct Flush<C> {
    pub context: C,
}

#[derive(Debug)]
pub struct Done<C> where C: Context {
    pub current_offset: u64,
    pub task: TaskDone<C>,
}

#[derive(Debug)]
pub struct TaskDone<C> where C: Context {
    pub block_id: block::Id,
    pub kind: TaskDoneKind<C>,
}

#[derive(Debug)]
pub enum TaskDoneKind<C> where C: Context {
    WriteBlock(TaskDoneWriteBlock<C::WriteBlock>),
    ReadBlock(TaskDoneReadBlock<C::ReadBlock>),
    DeleteBlock(TaskDoneDeleteBlock<C::DeleteBlock>),
}

#[derive(Debug)]
pub struct TaskDoneWriteBlock<C> {
    pub context: WriteBlockContext<C>,
}

#[derive(Debug)]
pub struct TaskDoneReadBlock<C> {
    pub block_bytes: block::BytesMut,
    pub context: ReadBlockContext<C>,
}

#[derive(Debug)]
pub struct TaskDoneDeleteBlock<C> {
    pub context: DeleteBlockContext<C>,
}
