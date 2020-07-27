use std::cmp;

use super::{
    block,
    storage,
    context::Context,
};

pub mod queue;
mod pending;

#[derive(Debug)]
struct Task<C> where C: Context {
    offset: u64,
    serial: usize,
    task: TaskKind<C>,
}

impl<C> PartialEq for Task<C> where C: Context {
    fn eq(&self, other: &Task<C>) -> bool {
        self.offset == other.offset && self.serial == other.serial
    }
}

impl<C> Eq for Task<C> where C: Context { }

impl<C> PartialOrd for Task<C> where C: Context {
    fn partial_cmp(&self, other: &Task<C>) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<C> Ord for Task<C> where C: Context {
    fn cmp(&self, other: &Task<C>) -> cmp::Ordering {
        other.offset.cmp(&self.offset)
            .then_with(|| self.serial.cmp(&other.serial))
    }
}

#[derive(PartialEq, Eq, Debug)]
pub enum TaskKind<C> where C: Context {
    WriteBlock(WriteBlock<C::WriteBlock>),
    ReadBlock(ReadBlock<C::ReadBlock>),
    MarkTombstone(MarkTombstone<C::DeleteBlock>),
}

#[derive(PartialEq, Eq, Debug)]
pub struct WriteBlock<C> {
    pub block_id: block::Id,
    pub block_bytes: block::Bytes,
    pub commit_type: CommitType,
    pub context: WriteBlockContext<C>,
}

#[derive(PartialEq, Eq, Debug)]
pub enum CommitType {
    CommitOnly,
    CommitAndEof,
}

#[derive(PartialEq, Eq, Debug)]
pub enum WriteBlockContext<C> {
    External(C),
}

#[derive(PartialEq, Eq, Debug)]
pub struct ReadBlock<C> {
    pub block_header: storage::BlockHeader,
    pub block_bytes: block::BytesMut,
    pub context: ReadBlockContext<C>,
}

#[derive(PartialEq, Eq, Debug)]
pub enum ReadBlockContext<C> {
    External(C),
}

#[derive(PartialEq, Eq, Debug)]
pub struct MarkTombstone<C> {
    pub block_id: block::Id,
    pub context: MarkTombstoneContext<C>,
}

#[derive(PartialEq, Eq, Debug)]
pub enum MarkTombstoneContext<C> {
    External(C),
}

#[derive(Debug)]
pub struct Done<C> where C: Context {
    pub current_offset: u64,
    pub task: TaskDone<C>,
}

#[derive(Debug)]
pub enum TaskDone<C> where C: Context {
    WriteBlock(TaskDoneWriteBlock<C::WriteBlock>),
    ReadBlock(TaskDoneReadBlock<C::ReadBlock>),
    MarkTombstone(TaskDoneMarkTombstone<C::DeleteBlock>),
}

#[derive(Debug)]
pub struct TaskDoneWriteBlock<C> {
    pub block_id: block::Id,
    pub context: WriteBlockContext<C>,
}

#[derive(Debug)]
pub struct TaskDoneReadBlock<C> {
    pub block_id: block::Id,
    pub block_bytes: block::BytesMut,
    pub context: ReadBlockContext<C>,
}

#[derive(Debug)]
pub struct TaskDoneMarkTombstone<C> {
    pub block_id: block::Id,
    pub context: MarkTombstoneContext<C>,
}
