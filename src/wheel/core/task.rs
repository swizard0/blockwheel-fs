use std::{
    fmt,
    cmp,
};

use alloc_pool::bytes::{
    Bytes,
    BytesMut,
};

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

pub enum TaskKind<C> where C: Context {
    WriteBlock(WriteBlock<C::WriteBlock>),
    ReadBlock(ReadBlock<C>),
    DeleteBlock(DeleteBlock<C::DeleteBlock>),
}

impl<C> fmt::Debug for TaskKind<C> where C: Context {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TaskKind::WriteBlock(write_block) =>
                fmt.debug_tuple("WriteBlock").field(write_block).finish(),
            TaskKind::ReadBlock(read_block) =>
                fmt.debug_tuple("ReadBlock").field(read_block).finish(),
            TaskKind::DeleteBlock(delete_block) =>
                fmt.debug_tuple("DeleteBlock").field(delete_block).finish(),
        }
    }
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Commit {
    None,
    WithTerminator,
}

pub struct WriteBlock<C> {
    pub write_block_bytes: Bytes,
    pub commit: Commit,
    pub context: WriteBlockContext<C>,
}

impl<C> fmt::Debug for WriteBlock<C> {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt.debug_struct("WriteBlock")
            .field("write_block_bytes", &self.write_block_bytes)
            .field("context", &self.context)
            .finish()
    }
}

pub enum WriteBlockContext<C> {
    External(C),
    Defrag,
}

impl<C> fmt::Debug for WriteBlockContext<C> {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WriteBlockContext::External(..) =>
                write!(fmt, "WriteBlockContext::External(..)"),
            WriteBlockContext::Defrag =>
                write!(fmt, "WriteBlockContext::Defrag"),
        }
    }
}

impl<C> cmp::PartialEq for WriteBlockContext<C> where C: PartialEq {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (WriteBlockContext::External(a), WriteBlockContext::External(b)) =>
                a == b,
            (WriteBlockContext::Defrag, WriteBlockContext::Defrag) =>
                true,
            _ =>
                false,
        }
    }
}

pub struct ReadBlock<C> where C: Context {
    pub block_header: storage::BlockHeader,
    pub context: ReadBlockContext<C>,
}

impl<C> fmt::Debug for ReadBlock<C> where C: Context {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt.debug_struct("ReadBlock")
            .field("block_header", &self.block_header)
            .field("context", &self.context)
            .finish()
    }
}

pub enum ReadBlockContext<C> where C: Context {
    Process(ReadBlockProcessContext<C>),
    Defrag(ReadBlockDefragContext),
}

pub struct ReadBlockDefragContext {
    pub defrag_gaps: DefragGaps,
}

pub enum ReadBlockProcessContext<C> where C: Context {
    External(C::ReadBlock),
    IterBlocks {
        iter_blocks_stream_context: C::IterBlocksStream,
        next_block_id: block::Id,
    },
}

impl<C> cmp::PartialEq for ReadBlockProcessContext<C> where C: Context, C::ReadBlock: PartialEq, C::IterBlocksStream: PartialEq {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (ReadBlockProcessContext::External(a), ReadBlockProcessContext::External(b)) =>
                a == b,
            (
                ReadBlockProcessContext::IterBlocks { iter_blocks_stream_context: a, .. },
                ReadBlockProcessContext::IterBlocks { iter_blocks_stream_context: b, .. },
            ) =>
                a == b,
            _ =>
                false,
        }
    }
}

impl<C> cmp::PartialEq for ReadBlockContext<C> where C: Context, C::ReadBlock: PartialEq, C::IterBlocksStream: PartialEq {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (ReadBlockContext::Process(a), ReadBlockContext::Process(b)) =>
                a == b,
            (
                ReadBlockContext::Defrag(ReadBlockDefragContext { defrag_gaps: a, }),
                ReadBlockContext::Defrag(ReadBlockDefragContext { defrag_gaps: b, }),
            ) =>
                a == b,
            _ =>
                false,
        }
    }
}

impl<C> fmt::Debug for ReadBlockProcessContext<C> where C: Context {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ReadBlockProcessContext::External(..) =>
                write!(fmt, "ReadBlockProcessContext::External(..)"),
            ReadBlockProcessContext::IterBlocks { .. } =>
                write!(fmt, "ReadBlockProcessContext::IterBlocks(..)"),
        }
    }
}

impl<C> fmt::Debug for ReadBlockContext<C> where C: Context {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ReadBlockContext::Process(context) =>
                write!(fmt, "ReadBlockContext::Process({:?})", context),
            ReadBlockContext::Defrag(ReadBlockDefragContext { defrag_gaps, }) =>
                write!(fmt, "ReadBlockContext::Defrag(ReadBlockDefragContext {{ defrag_gaps: {:?} }})", defrag_gaps),
        }
    }
}

pub struct DeleteBlock<C> {
    pub delete_block_bytes: Bytes,
    pub commit: Commit,
    pub context: DeleteBlockContext<C>,
}

impl<C> fmt::Debug for DeleteBlock<C> {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt.debug_struct("DeleteBlock")
            .field("delete_block_bytes", &self.delete_block_bytes)
            .field("context", &self.context)
            .finish()
    }
}

pub enum DeleteBlockContext<C> {
    External(C),
    Defrag {
        defrag_gaps: DefragGaps,
        block_bytes: Bytes,
    },
}

impl<C> cmp::PartialEq for DeleteBlockContext<C> where C: PartialEq {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (DeleteBlockContext::External(a), DeleteBlockContext::External(b)) =>
                a == b,
            (
                DeleteBlockContext::Defrag { defrag_gaps: dga, block_bytes: bba, },
                DeleteBlockContext::Defrag { defrag_gaps: dgb, block_bytes: bbb, },
            ) =>
                dga == dgb && bba == bbb,
            _ =>
                false,
        }
    }
}

impl<C> fmt::Debug for DeleteBlockContext<C> {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DeleteBlockContext::External(..) =>
                write!(fmt, "DeleteBlockContext::External(..)"),
            DeleteBlockContext::Defrag { .. } =>
                write!(fmt, "DeleteBlockContext::Defrag"),
        }
    }
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

pub enum TaskDoneKind<C> where C: Context {
    WriteBlock(TaskDoneWriteBlock<C::WriteBlock>),
    ReadBlock(TaskDoneReadBlock<C>),
    DeleteBlock(TaskDoneDeleteBlock<C::DeleteBlock>),
}

impl<C> fmt::Debug for TaskDoneKind<C> where C: Context {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TaskDoneKind::WriteBlock(write_block) =>
                fmt.debug_tuple("WriteBlock").field(write_block).finish(),
            TaskDoneKind::ReadBlock(read_block) =>
                fmt.debug_tuple("ReadBlock").field(read_block).finish(),
            TaskDoneKind::DeleteBlock(delete_block) =>
                fmt.debug_tuple("DeleteBlock").field(delete_block).finish(),
        }
    }
}

pub struct TaskDoneWriteBlock<C> {
    pub context: WriteBlockContext<C>,
}

impl<C> fmt::Debug for TaskDoneWriteBlock<C> {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt.debug_struct("TaskDoneWriteBlock")
            .field("context", &self.context)
            .finish()
    }
}

pub struct TaskDoneReadBlock<C> where C: Context {
    pub block_bytes: BytesMut,
    pub context: ReadBlockContext<C>,
}

impl<C> fmt::Debug for TaskDoneReadBlock<C> where C: Context {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt.debug_struct("TaskDoneReadBlock")
            .field("block_bytes", &self.block_bytes)
            .field("context", &self.context)
            .finish()
    }
}

pub struct TaskDoneDeleteBlock<C> {
    pub context: DeleteBlockContext<C>,
}

impl<C> fmt::Debug for TaskDoneDeleteBlock<C> {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt.debug_struct("TaskDoneDeleteBlock")
            .field("context", &self.context)
            .finish()
    }
}
