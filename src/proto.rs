use super::{
    block,
    wheel::context::Context,
};

#[derive(PartialEq, Eq, Debug)]
pub enum Request<C> where C: Context {
    LendBlock(RequestLendBlock<C::LendBlock>),
    RepayBlock(RequestRepayBlock),
    WriteBlock(RequestWriteBlock<C::WriteBlock>),
    ReadBlock(RequestReadBlock<C::ReadBlock>),
    DeleteBlock(RequestDeleteBlock<C::DeleteBlock>),
}

#[derive(PartialEq, Eq, Debug)]
pub struct RequestLendBlock<C> {
    pub context: C,
}

#[derive(PartialEq, Eq, Debug)]
pub struct RequestRepayBlock {
    pub block_bytes: block::Bytes,
}

#[derive(PartialEq, Eq, Debug)]
pub struct RequestWriteBlock<C> {
    pub block_bytes: block::Bytes,
    pub context: C,
}

#[derive(PartialEq, Eq, Debug)]
pub struct RequestReadBlock<C> {
    pub block_id: block::Id,
    pub context: C,
}

#[derive(PartialEq, Eq, Debug)]
pub struct RequestDeleteBlock<C> {
    pub block_id: block::Id,
    pub context: C,
}
