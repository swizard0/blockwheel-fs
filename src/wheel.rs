use futures::{
    stream,
    channel::{
        mpsc,
        oneshot,
    },
};

use ero::{
    ErrorSeverity,
};

use super::{
    block,
    Params,
    Deleted,
};


pub enum Request {
    LendBlock {
        reply_tx: oneshot::Sender<block::BytesMut>,
    },
    RepayBlock {
        block_bytes: block::Bytes,
    },
    WriteBlock {
        block_bytes: block::Bytes,
        reply_tx: oneshot::Sender<block::Id>,
    },
    ReadBlock {
        block_id: block::Id,
        reply_tx: oneshot::Sender<block::BytesMut>,
    },
    DeleteBlock {
        block_id: block::Id,
        reply_tx: oneshot::Sender<Deleted>,
    },
}

pub struct State {
    pub fused_request_rx: stream::Fuse<mpsc::Receiver<Request>>,
    pub params: Params,
}

pub async fn busyloop(state: State) -> Result<(), ErrorSeverity<State, ()>> {

    Ok(())
}
