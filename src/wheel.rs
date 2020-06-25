use futures::{
    select,
    stream,
    channel::{
        mpsc,
        oneshot,
    },
    StreamExt,
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

pub async fn busyloop(mut state: State) -> Result<(), ErrorSeverity<State, ()>> {
    let mut blocks_pool = BlocksPool::new();

    loop {
        enum Source<A> {
            Pid(A),
        }
        let req = select! {
            result = state.fused_request_rx.next() =>
                Source::Pid(result),
        };
        match req {
            Source::Pid(None) => {
                log::debug!("all Pid frontends have been terminated");
                return Ok(());
            },
            Source::Pid(Some(Request::LendBlock { reply_tx, })) => {
                let block = blocks_pool.lend();
                if let Err(_send_error) = reply_tx.send(block) {
                    log::warn!("Pid is gone during query result send");
                }
            },
            Source::Pid(Some(Request::RepayBlock { block_bytes, })) => {
                blocks_pool.repay(block_bytes);
            },
            Source::Pid(Some(Request::WriteBlock { block_bytes, reply_tx, })) => {

                unimplemented!()
            },
            Source::Pid(Some(Request::ReadBlock { block_id, reply_tx, })) => {

                unimplemented!()
            },
            Source::Pid(Some(Request::DeleteBlock { block_id, reply_tx, })) => {

                unimplemented!()
            },
        }
    }
}

struct BlocksPool {
    pool: Vec<block::Bytes>,
}

impl BlocksPool {
    fn new() -> BlocksPool {
        BlocksPool {
            pool: Vec::new(),
        }
    }

    fn lend(&mut self) -> block::BytesMut {
        let mut cursor = self.pool.len();
        while cursor > 0 {
            cursor -= 1;
            let frozen_block = self.pool.swap_remove(cursor);
            match frozen_block.into_mut() {
                Ok(block) =>
                    return block,
                Err(frozen_block) =>
                    self.pool.push(frozen_block),
            }
        }

        block::BytesMut::new()
    }

    fn repay(&mut self, block_bytes: block::Bytes) {
        self.pool.push(block_bytes)
    }
}
