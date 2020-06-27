use std::{
    io,
    path::PathBuf,
};

use futures::{
    select,
    stream,
    channel::{
        mpsc,
        oneshot,
    },
    StreamExt,
};

use tokio::{
    fs,
    io::{
        BufReader,
        BufWriter,
        AsyncReadExt,
        AsyncWriteExt,
    },
};

use serde_derive::{
    Serialize,
    Deserialize,
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

#[derive(Debug)]
pub enum Error {
    WheelFileMetadata {
        wheel_filename: PathBuf,
        error: io::Error,
    },
    WheelFileOpen {
        wheel_filename: PathBuf,
        error: io::Error,
    },
    WheelFileWheelHeaderEncode(bincode::Error),
    WheelFileWheelHeaderWrite(io::Error),
    WheelFileEofBlockHeaderEncode(bincode::Error),
    WheelFileEofBlockHeaderWrite(io::Error),
    WheelFileZeroInitWrite(io::Error),
    WheelFileZeroInitFlush(io::Error),
}

pub struct State {
    pub fused_request_rx: stream::Fuse<mpsc::Receiver<Request>>,
    pub params: Params,
}

pub async fn busyloop_init(state: State) -> Result<(), ErrorSeverity<State, Error>> {
    let (wheel, state) = match fs::metadata(&state.params.wheel_filename).await {
        Ok(ref metadata) if metadata.file_type().is_file() =>
            wheel_open(state).await?,
        Ok(_metadata) => {
            log::error!("[ {:?} ] is not a file", state.params.wheel_filename);
            return Err(ErrorSeverity::Recoverable { state, });
        },
        Err(ref error) if error.kind() == io::ErrorKind::NotFound =>
            wheel_create(state).await?,
        Err(error) =>
            return Err(ErrorSeverity::Fatal(Error::WheelFileMetadata {
                wheel_filename: state.params.wheel_filename,
                error,
            })),
    };

    busyloop(state, wheel).await
}

async fn busyloop(mut state: State, wheel: Wheel) -> Result<(), ErrorSeverity<State, Error>> {
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

struct Wheel;

const WHEEL_MAGIC: u64 = 0xc0f124c9f1ba71d5;
const WHEEL_VERSION: usize = 1;

#[derive(Serialize, Deserialize)]
struct WheelHeader {
    magic: u64,
    version: usize,
    size_bytes: usize,
}

#[derive(Serialize, Deserialize)]
enum BlockHeader {
    EndOfFile,
}

struct WheelWriter<'a> {
    wheel_file_writer: BufWriter<&'a mut fs::File>,
    work_block: &'a mut Vec<u8>,
}

impl<'a> WheelWriter<'a> {
    fn new(wheel_file: &'a mut fs::File, work_block: &'a mut Vec<u8>, work_block_size: usize) -> WheelWriter<'a> {
        WheelWriter {
            wheel_file_writer: BufWriter::with_capacity(work_block_size, wheel_file),
            work_block,
        }
    }

    async fn write_serialize<T, S, SME, WME>(
        &mut self,
        object: &T,
        serialize_map_err: SME,
        write_map_err: WME,
    )
        -> Result<(), ErrorSeverity<S, Error>>
    where T: serde::Serialize,
          SME: Fn(bincode::Error) -> Error,
          WME: Fn(io::Error) -> Error,
    {
        bincode::serialize_into(self.work_block(), object)
            .map_err(serialize_map_err)
            .map_err(ErrorSeverity::Fatal)?;
        self.write_work_block(write_map_err).await
    }

    async fn write_work_block<S, WME>(&mut self, write_map_err: WME) -> Result<(), ErrorSeverity<S, Error>> where WME: Fn(io::Error) -> Error {
        self.wheel_file_writer.write_all(self.work_block).await
            .map_err(write_map_err)
            .map_err(ErrorSeverity::Fatal)?;
        self.work_block.clear();
        Ok(())
    }

    async fn flush<S, FME>(&mut self, flush_map_err: FME) -> Result<(), ErrorSeverity<S, Error>> where FME: Fn(io::Error) -> Error {
        self.wheel_file_writer.flush().await
            .map_err(flush_map_err)
            .map_err(ErrorSeverity::Fatal)
    }

    fn work_block(&mut self) -> &mut &'a mut Vec<u8> {
        &mut self.work_block
    }
}

async fn wheel_create(state: State) -> Result<(Wheel, State), ErrorSeverity<State, Error>> {
    log::debug!("creating new wheel file [ {:?} ]", state.params.wheel_filename);

    let maybe_file = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(&state.params.wheel_filename)
        .await;
    let mut wheel_file = match maybe_file {
        Ok(file) =>
            file,
        Err(error) =>
            return Err(ErrorSeverity::Fatal(Error::WheelFileOpen {
                wheel_filename: state.params.wheel_filename,
                error,
            })),
    };
    let mut work_block: Vec<u8> = Vec::with_capacity(state.params.work_block_size);
    let mut wheel_writer = WheelWriter::new(&mut wheel_file, &mut work_block, state.params.work_block_size);

    let wheel_header = WheelHeader {
        magic: WHEEL_MAGIC,
        version: WHEEL_VERSION,
        size_bytes: state.params.init_wheel_size_bytes,
    };
    wheel_writer.write_serialize(&wheel_header, Error::WheelFileWheelHeaderEncode, Error::WheelFileWheelHeaderWrite).await?;

    let eof_block_header = BlockHeader::EndOfFile;
    wheel_writer.write_serialize(&eof_block_header, Error::WheelFileEofBlockHeaderEncode, Error::WheelFileEofBlockHeaderWrite).await?;

    let mut total_initialized = 0;
    while total_initialized < state.params.init_wheel_size_bytes {
        let bytes_remain = state.params.init_wheel_size_bytes - total_initialized;
        let write_amount = if bytes_remain < state.params.init_wheel_size_bytes {
            bytes_remain
        } else {
            state.params.init_wheel_size_bytes
        };
        wheel_writer.work_block.extend((0 .. write_amount).map(|_| 0));
        wheel_writer.write_work_block(Error::WheelFileZeroInitWrite).await?;
    }
    wheel_writer.flush(Error::WheelFileZeroInitFlush).await?;

    unimplemented!()
}

async fn wheel_open(state: State) -> Result<(Wheel, State), ErrorSeverity<State, Error>> {
    log::debug!("opening existing wheel file [ {:?} ]", state.params.wheel_filename);

    unimplemented!()
}
