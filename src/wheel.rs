use std::{
    io,
    path::PathBuf,
};

use futures::{
    select,
    stream,
    channel::{
        mpsc,
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

use ero::{
    ErrorSeverity,
};

use super::{
    block,
    proto,
    storage,
    Params,
};

mod gaps;
mod task;
mod pool;
mod index;
mod defrag;
mod schema;

#[derive(Debug)]
pub enum Error {
    InitWheelSizeIsTooSmall {
        provided: u64,
        required_min: u64,
    },
    WheelFileMetadata {
        wheel_filename: PathBuf,
        error: io::Error,
    },
    WheelFileOpen {
        wheel_filename: PathBuf,
        error: io::Error,
    },
    WheelFileDefaultRegularHeaderEncode(bincode::Error),
    WheelFileDefaultCommitTagEncode(bincode::Error),
    WheelFileWheelHeaderEncode(bincode::Error),
    WheelFileWheelHeaderWrite(io::Error),
    WheelFileEofBlockHeaderEncode(bincode::Error),
    WheelFileEofBlockHeaderWrite(io::Error),
    WheelFileZeroInitWrite(io::Error),
    WheelFileZeroInitFlush(io::Error),
}

pub struct State {
    pub fused_request_rx: stream::Fuse<mpsc::Receiver<proto::Request>>,
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

async fn busyloop(mut state: State, mut schema: schema::Schema) -> Result<(), ErrorSeverity<State, Error>> {
    let mut blocks_pool = pool::Blocks::new();

    let mut tasks_queue = task::Queue::new();

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

            Source::Pid(Some(proto::Request::LendBlock(proto::RequestLendBlock { reply_tx, }))) => {
                let block = blocks_pool.lend();
                if let Err(_send_error) = reply_tx.send(block) {
                    log::warn!("Pid is gone during query result send");
                }
            },

            Source::Pid(Some(proto::Request::RepayBlock(proto::RequestRepayBlock { block_bytes, }))) => {
                blocks_pool.repay(block_bytes);
            },

            Source::Pid(Some(proto::Request::WriteBlock(request_write_block))) =>
                schema.process_write_block_request(request_write_block, &mut tasks_queue),

            Source::Pid(Some(proto::Request::ReadBlock(proto::RequestReadBlock { block_id, reply_tx, }))) => {

                unimplemented!()
            },

            Source::Pid(Some(proto::Request::DeleteBlock(proto::RequestDeleteBlock { block_id, reply_tx, }))) => {

                unimplemented!()
            },
        }
    }
}

struct WheelWriter<'a> {
    wheel_file_writer: BufWriter<&'a mut fs::File>,
    work_block: &'a mut Vec<u8>,
    cursor: u64,
}

impl<'a> WheelWriter<'a> {
    fn new(wheel_file: &'a mut fs::File, work_block: &'a mut Vec<u8>, work_block_size: usize) -> WheelWriter<'a> {
        WheelWriter {
            wheel_file_writer: BufWriter::with_capacity(work_block_size, wheel_file),
            work_block,
            cursor: 0,
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
        self.cursor += self.work_block.len() as u64;
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

async fn wheel_create(state: State) -> Result<(schema::Schema, State), ErrorSeverity<State, Error>> {
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

    bincode::serialize_into(&mut work_block, &storage::BlockHeader::Regular(storage::BlockHeaderRegular::default()))
        .map_err(Error::WheelFileDefaultRegularHeaderEncode)
        .map_err(ErrorSeverity::Fatal)?;
    let regular_block_header_size = work_block.len();
    work_block.clear();

    bincode::serialize_into(&mut work_block, &storage::CommitTag::default())
        .map_err(Error::WheelFileDefaultCommitTagEncode)
        .map_err(ErrorSeverity::Fatal)?;
    let commit_tag_size = work_block.len();
    work_block.clear();

    let mut wheel_writer = WheelWriter::new(&mut wheel_file, &mut work_block, state.params.work_block_size);

    let wheel_header = storage::WheelHeader {
        size_bytes: state.params.init_wheel_size_bytes,
        ..storage::WheelHeader::default()
    };
    wheel_writer.write_serialize(&wheel_header, Error::WheelFileWheelHeaderEncode, Error::WheelFileWheelHeaderWrite).await?;
    let wheel_header_size = wheel_writer.cursor as usize;

    let eof_block_header = storage::BlockHeader::EndOfFile;
    let eof_block_start_offset = wheel_writer.cursor;
    wheel_writer.write_serialize(&eof_block_header, Error::WheelFileEofBlockHeaderEncode, Error::WheelFileEofBlockHeaderWrite).await?;
    let eof_block_header_size = wheel_writer.cursor as usize - wheel_header_size;

    let storage_layout = storage::Layout {
        wheel_header_size,
        regular_block_header_size,
        eof_block_header_size,
        commit_tag_size,
    };

    let size_bytes_total = state.params.init_wheel_size_bytes as u64;

    if size_bytes_total < storage_layout.data_size_service_min() as u64 {
        return Err(ErrorSeverity::Fatal(Error::InitWheelSizeIsTooSmall {
            provided: size_bytes_total,
            required_min: storage_layout.data_size_service_min() as u64,
        }));
    }

    while wheel_writer.cursor < size_bytes_total {
        let bytes_remain = size_bytes_total - wheel_writer.cursor;
        let write_amount = if bytes_remain < size_bytes_total {
            bytes_remain
        } else {
            size_bytes_total
        };
        wheel_writer.work_block.extend((0 .. write_amount).map(|_| 0));
        wheel_writer.write_work_block(Error::WheelFileZeroInitWrite).await?;
    }
    wheel_writer.flush(Error::WheelFileZeroInitFlush).await?;

    let mut schema = schema::Schema::new(storage_layout);
    schema.initialize_empty(
        eof_block_start_offset,
        eof_block_header,
        size_bytes_total,
    );

    log::debug!("initialized wheel schema: {:?}", schema);

    Ok((schema, state))
}

async fn wheel_open(state: State) -> Result<(schema::Schema, State), ErrorSeverity<State, Error>> {
    log::debug!("opening existing wheel file [ {:?} ]", state.params.wheel_filename);

    unimplemented!()
}
