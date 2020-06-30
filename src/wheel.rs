use std::{
    io,
    cmp,
    path::PathBuf,
    collections::{
        BinaryHeap,
    },
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

use ero::{
    ErrorSeverity,
};

use super::{
    gaps,
    block,
    proto,
    index,
    Params,
    storage::{
        WheelHeader,
        BlockHeader,
        BlockHeaderRegular,
    },
};

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

async fn busyloop(mut state: State, mut wheel: Wheel) -> Result<(), ErrorSeverity<State, Error>> {
    let mut blocks_pool = BlocksPool::new();

    let mut wheel_queue: BinaryHeap<WheelTask> = BinaryHeap::new();

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
                process_write_block_request(request_write_block, &mut wheel, &mut wheel_queue)?,

            Source::Pid(Some(proto::Request::ReadBlock(proto::RequestReadBlock { block_id, reply_tx, }))) => {

                unimplemented!()
            },

            Source::Pid(Some(proto::Request::DeleteBlock(proto::RequestDeleteBlock { block_id, reply_tx, }))) => {

                unimplemented!()
            },
        }
    }
}

fn process_write_block_request(
    proto::RequestWriteBlock { block_bytes, reply_tx, }: proto::RequestWriteBlock,
    wheel: &mut Wheel,
    wheel_queue: &mut BinaryHeap<WheelTask>,
)
    -> Result<(), ErrorSeverity<State, Error>>
{
    let block_id = wheel.next_block_id.clone();
    wheel.next_block_id = wheel.next_block_id.next();
    let task_write_block = TaskWriteBlock { block_id, block_bytes, reply_tx, };

    let space_required = task_write_block.block_bytes.len();

    match wheel.gaps.allocate(space_required, &wheel.blocks_index) {
        Ok(gaps::Allocated::Success { space_available, between, }) => {

            unimplemented!()
        },
        Ok(gaps::Allocated::PendingDefragmentation) => {

            unimplemented!()
        },
        Err(gaps::Error::NoSpaceLeft) => {

            unimplemented!()
        }
    }

    Ok(())
}

#[derive(Debug)]
struct WheelTask {
    offset: u64,
    task: WheelTaskKind,
}

impl PartialEq for WheelTask {
    fn eq(&self, other: &WheelTask) -> bool {
        self.offset == other.offset
    }
}

impl Eq for WheelTask { }

impl PartialOrd for WheelTask {
    fn partial_cmp(&self, other: &WheelTask) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for WheelTask {
    fn cmp(&self, other: &WheelTask) -> cmp::Ordering {
        other.offset.cmp(&self.offset)
    }
}

#[derive(Debug)]
enum WheelTaskKind {
    WriteBlock(TaskWriteBlock),
}

#[derive(Debug)]
struct TaskWriteBlock {
    block_id: block::Id,
    block_bytes: block::Bytes,
    reply_tx: oneshot::Sender<block::Id>,
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

#[derive(Debug)]
struct Wheel {
    next_block_id: block::Id,
    regular_block_size: usize,
    blocks_index: index::Blocks,
    gaps: gaps::Index,
}

impl Wheel {
    pub fn new(regular_block_size: usize) -> Wheel {
        Wheel {
            next_block_id: block::Id::init(),
            regular_block_size,
            blocks_index: index::Blocks::new(),
            gaps: gaps::Index::new(),
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
    bincode::serialize_into(&mut work_block, &BlockHeader::Regular(BlockHeaderRegular::default()))
        .map_err(Error::WheelFileDefaultRegularHeaderEncode)
        .map_err(ErrorSeverity::Fatal)?;
    let regular_block_size = work_block.len();
    work_block.clear();

    let mut wheel_writer = WheelWriter::new(&mut wheel_file, &mut work_block, state.params.work_block_size);

    let wheel_header = WheelHeader {
        size_bytes: state.params.init_wheel_size_bytes,
        ..WheelHeader::default()
    };
    wheel_writer.write_serialize(&wheel_header, Error::WheelFileWheelHeaderEncode, Error::WheelFileWheelHeaderWrite).await?;

    let eof_block_header = BlockHeader::EndOfFile;
    let eof_block_start_offset = wheel_writer.cursor;
    wheel_writer.write_serialize(&eof_block_header, Error::WheelFileEofBlockHeaderEncode, Error::WheelFileEofBlockHeaderWrite).await?;
    let eof_block_end_offset = wheel_writer.cursor;

    let size_bytes_total = state.params.init_wheel_size_bytes as u64;
    if size_bytes_total < eof_block_end_offset + regular_block_size as u64 {
        return Err(ErrorSeverity::Fatal(Error::InitWheelSizeIsTooSmall {
            provided: size_bytes_total,
            required_min: eof_block_end_offset + regular_block_size as u64,
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

    let mut wheel = Wheel::new(regular_block_size);
    wheel.blocks_index.insert(
        wheel.next_block_id.clone(),
        index::BlockEntry {
            offset: eof_block_start_offset,
            header: eof_block_header,
        },
    );

    let space_available = size_bytes_total - eof_block_end_offset - regular_block_size as u64;
    if space_available > 0 {
        wheel.gaps.insert(
            space_available as usize,
            gaps::GapBetween::BlockAndEnd {
                left_block: wheel.next_block_id.clone(),
            },
        );
    }

    wheel.next_block_id = wheel.next_block_id.next();

    log::debug!("initialized wheel: {:?}", wheel);

    Ok((wheel, state))
}

async fn wheel_open(state: State) -> Result<(Wheel, State), ErrorSeverity<State, Error>> {
    log::debug!("opening existing wheel file [ {:?} ]", state.params.wheel_filename);

    unimplemented!()
}
