use std::{
    io,
};

use alloc_pool::{
    bytes::{
        BytesMut,
        BytesPool,
    },
};

use alloc_pool_pack::{
    WriteToBytesMut,
};

use arbeitssklave::{
    ewig,
};

use crate::{
    job,
    storage,
    blockwheel_context::{
        Context,
    },
    wheel::{
        performer_sklave,
        core::{
            task,
            performer,
        },
        interpret::{
            Order,
            Request,
            Error as InterpretError,
            block_append_terminator,
        },
    },
    EchoPolicy,
    InterpretStats,
    RamInterpreterParams,
};

#[derive(Debug)]
pub enum Error {
    WheelCreate(WheelCreateError),
    ThreadSpawn(io::Error),
    Arbeitssklave(arbeitssklave::Error),
}

#[derive(Debug)]
pub enum WheelCreateError {
    InitWheelSizeIsTooSmall {
        provided: usize,
        required_min: usize,
    },
}

pub fn bootstrap<E, P>(
    sklave: &ewig::Sklave<Order<E>, InterpretError>,
    params: RamInterpreterParams,
    performer_sklave_meister: performer_sklave::Meister<E>,
    performer_builder: performer::PerformerBuilderInit<Context<E>>,
    blocks_pool: BytesPool,
    thread_pool: P,
)
    -> Result<(), InterpretError>
where E: EchoPolicy,
      P: edeltraud::ThreadPool<job::Job<E>>,
{
    let WheelData { memory, storage_layout, performer, } =
        create(&params, &blocks_pool, performer_builder)
        .map_err(Error::WheelCreate)?;
    performer_sklave_meister
        .befehl(
            performer_sklave::Order::Bootstrap(
                performer_sklave::OrderBootstrap {
                    performer,
                },
            ),
            &thread_pool,
        )
        .map_err(Error::Arbeitssklave)?;
    run(sklave, memory, storage_layout, performer_sklave_meister, blocks_pool, thread_pool)
}

fn create<E>(
    params: &RamInterpreterParams,
    blocks_pool: &BytesPool,
    performer_builder: performer::PerformerBuilderInit<Context<E>>,
)
    -> Result<WheelData<E>, WheelCreateError>
where E: EchoPolicy,
{
    log::debug!("creating new ram file of {:?} bytes", params.init_wheel_size_bytes);

    let mut memory = blocks_pool.lend();
    memory.reserve(params.init_wheel_size_bytes);

    let wheel_header = storage::WheelHeader {
        size_bytes: params.init_wheel_size_bytes as u64,
        ..storage::WheelHeader::default()
    };
    wheel_header.write_to_bytes_mut(&mut memory);

    let terminator_tag = storage::TerminatorTag::default();
    terminator_tag.write_to_bytes_mut(&mut memory);

    let min_wheel_file_size = performer_builder.storage_layout().wheel_header_size
        + performer_builder.storage_layout().terminator_tag_size;
    assert_eq!(memory.len(), min_wheel_file_size);
    let size_bytes_total = params.init_wheel_size_bytes;
    if size_bytes_total < min_wheel_file_size {
        return Err(WheelCreateError::InitWheelSizeIsTooSmall {
            provided: size_bytes_total,
            required_min: min_wheel_file_size,
        });
    }

    memory.resize(size_bytes_total, 0);

    log::debug!("ram file create success");
    let storage_layout = performer_builder.storage_layout().clone();

    let (performer_builder, _work_block) = performer_builder.start_fill();

    Ok(WheelData {
        memory,
        storage_layout,
        performer: performer_builder
            .finish(params.init_wheel_size_bytes),
    })
}

struct WheelData<E> where E: EchoPolicy {
    memory: BytesMut,
    storage_layout: storage::Layout,
    performer: performer::Performer<Context<E>>,
}

pub fn run<E, P>(
    sklave: &ewig::Sklave<Order<E>, InterpretError>,
    memory: BytesMut,
    storage_layout: storage::Layout,
    performer_sklave_meister: performer_sklave::Meister<E>,
    blocks_pool: BytesPool,
    thread_pool: P,
)
    -> Result<(), InterpretError>
where E: EchoPolicy,
      P: edeltraud::ThreadPool<job::Job<E>>,
{
    log::debug!("running background interpreter job");

    let mut stats = InterpretStats::default();

    let mut terminator_block_bytes = blocks_pool.lend();
    block_append_terminator(&mut terminator_block_bytes);

    let mut cursor = io::Cursor::new(memory);
    cursor.set_position(storage_layout.wheel_header_size as u64);

    'outer: loop {
        let orders = sklave.zu_ihren_diensten()?;
        for order in orders {
            match order {

                Order::Request(Request { offset, task, }) => {
                    stats.count_total += 1;
                    #[allow(clippy::comparison_chain)]
                    if cursor.position() != offset {
                        if cursor.position() < offset {
                            stats.count_seek_forward += 1;
                        } else if cursor.position() > offset {
                            stats.count_seek_backward += 1;
                        }
                        cursor.set_position(offset);
                    } else {
                        stats.count_no_seek += 1;
                    }

                    match task.kind {
                        task::TaskKind::WriteBlock(write_block) => {
                            let start = cursor.position() as usize;
                            let slice = cursor.get_mut();
                            let written = match write_block.write_block_bytes {
                                task::WriteBlockBytes::Chunk(write_block_bytes) => {
                                    slice[start .. start + write_block_bytes.len()]
                                        .copy_from_slice(&write_block_bytes);
                                    write_block_bytes.len()
                                },
                                task::WriteBlockBytes::Composite(task::WriteBlockBytesComposite {
                                    block_header,
                                    block_bytes,
                                    commit_tag,
                                }) => {
                                    let mut offset = start;
                                    slice[offset .. offset + block_header.len()]
                                        .copy_from_slice(&block_header);
                                    offset += block_header.len();
                                    slice[offset .. offset + block_bytes.len()]
                                        .copy_from_slice(&block_bytes);
                                    offset += block_bytes.len();
                                    slice[offset .. offset + commit_tag.len()]
                                        .copy_from_slice(&commit_tag);
                                    offset += commit_tag.len();
                                    offset - start
                                },
                            };

                            match write_block.commit {
                                task::Commit::None =>
                                    (),
                                task::Commit::WithTerminator => {
                                    slice[start + written .. start + written + terminator_block_bytes.len()]
                                        .copy_from_slice(&terminator_block_bytes);
                                    // note: do not count terminator length in cursor in order to overwrite it during next write
                                }
                            }

                            cursor.set_position(start as u64 + written as u64);

                            let order = performer_sklave::Order::TaskDoneStats(
                                performer_sklave::OrderTaskDoneStats {
                                    task_done: task::Done {
                                        current_offset: cursor.position(),
                                        task: task::TaskDone {
                                            block_id: task.block_id,
                                            kind: task::TaskDoneKind::WriteBlock(task::TaskDoneWriteBlock {
                                                context: write_block.context,
                                            }),
                                        },
                                    },
                                    stats,
                                },
                            );
                            match performer_sklave_meister.befehl(order, &thread_pool) {
                                Ok(()) =>
                                    (),
                                Err(arbeitssklave::Error::Terminated) =>
                                    break 'outer,
                                Err(error) =>
                                    return Err(Error::Arbeitssklave(error).into()),
                            }
                        },

                        task::TaskKind::ReadBlock(task::ReadBlock {
                            block_header,
                            context,
                        }) => {
                            let total_chunk_size = storage_layout.data_size_block_min()
                                + block_header.block_size as usize;
                            let mut block_bytes = blocks_pool.lend();
                            block_bytes.resize(total_chunk_size, 0);
                            let start = cursor.position() as usize;
                            let slice = cursor.get_ref();
                            block_bytes.copy_from_slice(&slice[start .. start + total_chunk_size]);
                            cursor.set_position(start as u64 + block_bytes.len() as u64);

                            let order = performer_sklave::Order::TaskDoneStats(
                                performer_sklave::OrderTaskDoneStats {
                                    task_done: task::Done {
                                        current_offset: cursor.position(),
                                        task: task::TaskDone {
                                            block_id: block_header.block_id,
                                            kind: task::TaskDoneKind::ReadBlock(task::TaskDoneReadBlock {
                                                block_bytes,
                                                context,
                                            }),
                                        },
                                    },
                                    stats,
                                },
                            );
                            match performer_sklave_meister.befehl(order, &thread_pool) {
                                Ok(()) =>
                                    (),
                                Err(arbeitssklave::Error::Terminated) =>
                                    break 'outer,
                                Err(error) =>
                                    return Err(Error::Arbeitssklave(error).into()),
                            }
                        },

                        task::TaskKind::DeleteBlock(delete_block) => {
                            let start = cursor.position() as usize;
                            let slice = cursor.get_mut();
                            slice[start .. start + delete_block.delete_block_bytes.len()]
                                .copy_from_slice(&delete_block.delete_block_bytes);
                            let written = delete_block.delete_block_bytes.len();

                            match delete_block.commit {
                                task::Commit::None =>
                                    (),
                                task::Commit::WithTerminator => {
                                    slice[start + written .. start + written + terminator_block_bytes.len()]
                                        .copy_from_slice(&terminator_block_bytes);
                                    // note: do not count terminator length in cursor in order to overwrite it during next write
                                }
                            }

                            cursor.set_position(start as u64 + written as u64);

                            let order = performer_sklave::Order::TaskDoneStats(
                                performer_sklave::OrderTaskDoneStats {
                                    task_done: task::Done {
                                        current_offset: cursor.position(),
                                        task: task::TaskDone {
                                            block_id: task.block_id,
                                            kind: task::TaskDoneKind::DeleteBlock(task::TaskDoneDeleteBlock {
                                                context: delete_block.context,
                                            }),
                                        },
                                    },
                                    stats,
                                },
                            );
                            match performer_sklave_meister.befehl(order, &thread_pool) {
                                Ok(()) =>
                                    (),
                                Err(arbeitssklave::Error::Terminated) =>
                                    break 'outer,
                                Err(error) =>
                                    return Err(Error::Arbeitssklave(error).into()),
                            }
                        },
                    }
                },

                Order::DeviceSync { flush_context, } => {
                    let order = performer_sklave::Order::DeviceSyncDone(
                        performer_sklave::OrderDeviceSyncDone {
                            flush_context,
                        },
                    );
                    match performer_sklave_meister.befehl(order, &thread_pool) {
                        Ok(()) =>
                            (),
                        Err(arbeitssklave::Error::Terminated) =>
                            break 'outer,
                        Err(error) =>
                            return Err(Error::Arbeitssklave(error).into()),
                    }
                },

            }
        }
    }

    log::debug!("performer meister dropped in interpret_loop, shutting down");
    Ok(())
}
