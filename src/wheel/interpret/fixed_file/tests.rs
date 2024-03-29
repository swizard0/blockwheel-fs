use std::{
    fs,
    sync::{
        mpsc,
        Mutex,
    },
    path::{
        PathBuf,
    },
};

use alloc_pool::{
    bytes::{
        Bytes,
        BytesMut,
        BytesPool,
    }
};

use arbeitssklave::{
    ewig,
    komm,
};

use crate::{
    block,
    wheel::{
        lru,
        core::{
            task,
            schema,
            performer,
        },
        interpret::{
            self,
            fixed_file::{
                bootstrap,
                open,
                create,
            },
        },
        performer_sklave,
    },
    EchoPolicy,
    FixedFileInterpreterParams,
    Info,
    Deleted,
    Flushed,
    IterBlocks,
    IterBlocksItem,
    RequestWriteBlockError,
    RequestReadBlockError,
    RequestDeleteBlockError,
};

fn init() {
    let _ = env_logger::builder()
        .is_test(true)
        .try_init();
}

#[test]
fn create_read_empty() {
    init();
    let blocks_pool = BytesPool::new();
    let wheel_filename = "/tmp/blockwheel_create_read_empty";
    let _wheel_data =
        create::<LocalEchoPolicy>(
            &FixedFileInterpreterParams {
                wheel_filename: wheel_filename.into(),
                init_wheel_size_bytes: 256 * 1024,
            },
            performer::PerformerBuilderInit::new(
                lru::Cache::new(0),
                None,
                64 * 1024,
                &blocks_pool,
            ).unwrap(),
        )
        .unwrap();
    let _wheel_open_status =
        open::<LocalEchoPolicy>(
            &FixedFileInterpreterParams {
                wheel_filename: wheel_filename.into(),
                init_wheel_size_bytes: 256 * 1024,
            },
            performer::PerformerBuilderInit::new(
                lru::Cache::new(0),
                None,
                64 * 1024,
                &blocks_pool,
            ).unwrap(),
            &blocks_pool,
        )
        .unwrap();
    fs::remove_file(wheel_filename)
        .unwrap();
}

#[test]
fn create_read_one() {
    init();
    let edeltraud: edeltraud::Edeltraud<Job> = edeltraud::Builder::new()
        .build::<_, JobUnit<_>>()
        .unwrap();
    let thread_pool = edeltraud.handle();
    let (orders_tx, orders_rx) = mpsc::channel();
    let endpoint_meister = arbeitssklave::Freie::new()
        .versklaven(EndpointWelt { tag: "create_read_one", orders_tx: Mutex::new(orders_tx), }, &thread_pool)
        .unwrap();
    let sendegeraet =
        komm::Sendegeraet::starten(&endpoint_meister, thread_pool.clone());
    let forwarder_meister = arbeitssklave::Freie::new()
        .versklaven(ForwarderWelt { endpoint_meister: endpoint_meister.clone(), }, &thread_pool)
        .unwrap();

    let blocks_pool = BytesPool::new();
    let wheel_filename = "/tmp/blockwheel_create_read_one";
    fs::remove_file(wheel_filename).ok();

    let interpreter = make_interpreter(
        wheel_filename.into(),
        forwarder_meister.clone(),
        endpoint_meister.clone(),
        blocks_pool.clone(),
        thread_pool.clone(),
    );
    let performer = match orders_rx.recv() {
        Ok(Order::PerformerSklave(performer_sklave::Order::Bootstrap(performer_sklave::OrderBootstrap {
            performer,
        }))) =>
            performer,
        other_order =>
            panic!("unexpected order received: {other_order:?}"),
    };
    let schema = performer.decompose();

    let block_id = block::Id::init();
    let interpret::RunBlockPrepareWriteJobDone { write_block_bytes, } =
        interpret::run_block_prepare_write_job(
            block_id.clone(),
            hello_world_bytes(),
            blocks_pool.clone(),
        )
        .unwrap();
    let task = task::Task {
        block_id,
        kind: task::TaskKind::WriteBlock(task::WriteBlock {
            write_block_bytes,
            commit: task::Commit::WithTerminator,
            context: task::WriteBlockContext::External(sendegeraet.rueckkopplung(ReplyWriteBlock)),
        }),
    };
    interpreter.push_task(schema.storage_layout().wheel_header_size as u64, task)
        .unwrap();
    match orders_rx.recv() {
        Ok(Order::PerformerSklave(
            performer_sklave::Order::TaskDoneStats(
                performer_sklave::OrderTaskDoneStats {
                    task_done: task::Done {
                        task: task::TaskDone {
                            block_id,
                            kind: task::TaskDoneKind::WriteBlock(task::TaskDoneWriteBlock {
                                context: task::WriteBlockContext::External(rueckkopplung),
                            }),
                        },
                        ..
                    },
                    ..
                },
            ),
        )) if block_id == block::Id::init() => {
            rueckkopplung.commit(Ok(block_id)).unwrap();
        },
        other_order =>
            panic!("unexpected order received: {other_order:?}"),
    }
    match orders_rx.recv() {
        Ok(Order::Reply(OrderReply::WriteBlock(komm::Umschlag {
            inhalt: Ok(block_id),
            stamp: ReplyWriteBlock,
        }))) if block_id == block::Id::init() =>
            (),
        other_order =>
            panic!("unexpected order received: {other_order:?}"),
    }

    interpreter.device_sync(sendegeraet.rueckkopplung(ReplyFlush)).unwrap();
    match orders_rx.recv() {
        Ok(Order::PerformerSklave(
            performer_sklave::Order::DeviceSyncDone(
                performer_sklave::OrderDeviceSyncDone {
                    flush_context: rueckkopplung,
                },
            ),
        )) => {
            rueckkopplung.commit(Flushed).unwrap();
        },
        other_order =>
            panic!("unexpected order received: {other_order:?}"),
    }
    match orders_rx.recv() {
        Ok(Order::Reply(OrderReply::Flush(komm::Umschlag { inhalt: Flushed, stamp: ReplyFlush, }))) =>
            (),
        other_order =>
            panic!("unexpected order received: {other_order:?}"),
    }

    drop(interpreter);
    match orders_rx.recv() {
        Ok(Order::InterpreterError(interpret::Error::Ewig(arbeitssklave::ewig::Error::Terminated))) =>
            (),
        other_order =>
            panic!("unexpected order received: {other_order:?}"),
    }

    let interpreter = make_interpreter(
        wheel_filename.into(),
        forwarder_meister,
        endpoint_meister,
        blocks_pool,
        thread_pool,
    );
    let performer = match orders_rx.recv() {
        Ok(Order::PerformerSklave(performer_sklave::Order::Bootstrap(performer_sklave::OrderBootstrap {
            performer,
        }))) =>
            performer,
        other_order =>
            panic!("unexpected order received: {other_order:?}"),
    };
    let mut schema = performer.decompose();

    let block_id = block::Id::init();
    let expected_offset = schema.storage_layout().wheel_header_size as u64;
    let block_header = match schema.process_read_block_request(&block_id) {
        schema::ReadBlockOp::CacheHit(schema::ReadBlockCacheHit { .. }) =>
            panic!("unexpected cache hit for block_id = {block_id:?}"),
        schema::ReadBlockOp::Perform(schema::ReadBlockPerform { block_header, }) => {
            let task = task::Task {
                block_id: block_header.block_id.clone(),
                kind: task::TaskKind::ReadBlock(task::ReadBlock {
                    block_header: block_header.clone(),
                    context: task::ReadBlockContext::Process(task::ReadBlockProcessContext::External(
                        sendegeraet.rueckkopplung(ReplyReadBlock),
                    )),
                })
            };
            interpreter.push_task(expected_offset, task)
                .unwrap();
            match orders_rx.recv() {
                Ok(Order::PerformerSklave(
                    performer_sklave::Order::TaskDoneStats(
                        performer_sklave::OrderTaskDoneStats {
                            task_done: task::Done {
                                task: task::TaskDone {
                                    block_id,
                                    kind: task::TaskDoneKind::ReadBlock(task::TaskDoneReadBlock {
                                        block_bytes,
                                        context: task::ReadBlockContext::Process(task::ReadBlockProcessContext::External(
                                            rueckkopplung,
                                        )),
                                        ..
                                    }),
                                },
                                ..
                            },
                            ..
                        },
                    ),
                )) if block_id == block_header.block_id => {
                    rueckkopplung.commit(Ok(block_bytes.freeze())).unwrap();
                    block_header.clone()
                },
                other_order =>
                    panic!("unexpected order received: {other_order:?}"),
            }
        },
        schema::ReadBlockOp::NotFound =>
            panic!("unexpected read not found for block_id = {block_id:?}"),
    };
    let block_bytes = match orders_rx.recv() {
        Ok(Order::Reply(OrderReply::ReadBlock(komm::Umschlag {
            inhalt: Ok(block_bytes),
            stamp: ReplyReadBlock,
        }))) =>
            block_bytes,
        other_order =>
            panic!("unexpected order received: {other_order:?}"),
    };
    let interpret::RunBlockProcessReadJobDone { block_bytes, .. } =
        interpret::run_block_process_read_job(
            schema.storage_layout().clone(),
            block_header,
            block_bytes,
        )
        .unwrap();
    assert_eq!(block_bytes, hello_world_bytes());

    fs::remove_file(wheel_filename)
        .unwrap();
}

#[test]
fn create_write_overlap_read_one() {
    init();
    let edeltraud: edeltraud::Edeltraud<Job> = edeltraud::Builder::new()
        .build::<_, JobUnit<_>>()
        .unwrap();
    let thread_pool = edeltraud.handle();
    let (orders_tx, orders_rx) = mpsc::channel();
    let endpoint_meister = arbeitssklave::Freie::new()
        .versklaven(EndpointWelt { tag: "create_write_overlap_read_one", orders_tx: Mutex::new(orders_tx), }, &thread_pool)
        .unwrap();
    let sendegeraet =
        komm::Sendegeraet::starten(&endpoint_meister, thread_pool.clone());
    let forwarder_meister = arbeitssklave::Freie::new()
        .versklaven(ForwarderWelt { endpoint_meister: endpoint_meister.clone(), }, &thread_pool)
        .unwrap();

    let blocks_pool = BytesPool::new();
    let wheel_filename = "/tmp/create_write_overlap_read_one";

    fs::remove_file(wheel_filename).ok();

    let interpreter = make_interpreter(
        wheel_filename.into(),
        forwarder_meister.clone(),
        endpoint_meister.clone(),
        blocks_pool.clone(),
        thread_pool.clone(),
    );
    let performer = match orders_rx.recv() {
        Ok(Order::PerformerSklave(performer_sklave::Order::Bootstrap(performer_sklave::OrderBootstrap {
            performer,
        }))) =>
            performer,
        other_order =>
            panic!("unexpected order received: {other_order:?}"),
    };
    let schema = performer.decompose();

    // write first block
    let block_id = block::Id::init();
    let interpret::RunBlockPrepareWriteJobDone { write_block_bytes, } =
        interpret::run_block_prepare_write_job(
            block_id.clone(),
            hello_world_bytes(),
            blocks_pool.clone(),
        )
        .unwrap();
    let task = task::Task {
        block_id,
        kind: task::TaskKind::WriteBlock(task::WriteBlock {
            write_block_bytes,
            commit: task::Commit::WithTerminator,
            context: task::WriteBlockContext::External(sendegeraet.rueckkopplung(ReplyWriteBlock)),
        }),
    };
    interpreter.push_task(schema.storage_layout().wheel_header_size as u64, task)
        .unwrap();
    match orders_rx.recv() {
        Ok(Order::PerformerSklave(
            performer_sklave::Order::TaskDoneStats(
                performer_sklave::OrderTaskDoneStats {
                    task_done: task::Done {
                        task: task::TaskDone {
                            block_id,
                            kind: task::TaskDoneKind::WriteBlock(task::TaskDoneWriteBlock {
                                context: task::WriteBlockContext::External(rueckkopplung),
                            }),
                        },
                        ..
                    },
                    ..
                },
            ),
        )) if block_id == block::Id::init() => {
            rueckkopplung.commit(Ok(block_id)).unwrap();
        },
        other_order =>
            panic!("unexpected order received: {other_order:?}"),
    }
    match orders_rx.recv() {
        Ok(Order::Reply(OrderReply::WriteBlock(komm::Umschlag {
            inhalt: Ok(block_id),
            stamp: ReplyWriteBlock,
        }))) if block_id == block::Id::init() =>
            (),
        other_order =>
            panic!("unexpected order received: {other_order:?}"),
    }

    // partially overwrite first block with second
    let block_id = block::Id::init().next();
    let interpret::RunBlockPrepareWriteJobDone { write_block_bytes, } =
        interpret::run_block_prepare_write_job(
            block_id.clone(),
            hello_world_bytes(),
            blocks_pool.clone(),
        )
        .unwrap();
    let task = task::Task {
        block_id,
        kind: task::TaskKind::WriteBlock(task::WriteBlock {
            write_block_bytes,
            commit: task::Commit::WithTerminator,
            context: task::WriteBlockContext::External(sendegeraet.rueckkopplung(ReplyWriteBlock)),
        }),
    };
    interpreter.push_task(
        schema.storage_layout().wheel_header_size as u64
            + schema.storage_layout().block_header_size as u64,
        task,
    ).unwrap();
    match orders_rx.recv() {
        Ok(Order::PerformerSklave(
            performer_sklave::Order::TaskDoneStats(
                performer_sklave::OrderTaskDoneStats {
                    task_done: task::Done {
                        task: task::TaskDone {
                            block_id,
                            kind: task::TaskDoneKind::WriteBlock(task::TaskDoneWriteBlock {
                                context: task::WriteBlockContext::External(rueckkopplung),
                            }),
                        },
                        ..
                    },
                    ..
                },
            ),
        )) if block_id == block::Id::init().next() => {
            rueckkopplung.commit(Ok(block_id)).unwrap();
        },
        other_order =>
            panic!("unexpected order received: {other_order:?}"),
    }
    match orders_rx.recv() {
        Ok(Order::Reply(OrderReply::WriteBlock(komm::Umschlag {
            inhalt: Ok(block_id),
            stamp: ReplyWriteBlock,
        }))) if block_id == block::Id::init().next() =>
            (),
        other_order =>
            panic!("unexpected order received: {other_order:?}"),
    }

    // fsync
    interpreter.device_sync(sendegeraet.rueckkopplung(ReplyFlush)).unwrap();
    match orders_rx.recv() {
        Ok(Order::PerformerSklave(
            performer_sklave::Order::DeviceSyncDone(
                performer_sklave::OrderDeviceSyncDone {
                    flush_context: rueckkopplung,
                },
            ),
        )) => {
            rueckkopplung.commit(Flushed).unwrap();
        },
        other_order =>
            panic!("unexpected order received: {other_order:?}"),
    }
    match orders_rx.recv() {
        Ok(Order::Reply(OrderReply::Flush(komm::Umschlag { inhalt: Flushed, stamp: ReplyFlush, }))) =>
            (),
        other_order =>
            panic!("unexpected order received: {other_order:?}"),
    }

    // open existing and read
    drop(interpreter);
    match orders_rx.recv() {
        Ok(Order::InterpreterError(interpret::Error::Ewig(arbeitssklave::ewig::Error::Terminated))) =>
            (),
        other_order =>
            panic!("unexpected order received: {other_order:?}"),
    }

    let interpreter = make_interpreter(
        wheel_filename.into(),
        forwarder_meister,
        endpoint_meister,
        blocks_pool,
        thread_pool,
    );
    let performer = match orders_rx.recv() {
        Ok(Order::PerformerSklave(performer_sklave::Order::Bootstrap(performer_sklave::OrderBootstrap {
            performer,
        }))) =>
            performer,
        other_order =>
            panic!("unexpected order received: {other_order:?}"),
    };
    let mut schema = performer.decompose();

    let block_id = block::Id::init();
    match schema.process_read_block_request(&block_id) {
        schema::ReadBlockOp::CacheHit(schema::ReadBlockCacheHit { .. }) =>
            panic!("unexpected cache hit for block_id = {block_id:?}"),
        schema::ReadBlockOp::Perform(schema::ReadBlockPerform { .. }) =>
            panic!("unexpected read perform for block_id = {block_id:?}"),
        schema::ReadBlockOp::NotFound =>
            (),
    }
    let block_id = block_id.next();
    let expected_offset = schema.storage_layout().wheel_header_size as u64
        + schema.storage_layout().block_header_size as u64;
    let block_header = match schema.process_read_block_request(&block_id) {
        schema::ReadBlockOp::CacheHit(schema::ReadBlockCacheHit { .. }) =>
            panic!("unexpected cache hit for block_id = {block_id:?}"),
        schema::ReadBlockOp::Perform(schema::ReadBlockPerform { block_header, }) => {
            let task = task::Task {
                block_id: block_header.block_id.clone(),
                kind: task::TaskKind::ReadBlock(task::ReadBlock {
                    block_header: block_header.clone(),
                    context: task::ReadBlockContext::Process(task::ReadBlockProcessContext::External(
                        sendegeraet.rueckkopplung(ReplyReadBlock),
                    )),
                })
            };
            interpreter.push_task(expected_offset, task)
                .unwrap();
            match orders_rx.recv() {
                Ok(Order::PerformerSklave(
                    performer_sklave::Order::TaskDoneStats(
                        performer_sklave::OrderTaskDoneStats {
                            task_done: task::Done {
                                task: task::TaskDone {
                                    block_id,
                                    kind: task::TaskDoneKind::ReadBlock(task::TaskDoneReadBlock {
                                        block_bytes,
                                        context: task::ReadBlockContext::Process(task::ReadBlockProcessContext::External(
                                            rueckkopplung,
                                        )),
                                        ..
                                    }),
                                },
                                ..
                            },
                            ..
                        },
                    ),
                )) if block_id == block_header.block_id => {
                    rueckkopplung.commit(Ok(block_bytes.freeze())).unwrap();
                    block_header.clone()
                },
                other_order =>
                    panic!("unexpected order received: {other_order:?}"),
            }
        },
        schema::ReadBlockOp::NotFound =>
            panic!("unexpected read not found for block_id = {block_id:?}"),
    };
    let block_bytes = match orders_rx.recv() {
        Ok(Order::Reply(OrderReply::ReadBlock(komm::Umschlag {
            inhalt: Ok(block_bytes),
            stamp: ReplyReadBlock,
        }))) =>
            block_bytes,
        other_order =>
            panic!("unexpected order received: {other_order:?}"),
    };
    let interpret::RunBlockProcessReadJobDone { block_bytes, .. } =
        interpret::run_block_process_read_job(
            schema.storage_layout().clone(),
            block_header,
            block_bytes,
        )
        .unwrap();
    assert_eq!(block_bytes, hello_world_bytes());

    fs::remove_file(wheel_filename).unwrap();
}

#[test]
fn create_write_delete_read_one() {
    let edeltraud: edeltraud::Edeltraud<Job> = edeltraud::Builder::new()
        .build::<_, JobUnit<_>>()
        .unwrap();
    let thread_pool = edeltraud.handle();
    let (orders_tx, orders_rx) = mpsc::channel();
    let endpoint_meister = arbeitssklave::Freie::new()
        .versklaven(EndpointWelt { tag: "create_write_delete_read_one", orders_tx: Mutex::new(orders_tx), }, &thread_pool)
        .unwrap();
    let sendegeraet =
        komm::Sendegeraet::starten(&endpoint_meister, thread_pool.clone());
    let forwarder_meister = arbeitssklave::Freie::new()
        .versklaven(ForwarderWelt { endpoint_meister: endpoint_meister.clone(), }, &thread_pool)
        .unwrap();

    let blocks_pool = BytesPool::new();
    let wheel_filename = "/tmp/create_write_delete_read_one";

    fs::remove_file(wheel_filename).ok();

    let interpreter = make_interpreter(
        wheel_filename.into(),
        forwarder_meister.clone(),
        endpoint_meister.clone(),
        blocks_pool.clone(),
        thread_pool.clone(),
    );
    let performer = match orders_rx.recv() {
        Ok(Order::PerformerSklave(performer_sklave::Order::Bootstrap(performer_sklave::OrderBootstrap {
            performer,
        }))) =>
            performer,
        other_order =>
            panic!("unexpected order received: {other_order:?}"),
    };
    let schema = performer.decompose();

    // write first block
    let block_id = block::Id::init();
    let interpret::RunBlockPrepareWriteJobDone { write_block_bytes, } =
        interpret::run_block_prepare_write_job(
            block_id.clone(),
            hello_world_bytes(),
            blocks_pool.clone(),
        )
        .unwrap();
    let task = task::Task {
        block_id: block_id.clone(),
        kind: task::TaskKind::WriteBlock(task::WriteBlock {
            write_block_bytes,
            commit: task::Commit::WithTerminator,
            context: task::WriteBlockContext::External(sendegeraet.rueckkopplung(ReplyWriteBlock)),
        }),
    };
    interpreter.push_task(schema.storage_layout().wheel_header_size as u64, task)
        .unwrap();
    let current_offset = match orders_rx.recv() {
        Ok(Order::PerformerSklave(
            performer_sklave::Order::TaskDoneStats(
                performer_sklave::OrderTaskDoneStats {
                    task_done: task::Done {
                        current_offset,
                        task: task::TaskDone {
                            block_id,
                            kind: task::TaskDoneKind::WriteBlock(task::TaskDoneWriteBlock {
                                context: task::WriteBlockContext::External(rueckkopplung),
                            }),
                        },
                        ..
                    },
                    ..
                },
            ),
        )) if block_id == block::Id::init() => {
            rueckkopplung.commit(Ok(block_id)).unwrap();
            current_offset
        },
        other_order =>
            panic!("unexpected order received: {other_order:?}"),
    };
    match orders_rx.recv() {
        Ok(Order::Reply(OrderReply::WriteBlock(komm::Umschlag {
            inhalt: Ok(block_id),
            stamp: ReplyWriteBlock,
        }))) if block_id == block::Id::init() =>
            (),
        other_order =>
            panic!("unexpected order received: {other_order:?}"),
    }

    // write second block
    let block_id = block_id.next();
    let interpret::RunBlockPrepareWriteJobDone { write_block_bytes, } =
        interpret::run_block_prepare_write_job(
            block_id.clone(),
            hello_world_bytes(),
            blocks_pool.clone(),
        )
        .unwrap();
    let task = task::Task {
        block_id,
        kind: task::TaskKind::WriteBlock(task::WriteBlock {
            write_block_bytes,
            commit: task::Commit::WithTerminator,
            context: task::WriteBlockContext::External(sendegeraet.rueckkopplung(ReplyWriteBlock)),
        }),
    };
    interpreter.push_task(current_offset, task)
        .unwrap();
    match orders_rx.recv() {
        Ok(Order::PerformerSklave(
            performer_sklave::Order::TaskDoneStats(
                performer_sklave::OrderTaskDoneStats {
                    task_done: task::Done {
                        task: task::TaskDone {
                            block_id,
                            kind: task::TaskDoneKind::WriteBlock(task::TaskDoneWriteBlock {
                                context: task::WriteBlockContext::External(rueckkopplung),
                            }),
                        },
                        ..
                    },
                    ..
                },
            ),
        )) if block_id == block::Id::init().next() => {
            rueckkopplung.commit(Ok(block_id)).unwrap();
        },
        other_order =>
            panic!("unexpected order received: {other_order:?}"),
    }
    match orders_rx.recv() {
        Ok(Order::Reply(OrderReply::WriteBlock(komm::Umschlag {
            inhalt: Ok(block_id),
            stamp: ReplyWriteBlock,
        }))) if block_id == block::Id::init().next() =>
            (),
        other_order =>
            panic!("unexpected order received: {other_order:?}"),
    }

    // delete first block
    let block_id = block::Id::init();
    let interpret::RunBlockPrepareDeleteJobDone { delete_block_bytes, } =
        interpret::run_block_prepare_delete_job(
            blocks_pool.clone(),
        )
        .unwrap();
    let task = task::Task {
        block_id: block_id.clone(),
        kind: task::TaskKind::DeleteBlock(task::DeleteBlock {
            delete_block_bytes: delete_block_bytes.freeze(),
            commit: task::Commit::None,
            context: task::DeleteBlockContext::External(sendegeraet.rueckkopplung(ReplyDeleteBlock)),
        }),
    };
    interpreter.push_task(schema.storage_layout().wheel_header_size as u64, task)
        .unwrap();
    match orders_rx.recv() {
        Ok(Order::PerformerSklave(
            performer_sklave::Order::TaskDoneStats(
                performer_sklave::OrderTaskDoneStats {
                    task_done: task::Done {
                        task: task::TaskDone {
                            block_id,
                            kind: task::TaskDoneKind::DeleteBlock(task::TaskDoneDeleteBlock {
                                context: task::DeleteBlockContext::External(rueckkopplung),
                            }),
                        },
                        ..
                    },
                    ..
                },
            ),
        )) if block_id == block::Id::init() => {
            rueckkopplung.commit(Ok(Deleted)).unwrap();
        },
        other_order =>
            panic!("unexpected order received: {other_order:?}"),
    }
    match orders_rx.recv() {
        Ok(Order::Reply(OrderReply::DeleteBlock(komm::Umschlag {
            inhalt: Ok(Deleted),
            stamp: ReplyDeleteBlock,
        }))) if block_id == block::Id::init() =>
            (),
        other_order =>
            panic!("unexpected order received: {other_order:?}"),
    }

    // fsync
    interpreter.device_sync(sendegeraet.rueckkopplung(ReplyFlush)).unwrap();
    match orders_rx.recv() {
        Ok(Order::PerformerSklave(
            performer_sklave::Order::DeviceSyncDone(
                performer_sklave::OrderDeviceSyncDone {
                    flush_context: rueckkopplung,
                },
            ),
        )) => {
            rueckkopplung.commit(Flushed).unwrap();
        },
        other_order =>
            panic!("unexpected order received: {other_order:?}"),
    }
    match orders_rx.recv() {
        Ok(Order::Reply(OrderReply::Flush(komm::Umschlag { inhalt: Flushed, stamp: ReplyFlush, }))) =>
            (),
        other_order =>
            panic!("unexpected order received: {other_order:?}"),
    }

    // open existing and read
    drop(interpreter);
    match orders_rx.recv() {
        Ok(Order::InterpreterError(interpret::Error::Ewig(arbeitssklave::ewig::Error::Terminated))) =>
            (),
        other_order =>
            panic!("unexpected order received: {other_order:?}"),
    }

    let interpreter = make_interpreter(
        wheel_filename.into(),
        forwarder_meister,
        endpoint_meister,
        blocks_pool,
        thread_pool,
    );
    let performer = match orders_rx.recv() {
        Ok(Order::PerformerSklave(performer_sklave::Order::Bootstrap(performer_sklave::OrderBootstrap {
            performer,
        }))) =>
            performer,
        other_order =>
            panic!("unexpected order received: {other_order:?}"),
    };
    let mut schema = performer.decompose();

    let block_id = block::Id::init();
    match schema.process_read_block_request(&block_id) {
        schema::ReadBlockOp::CacheHit(schema::ReadBlockCacheHit { .. }) =>
            panic!("unexpected cache hit for block_id = {block_id:?}"),
        schema::ReadBlockOp::Perform(schema::ReadBlockPerform { .. }) =>
            panic!("unexpected read perform for block_id = {block_id:?}"),
        schema::ReadBlockOp::NotFound =>
            (),
    }
    let block_id = block_id.next();
    let expected_offset = schema.storage_layout().wheel_header_size as u64
        + schema.storage_layout().data_size_block_min() as u64
        + hello_world_bytes().len() as u64;

    let block_header = match schema.process_read_block_request(&block_id) {
        schema::ReadBlockOp::CacheHit(schema::ReadBlockCacheHit { .. }) =>
            panic!("unexpected cache hit for block_id = {block_id:?}"),
        schema::ReadBlockOp::Perform(schema::ReadBlockPerform { block_header, }) => {
            let task = task::Task {
                block_id: block_header.block_id.clone(),
                kind: task::TaskKind::ReadBlock(task::ReadBlock {
                    block_header: block_header.clone(),
                    context: task::ReadBlockContext::Process(task::ReadBlockProcessContext::External(
                        sendegeraet.rueckkopplung(ReplyReadBlock),
                    )),
                })
            };
            interpreter.push_task(expected_offset, task)
                .unwrap();
            match orders_rx.recv() {
                Ok(Order::PerformerSklave(
                    performer_sklave::Order::TaskDoneStats(
                        performer_sklave::OrderTaskDoneStats {
                            task_done: task::Done {
                                task: task::TaskDone {
                                    block_id,
                                    kind: task::TaskDoneKind::ReadBlock(task::TaskDoneReadBlock {
                                        block_bytes,
                                        context: task::ReadBlockContext::Process(task::ReadBlockProcessContext::External(
                                            rueckkopplung,
                                        )),
                                        ..
                                    }),
                                },
                                ..
                            },
                            ..
                        },
                    ),
                )) if block_id == block_header.block_id => {
                    rueckkopplung.commit(Ok(block_bytes.freeze())).unwrap();
                    block_header.clone()
                },
                other_order =>
                    panic!("unexpected order received: {other_order:?}"),
            }
        },
        schema::ReadBlockOp::NotFound =>
            panic!("unexpected read not found for block_id = {block_id:?}"),
    };
    let block_bytes = match orders_rx.recv() {
        Ok(Order::Reply(OrderReply::ReadBlock(komm::Umschlag {
            inhalt: Ok(block_bytes),
            stamp: ReplyReadBlock,
        }))) =>
            block_bytes,
        other_order =>
            panic!("unexpected order received: {other_order:?}"),
    };
    let interpret::RunBlockProcessReadJobDone { block_bytes, .. } =
        interpret::run_block_process_read_job(
            schema.storage_layout().clone(),
            block_header,
            block_bytes,
        )
        .unwrap();
    assert_eq!(block_bytes, hello_world_bytes());

    fs::remove_file(wheel_filename).unwrap();
}

#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
enum Order {
    PerformerSklave(performer_sklave::Order<LocalEchoPolicy>),
    Reply(OrderReply),
    InterpreterError(interpret::Error),
}

impl From<performer_sklave::Order<LocalEchoPolicy>> for Order {
    fn from(order: performer_sklave::Order<LocalEchoPolicy>) -> Order {
        Order::PerformerSklave(order)
    }
}

#[derive(Debug)]
enum OrderReply {
    InfoCancel(komm::UmschlagAbbrechen<ReplyInfo>),
    Info(komm::Umschlag<Info, ReplyInfo>),
    FlushCancel(komm::UmschlagAbbrechen<ReplyFlush>),
    Flush(komm::Umschlag<Flushed, ReplyFlush>),
    WriteBlockCancel(komm::UmschlagAbbrechen<ReplyWriteBlock>),
    WriteBlock(komm::Umschlag<Result<block::Id, RequestWriteBlockError>, ReplyWriteBlock>),
    ReadBlockCancel(komm::UmschlagAbbrechen<ReplyReadBlock>),
    ReadBlock(komm::Umschlag<Result<Bytes, RequestReadBlockError>, ReplyReadBlock>),
    DeleteBlockCancel(komm::UmschlagAbbrechen<ReplyDeleteBlock>),
    DeleteBlock(komm::Umschlag<Result<Deleted, RequestDeleteBlockError>, ReplyDeleteBlock>),
    IterBlocksInitCancel(komm::UmschlagAbbrechen<ReplyIterBlocksInit>),
    IterBlocksInit(komm::Umschlag<IterBlocks, ReplyIterBlocksInit>),
    IterBlocksNextCancel(komm::UmschlagAbbrechen<ReplyIterBlocksNext>),
    IterBlocksNext(komm::Umschlag<IterBlocksItem, ReplyIterBlocksNext>),
}

#[derive(Debug)]
struct ReplyInfo;
#[derive(Debug)]
struct ReplyFlush;
#[derive(Debug)]
struct ReplyWriteBlock;
#[derive(Debug)]
struct ReplyReadBlock;
#[derive(Debug)]
struct ReplyDeleteBlock;
#[derive(Debug)]
struct ReplyIterBlocksInit;
#[derive(Debug)]
struct ReplyIterBlocksNext;

impl From<komm::UmschlagAbbrechen<ReplyInfo>> for Order {
    fn from(v: komm::UmschlagAbbrechen<ReplyInfo>) -> Order {
        Order::Reply(OrderReply::InfoCancel(v))
    }
}

impl From<komm::Umschlag<Info, ReplyInfo>> for Order {
    fn from(v: komm::Umschlag<Info, ReplyInfo>) -> Order {
        Order::Reply(OrderReply::Info(v))
    }
}

impl From<komm::UmschlagAbbrechen<ReplyFlush>> for Order {
    fn from(v: komm::UmschlagAbbrechen<ReplyFlush>) -> Order {
        Order::Reply(OrderReply::FlushCancel(v))
    }
}

impl From<komm::Umschlag<Flushed, ReplyFlush>> for Order {
    fn from(v: komm::Umschlag<Flushed, ReplyFlush>) -> Order {
        Order::Reply(OrderReply::Flush(v))
    }
}

impl From<komm::UmschlagAbbrechen<ReplyWriteBlock>> for Order {
    fn from(v: komm::UmschlagAbbrechen<ReplyWriteBlock>) -> Order {
        Order::Reply(OrderReply::WriteBlockCancel(v))
    }
}

impl From<komm::Umschlag<Result<block::Id, RequestWriteBlockError>, ReplyWriteBlock>> for Order {
    fn from(v: komm::Umschlag<Result<block::Id, RequestWriteBlockError>, ReplyWriteBlock>) -> Order {
        Order::Reply(OrderReply::WriteBlock(v))
    }
}

impl From<komm::UmschlagAbbrechen<ReplyReadBlock>> for Order {
    fn from(v: komm::UmschlagAbbrechen<ReplyReadBlock>) -> Order {
        Order::Reply(OrderReply::ReadBlockCancel(v))
    }
}

impl From<komm::Umschlag<Result<Bytes, RequestReadBlockError>, ReplyReadBlock>> for Order {
    fn from(v: komm::Umschlag<Result<Bytes, RequestReadBlockError>, ReplyReadBlock>) -> Order {
        Order::Reply(OrderReply::ReadBlock(v))
    }
}

impl From<komm::UmschlagAbbrechen<ReplyDeleteBlock>> for Order {
    fn from(v: komm::UmschlagAbbrechen<ReplyDeleteBlock>) -> Order {
        Order::Reply(OrderReply::DeleteBlockCancel(v))
    }
}

impl From<komm::Umschlag<Result<Deleted, RequestDeleteBlockError>, ReplyDeleteBlock>> for Order {
    fn from(v: komm::Umschlag<Result<Deleted, RequestDeleteBlockError>, ReplyDeleteBlock>) -> Order {
        Order::Reply(OrderReply::DeleteBlock(v))
    }
}

impl From<komm::UmschlagAbbrechen<ReplyIterBlocksInit>> for Order {
    fn from(v: komm::UmschlagAbbrechen<ReplyIterBlocksInit>) -> Order {
        Order::Reply(OrderReply::IterBlocksInitCancel(v))
    }
}

impl From<komm::Umschlag<IterBlocks, ReplyIterBlocksInit>> for Order {
    fn from(v: komm::Umschlag<IterBlocks, ReplyIterBlocksInit>) -> Order {
        Order::Reply(OrderReply::IterBlocksInit(v))
    }
}

impl From<komm::UmschlagAbbrechen<ReplyIterBlocksNext>> for Order {
    fn from(v: komm::UmschlagAbbrechen<ReplyIterBlocksNext>) -> Order {
        Order::Reply(OrderReply::IterBlocksNextCancel(v))
    }
}

impl From<komm::Umschlag<IterBlocksItem, ReplyIterBlocksNext>> for Order {
    fn from(v: komm::Umschlag<IterBlocksItem, ReplyIterBlocksNext>) -> Order {
        Order::Reply(OrderReply::IterBlocksNext(v))
    }
}

struct EndpointWelt {
    tag: &'static str,
    orders_tx: Mutex<mpsc::Sender<Order>>,
}

struct ForwarderWelt {
    endpoint_meister: arbeitssklave::Meister<EndpointWelt, Order>,
}

enum Job {
    ForwarderSklave(arbeitssklave::SklaveJob<ForwarderWelt, performer_sklave::Order<LocalEchoPolicy>>),
    EndpointSklave(arbeitssklave::SklaveJob<EndpointWelt, Order>),
}

impl From<arbeitssklave::SklaveJob<ForwarderWelt, performer_sklave::Order<LocalEchoPolicy>>> for Job {
    fn from(sklave_job: arbeitssklave::SklaveJob<ForwarderWelt, performer_sklave::Order<LocalEchoPolicy>>) -> Job {
        Job::ForwarderSklave(sklave_job)
    }
}

impl From<arbeitssklave::SklaveJob<EndpointWelt, Order>> for Job {
    fn from(sklave_job: arbeitssklave::SklaveJob<EndpointWelt, Order>) -> Job {
        Job::EndpointSklave(sklave_job)
    }
}

pub struct JobUnit<J>(edeltraud::JobUnit<J, Job>);

impl<J> From<edeltraud::JobUnit<J, Job>> for JobUnit<J> {
    fn from(job_unit: edeltraud::JobUnit<J, Job>) -> Self {
        Self(job_unit)
    }
}

impl<J> edeltraud::Job for JobUnit<J>
where J: From<arbeitssklave::SklaveJob<EndpointWelt, Order>>,
{
    fn run(self) {
        match self.0.job {
            Job::ForwarderSklave(mut sklave_job) => {
                #[allow(clippy::while_let_loop)]
                loop {
                    match sklave_job.zu_ihren_diensten().unwrap() {
                        arbeitssklave::Gehorsam::Machen { mut befehle, } =>
                            loop {
                                match befehle.befehl() {
                                    arbeitssklave::SklavenBefehl::Mehr {
                                        befehl,
                                        mehr_befehle,
                                    } => {
                                        befehle = mehr_befehle;
                                        befehle.endpoint_meister.befehl(befehl.into(), &self.0.handle).unwrap();
                                    },
                                    arbeitssklave::SklavenBefehl::Ende {
                                        sklave_job: next_sklave_job,
                                    } => {
                                        sklave_job = next_sklave_job;
                                        break;
                                    },
                                }
                            },
                        arbeitssklave::Gehorsam::Rasten =>
                            break,
                    }
                }
            },
            Job::EndpointSklave(mut sklave_job) => {
                #[allow(clippy::while_let_loop)]
                loop {
                    match sklave_job.zu_ihren_diensten() {
                        Ok(arbeitssklave::Gehorsam::Machen { mut befehle, }) =>
                            loop {
                                match befehle.befehl() {
                                    arbeitssklave::SklavenBefehl::Mehr { befehl, mehr_befehle, } => {
                                        befehle = mehr_befehle;
                                        let Ok(tx_lock) = befehle.orders_tx.lock() else {
                                            log::error!("failed to lock mutex in EndpointSklave/{} job, terminating", befehle.tag);
                                            return;
                                        };
                                        if let Err(mpsc::SendError(befehl)) = tx_lock.send(befehl) {
                                            log::warn!("failed to send back order {befehl:?} in EndpointSklave/{} job, terminating", befehle.tag);
                                            return;
                                        }
                                    },
                                    arbeitssklave::SklavenBefehl::Ende { sklave_job: next_sklave_job, } => {
                                        sklave_job = next_sklave_job;
                                        break;
                                    },
                                }
                            },
                        Ok(arbeitssklave::Gehorsam::Rasten) =>
                            break,
                        Err(error) => {
                            log::info!("EndpointSklave::zu_ihren_diensten terminated with {error:?}");
                            break;
                        },
                    }
                }
            },
        }
    }
}

struct LocalEchoPolicy;

impl EchoPolicy for LocalEchoPolicy {
    type Info = komm::Rueckkopplung<Order, ReplyInfo>;
    type Flush = komm::Rueckkopplung<Order, ReplyFlush>;
    type WriteBlock = komm::Rueckkopplung<Order, ReplyWriteBlock>;
    type ReadBlock = komm::Rueckkopplung<Order, ReplyReadBlock>;
    type DeleteBlock = komm::Rueckkopplung<Order, ReplyDeleteBlock>;
    type IterBlocksInit = komm::Rueckkopplung<Order, ReplyIterBlocksInit>;
    type IterBlocksNext = komm::Rueckkopplung<Order, ReplyIterBlocksNext>;
}

fn hello_world_bytes() -> Bytes {
    let mut block_bytes_mut = BytesMut::new_detached(Vec::new());
    block_bytes_mut.extend("hello, world!".as_bytes().iter().cloned());
    block_bytes_mut.freeze()
}

fn make_interpreter<J>(
    wheel_filename: PathBuf,
    forwarder_meister: arbeitssklave::Meister<ForwarderWelt, performer_sklave::Order<LocalEchoPolicy>>,
    endpoint_meister: arbeitssklave::Meister<EndpointWelt, Order>,
    blocks_pool: BytesPool,
    thread_pool: edeltraud::Handle<J>,
)
    -> interpret::Interpreter<LocalEchoPolicy>
where J: From<arbeitssklave::SklaveJob<ForwarderWelt, performer_sklave::Order<LocalEchoPolicy>>>,
      J: From<arbeitssklave::SklaveJob<EndpointWelt, Order>>,
      J: Send + 'static,
{
    let interpreter_meister = ewig::Freie::new()
        .versklaven_als(
            "blockwheel_fs::wheel::interpret::fixed_file".to_string(),
            move |sklave| {
                let interpreter_result = bootstrap(
                    sklave,
                    FixedFileInterpreterParams {
                        wheel_filename,
                        init_wheel_size_bytes: 256 * 1024,
                    },
                    performer::PerformerBuilderInit::new(
                        lru::Cache::new(0),
                        None,
                        64 * 1024,
                        &blocks_pool,
                    ).unwrap(),
                    forwarder_meister,
                    blocks_pool,
                    thread_pool.clone(),
                );
                if let Err(error) = interpreter_result {
                    endpoint_meister.befehl(Order::InterpreterError(error), &thread_pool).ok();
                }
                Ok(())
            },
        )
        .unwrap();
    interpret::Interpreter { interpreter_meister, }
}
