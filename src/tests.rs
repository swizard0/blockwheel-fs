use std::{
    fs,
    sync::{
        atomic::{
            Ordering,
        },
    },
};

use crate::{
    stress,
    Params,
    InterpreterParams,
    RamInterpreterParams,
    DummyInterpreterParams,
    FixedFileInterpreterParams,
};

#[test]
fn stress_fixed_file() {
    init();

    let wheel_filename = "/tmp/blockwheel_stress";
    let work_block_size_bytes = 16 * 1024;
    let init_wheel_size_bytes = 1 * 1024 * 1024;

    let params = Params {
        interpreter: InterpreterParams::FixedFile(FixedFileInterpreterParams {
            wheel_filename: wheel_filename.into(),
            init_wheel_size_bytes,
        }),
        work_block_size_bytes,
        lru_cache_size_bytes: 0,
        defrag_parallel_tasks_limit: 8,
    };

    let limits = stress::Limits {
        active_tasks: 128,
        actions: 1024,
        block_size_bytes: work_block_size_bytes - 256,
    };

    let mut counter = stress::Counter::default();
    let mut blocks = Vec::new();

    // first fill wheel from scratch
    fs::remove_file(wheel_filename).ok();
    stress::stress_loop(params.clone(), &mut blocks, &mut counter, &limits).unwrap();

    assert_eq!(counter.reads + counter.writes + counter.deletes, limits.actions);

    log::info!("fixed_file scratch JOB_BLOCK_PREPARE_WRITE: {}", crate::job::JOB_BLOCK_PREPARE_WRITE.load(Ordering::SeqCst));
    log::info!("fixed_file scratch JOB_BLOCK_PROCESS_READ: {}", crate::job::JOB_BLOCK_PROCESS_READ.load(Ordering::SeqCst));
    log::info!("fixed_file scratch JOB_BLOCK_PREPARE_DELETE: {}", crate::job::JOB_BLOCK_PREPARE_DELETE.load(Ordering::SeqCst));
    log::info!("fixed_file scratch JOB_PERFORMER_SKLAVE: {}", crate::job::JOB_PERFORMER_SKLAVE.load(Ordering::SeqCst));

    // next load existing wheel and repeat stress with blocks
    counter.clear();
    stress::stress_loop(params, &mut blocks, &mut counter, &limits).unwrap();

    assert_eq!(counter.reads + counter.writes + counter.deletes, limits.actions);

    log::info!("fixed_file existing JOB_BLOCK_PREPARE_WRITE: {}", crate::job::JOB_BLOCK_PREPARE_WRITE.load(Ordering::SeqCst));
    log::info!("fixed_file existing JOB_BLOCK_PROCESS_READ: {}", crate::job::JOB_BLOCK_PROCESS_READ.load(Ordering::SeqCst));
    log::info!("fixed_file existing JOB_BLOCK_PREPARE_DELETE: {}", crate::job::JOB_BLOCK_PREPARE_DELETE.load(Ordering::SeqCst));
    log::info!("fixed_file existing JOB_PERFORMER_SKLAVE: {}", crate::job::JOB_PERFORMER_SKLAVE.load(Ordering::SeqCst));

    fs::remove_file(wheel_filename).ok();
}

#[test]
fn stress_ram() {
    init();

    let work_block_size_bytes = 16 * 1024;
    let init_wheel_size_bytes = 1024 * 1024;

    let params = Params {
        interpreter: InterpreterParams::Ram(RamInterpreterParams {
            init_wheel_size_bytes,
        }),
        work_block_size_bytes,
        lru_cache_size_bytes: 0,
        defrag_parallel_tasks_limit: 8,
    };

    let limits = stress::Limits {
        active_tasks: 192,
        actions: 1536,
        block_size_bytes: work_block_size_bytes - 256,
    };

    let mut counter = stress::Counter::default();
    let mut blocks = Vec::new();

    // fill wheel from scratch
    stress::stress_loop(params, &mut blocks, &mut counter, &limits).unwrap();

    assert_eq!(counter.reads + counter.writes + counter.deletes, limits.actions);

    log::info!("ram JOB_BLOCK_PREPARE_WRITE: {}", crate::job::JOB_BLOCK_PREPARE_WRITE.load(Ordering::SeqCst));
    log::info!("ram JOB_BLOCK_PROCESS_READ: {}", crate::job::JOB_BLOCK_PROCESS_READ.load(Ordering::SeqCst));
    log::info!("ram JOB_BLOCK_PREPARE_DELETE: {}", crate::job::JOB_BLOCK_PREPARE_DELETE.load(Ordering::SeqCst));
    log::info!("ram JOB_PERFORMER_SKLAVE: {}", crate::job::JOB_PERFORMER_SKLAVE.load(Ordering::SeqCst));
}

#[test]
fn stress_dummy() {
    init();

    let work_block_size_bytes = 16 * 1024;
    let init_wheel_size_bytes = 1024 * 1024;

    let params = Params {
        interpreter: InterpreterParams::Dummy(DummyInterpreterParams {
            init_wheel_size_bytes,
        }),
        work_block_size_bytes,
        lru_cache_size_bytes: 0,
        defrag_parallel_tasks_limit: 8,
    };

    let limits = stress::Limits {
        active_tasks: 192,
        actions: 1536,
        block_size_bytes: work_block_size_bytes - 256,
    };

    let mut counter = stress::Counter::default();
    let mut blocks = Vec::new();

    // fill wheel from scratch
    stress::stress_loop(params, &mut blocks, &mut counter, &limits).unwrap();

    assert_eq!(counter.reads + counter.writes + counter.deletes, limits.actions);

    log::info!("dummy JOB_BLOCK_PREPARE_WRITE: {}", crate::job::JOB_BLOCK_PREPARE_WRITE.load(Ordering::SeqCst));
    log::info!("dummy JOB_BLOCK_PROCESS_READ: {}", crate::job::JOB_BLOCK_PROCESS_READ.load(Ordering::SeqCst));
    log::info!("dummy JOB_BLOCK_PREPARE_DELETE: {}", crate::job::JOB_BLOCK_PREPARE_DELETE.load(Ordering::SeqCst));
    log::info!("dummy JOB_PERFORMER_SKLAVE: {}", crate::job::JOB_PERFORMER_SKLAVE.load(Ordering::SeqCst));
}

fn init() {
    let _ = env_logger::builder()
        .is_test(true)
        .try_init();
}
