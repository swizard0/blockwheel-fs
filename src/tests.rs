use std::{
    fs,
};

use crate::{
    stress,
    Params,
    InterpreterParams,
    RamInterpreterParams,
    FixedFileInterpreterParams,
};

#[test]
fn stress_fixed_file() {
    env_logger::init();

    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
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
        ..Default::default()
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
    runtime.block_on(stress::stress_loop(params.clone(), &mut blocks, &mut counter, &limits)).unwrap();

    assert_eq!(counter.reads + counter.writes + counter.deletes, limits.actions);

    // next load existing wheel and repeat stress with blocks
    counter.clear();
    runtime.block_on(stress::stress_loop(params.clone(), &mut blocks, &mut counter, &limits)).unwrap();

    assert_eq!(counter.reads + counter.writes + counter.deletes, limits.actions);

    fs::remove_file(wheel_filename).ok();
}

#[test]
fn stress_ram() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let work_block_size_bytes = 16 * 1024;
    let init_wheel_size_bytes = 1 * 1024 * 1024;

    let params = Params {
        interpreter: InterpreterParams::Ram(RamInterpreterParams {
            init_wheel_size_bytes,
        }),
        work_block_size_bytes,
        lru_cache_size_bytes: 0,
        defrag_parallel_tasks_limit: 8,
        ..Default::default()
    };

    let limits = stress::Limits {
        active_tasks: 192,
        actions: 1536,
        block_size_bytes: work_block_size_bytes - 256,
    };

    let mut counter = stress::Counter::default();
    let mut blocks = Vec::new();

    // fill wheel from scratch
    runtime.block_on(stress::stress_loop(params.clone(), &mut blocks, &mut counter, &limits)).unwrap();

    assert_eq!(counter.reads + counter.writes + counter.deletes, limits.actions);
}
