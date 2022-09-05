use clap::{
    Parser,
};

#[derive(Parser, Debug)]
struct CliArgs {
    /// Filename for blockwheel data
    #[clap(short = 'w', long, default_value = "wheel")]
    wheel_filename: String,
    /// Initial wheel size when creating new file (in bytes)
    #[clap(short = 's', long, default_value = "67108864")]
    init_wheel_size_bytes: usize,
    /// work io buffer size (in bytes)
    #[clap(long, default_value = "8388608")]
    work_block_size_bytes: usize,
    /// lru cache size (in bytes)
    #[clap(long, default_value = "0")]
    lru_cache_size_bytes: usize,
    /// parallel background defragmentation tasks count
    #[clap(long, default_value = "4")]
    defrag_parallel_tasks_limit: usize,
    /// stress parallel tasks count
    #[clap(long, default_value = "128")]
    stress_active_tasks_count: usize,
    /// total number of stress actions
    #[clap(long, default_value = "1024")]
    stress_actions_count: usize,
}

fn main() {
    pretty_env_logger::init_timed();
    let cli_args = CliArgs::parse();

    let params = blockwheel_fs::Params {
        interpreter: blockwheel_fs::InterpreterParams::FixedFile(
            blockwheel_fs::FixedFileInterpreterParams {
                wheel_filename: cli_args.wheel_filename.clone().into(),
                init_wheel_size_bytes: cli_args.init_wheel_size_bytes,
            },
        ),
        work_block_size_bytes: cli_args.work_block_size_bytes,
        lru_cache_size_bytes: cli_args.lru_cache_size_bytes,
        defrag_parallel_tasks_limit: cli_args.defrag_parallel_tasks_limit,
        ..Default::default()
    };

    let limits = blockwheel_fs::stress::Limits {
        active_tasks: cli_args.stress_active_tasks_count,
        actions: cli_args.stress_actions_count,
        block_size_bytes: cli_args.work_block_size_bytes - 256,
    };

    std::fs::remove_file(&cli_args.wheel_filename).ok();

    let mut blocks = Vec::new();
    let mut counter = blockwheel_fs::stress::Counter::default();
    let stress_result = blockwheel_fs::stress::stress_loop(params, &mut blocks, &mut counter, &limits);
    match stress_result {
        Ok(()) => {
            log::info!("stress task done: counters = {counter:?}");
            log::info!(" JOB_BLOCK_PREPARE_WRITE: {}", blockwheel_fs::job::JOB_BLOCK_PREPARE_WRITE.load(std::sync::atomic::Ordering::SeqCst));
            log::info!(" JOB_BLOCK_PROCESS_READ: {}", blockwheel_fs::job::JOB_BLOCK_PROCESS_READ.load(std::sync::atomic::Ordering::SeqCst));
            log::info!(" JOB_BLOCK_PREPARE_DELETE: {}", blockwheel_fs::job::JOB_BLOCK_PREPARE_DELETE.load(std::sync::atomic::Ordering::SeqCst));
            log::info!(" JOB_PERFORMER_SKLAVE: {}", blockwheel_fs::job::JOB_PERFORMER_SKLAVE.load(std::sync::atomic::Ordering::SeqCst));
        },
        Err(error) => {
            log::error!("stress task error: {error:?}");
        },
    }
}
