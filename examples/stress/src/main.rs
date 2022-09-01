use clap::{
    Parser,
};

use ero::{
    supervisor::{
        SupervisorGenServer,
    },
};

#[derive(Parser, Debug)]
struct CliArgs {
    /// Filename for blockwheel data
    #[clap(short = 'w', long, default_value = "wheel")]
    wheel_filename: String,
    /// Initial wheel size when creating new file (in bytes)
    #[clap(short = 's', long, default_value = "67108864")]
    init_wheel_size_bytes: usize,
    /// wheel ero task restart holdon (in seconds)
    #[clap(long, default_value = "4")]
    wheel_task_restart_sec: usize,
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

#[tokio::main]
async fn main() {
    pretty_env_logger::init();
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

    log::debug!("creating supervisor");
    let supervisor_gen_server = SupervisorGenServer::new();
    let mut supervisor_pid = supervisor_gen_server.pid();

    log::debug!("creating blockwheel_fs gen_server");
    let blockwheel_fs_gen_server = blockwheel_fs::GenServer::new();
    let _blockwheel_fs_pid = blockwheel_fs_gen_server.pid();

    std::fs::remove_file(&cli_args.wheel_filename).ok();

    supervisor_pid.spawn_link_permanent(async move {
        let mut blocks = Vec::new();
        let mut counter = blockwheel_fs::stress::Counter::default();
        let stress_task = blockwheel_fs::stress::stress_loop(params, &mut blocks, &mut counter, &limits);
        match stress_task.await {
            Ok(()) =>
                log::info!("stress task done: counters = {counter:?}"),
            Err(error) => {
                log::error!("stress task error: {error:?}");
                log::error!("blocks: {blocks:?}");
            },
        }
    });

    supervisor_gen_server.run().await;
}
