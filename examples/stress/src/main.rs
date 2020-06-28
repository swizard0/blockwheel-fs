
use structopt::StructOpt;

use log::{
    debug,
};

use ero::{
    supervisor::SupervisorGenServer,
};

use ero_blockwheel as blockwheel;

#[derive(Debug, StructOpt)]
struct Opt {
    /// Filename for blockwheel data
    #[structopt(short = "w", long = "wheel-filename", default_value = "wheel")]
    wheel_filename: String,
    /// Initial wheel size when creating new file (in bytes)
    #[structopt(short = "s", long = "init-wheel-size-bytes", default_value = "67108864")]
    init_wheel_size_bytes: usize,
    /// wheel ero task restart holdon (in seconds)
    #[structopt(long = "wheel-task-restart-sec", default_value = "4")]
    wheel_task_restart_sec: usize,
    /// work io buffer size (in bytes)
    #[structopt(long = "work-block-size", default_value = "8388608")]
    work_block_size: usize,
}

#[tokio::main]
async fn main() {
    pretty_env_logger::init();
    let opts = Opt::from_args();

    debug!("creating supervisor");
    let supervisor_gen_server = SupervisorGenServer::new();
    let mut supervisor_pid = supervisor_gen_server.pid();

    debug!("creating blockwheel gen_server");
    let blockwheel_gen_server = blockwheel::GenServer::new();
    let blockwheel_pid = blockwheel_gen_server.pid();

    supervisor_pid.spawn_link_permanent(blockwheel_gen_server.run(
        blockwheel::Params {
            wheel_filename: opts.wheel_filename.into(),
            init_wheel_size_bytes: opts.init_wheel_size_bytes,
            wheel_task_restart_sec: opts.wheel_task_restart_sec,
            work_block_size: opts.work_block_size,
        },
    ));

    supervisor_gen_server.run().await;
}
