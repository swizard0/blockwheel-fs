use tokio::runtime;

use super::{
    GenServer,
    CreateParams,
};

#[test]
fn create_empty_read_empty() {
    let mut runtime = runtime::Builder::new()
        .basic_scheduler()
        .build()
        .unwrap();

    let gen_server = runtime.block_on(async {
        GenServer::create(CreateParams {
            wheel_filename: "/tmp/blockwheel_create_empty_read_empty",
            work_block_size_bytes: 64 * 1024,
            init_wheel_size_bytes: 256 * 1024,
        }).await
    }).unwrap();
}
