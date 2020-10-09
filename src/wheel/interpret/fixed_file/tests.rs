use tokio::runtime;

use super::{
    GenServer,
    OpenParams,
    CreateParams,
};

#[derive(Debug)]
enum Error {
    Create(super::WheelCreateError),
    Open(super::WheelOpenError),
    Run(super::Error),
}

#[test]
fn create_empty_read_empty() {
    let mut runtime = runtime::Builder::new()
        .basic_scheduler()
        .build()
        .unwrap();

    let wheel_filename = "/tmp/blockwheel_create_empty_read_empty";

    runtime.block_on(async {
        let _gen_server = GenServer::create(CreateParams {
            wheel_filename,
            work_block_size_bytes: 64 * 1024,
            init_wheel_size_bytes: 256 * 1024,
        }).await.map_err(Error::Create)?;
        let _gen_server = GenServer::open(OpenParams {
            wheel_filename,
            work_block_size_bytes: 64 * 1024,
        }).await.map_err(Error::Open)?;
        Ok::<_, Error>(())
    }).unwrap();
}
