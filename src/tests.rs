use std::{
    fs,
    collections::HashMap,
};

use ero::{
    supervisor,
};

use super::{
    Params,
    GenServer,
};

#[test]
fn stress() {
    let mut runtime = tokio::runtime::Builder::new()
        .basic_scheduler()
        .build()
        .unwrap();
    let wheel_filename = "/tmp/stress";
    runtime.block_on(async {
        let supervisor_gen_server = supervisor::SupervisorGenServer::new();
        let mut supervisor_pid = supervisor_gen_server.pid();
        tokio::spawn(supervisor_gen_server.run());

        let gen_server = GenServer::new();
        let mut pid = gen_server.pid();
        supervisor_pid.spawn_link_permanent(
            gen_server.run(
                supervisor_pid.clone(),
                Params {
                    wheel_filename: wheel_filename.into(),
                    init_wheel_size_bytes: 2 * 1024 * 1024,
                    work_block_size_bytes: 256 * 1024,
                    lru_cache_size_bytes: 0,
                    ..Default::default()
                },
            ),
        );

        // let mut blocks = HashMap::new();
        let mut rng = rand::thread_rng();

        let block = pid.lend_block().await
            .map_err(|ero::NoProcError| Error::WheelGoneDuringLendBlock)?;

        Ok::<_, Error>(())
    }).unwrap();
    fs::remove_file(wheel_filename).ok();
}

#[derive(Debug)]
enum Error {
    WheelGoneDuringLendBlock,
}
