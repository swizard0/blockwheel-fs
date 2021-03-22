use alloc_pool::bytes::{
    Bytes,
};

use crate::{
    block,
    wheel::interpret,
};

pub enum Job {
    CalculateCrc { block_bytes: Bytes, },
    BlockProcess(interpret::BlockProcessJobArgs),
}

pub enum JobOutput {
    CalculateCrc(CalculateCrcDone),
    BlockProcess(BlockProcessDone),
}

impl edeltraud::Job for Job {
    type Output = JobOutput;

    fn run(self) -> Self::Output {
        match self {
            Job::CalculateCrc { ref block_bytes, } =>
                JobOutput::CalculateCrc(CalculateCrcDone { crc: block::crc(block_bytes), }),
            Job::BlockProcess(args) =>
                JobOutput::BlockProcess(BlockProcessDone(interpret::block_process_job(args))),
        }
    }
}

pub struct CalculateCrcDone {
    pub crc: u64,
}

impl From<JobOutput> for CalculateCrcDone {
    fn from(output: JobOutput) -> Self {
        match output {
            JobOutput::CalculateCrc(done) =>
                done,
            _other =>
                panic!("expected JobOutput::CalculateCrc but got other"),
        }
    }
}

pub struct BlockProcessDone(pub interpret::BlockProcessJobOutput);

impl From<JobOutput> for BlockProcessDone {
    fn from(output: JobOutput) -> Self {
        match output {
            JobOutput::BlockProcess(done) =>
                done,
            _other =>
                panic!("expected JobOutput::CalculateCrc but got other"),
        }
    }
}
