use alloc_pool::bytes::{
    Bytes,
};

use crate::{
    wheel::interpret,
};

pub enum Job {
    BlockPrepareWrite(interpret::BlockPrepareWriteJobArgs),
    BlockProcessRead(interpret::BlockProcessReadJobArgs),
    BlockPrepareDelete(interpret::BlockPrepareDeleteJobArgs),
}

pub enum JobOutput {
    BlockPrepareWrite(BlockPrepareWriteDone),
    BlockProcessRead(BlockProcessReadDone),
    BlockPrepareDelete(BlockPrepareDeleteDone),
}

impl edeltraud::Job for Job {
    type Output = JobOutput;

    fn run(self) -> Self::Output {
        match self {
            Job::BlockPrepareWrite(args) =>
                JobOutput::BlockPrepareWrite(BlockPrepareWriteDone(interpret::block_prepare_write_job(args))),
            Job::BlockProcessRead(args) =>
                JobOutput::BlockProcessRead(BlockProcessReadDone(interpret::block_process_read_job(args))),
            Job::BlockPrepareDelete(args) =>
                JobOutput::BlockPrepareDelete(BlockPrepareDeleteDone(interpret::block_prepare_delete_job(args))),
        }
    }
}

pub struct BlockPrepareWriteDone(pub interpret::BlockPrepareWriteJobOutput);

impl From<JobOutput> for BlockPrepareWriteDone {
    fn from(output: JobOutput) -> Self {
        match output {
            JobOutput::BlockPrepareWrite(done) =>
                done,
            _other =>
                panic!("expected JobOutput::BlockPrepareWrite but got other"),
        }
    }
}

pub struct BlockProcessReadDone(pub interpret::BlockProcessReadJobOutput);

impl From<JobOutput> for BlockProcessReadDone {
    fn from(output: JobOutput) -> Self {
        match output {
            JobOutput::BlockProcessRead(done) =>
                done,
            _other =>
                panic!("expected JobOutput::BlockProcessRead but got other"),
        }
    }
}

pub struct BlockPrepareDeleteDone(pub interpret::BlockPrepareDeleteJobOutput);

impl From<JobOutput> for BlockPrepareDeleteDone {
    fn from(output: JobOutput) -> Self {
        match output {
            JobOutput::BlockPrepareDelete(done) =>
                done,
            _other =>
                panic!("expected JobOutput::BlockPrepareDelete but got other"),
        }
    }
}
