use std::{
    sync::{
        atomic::{
            Ordering,
            AtomicUsize,
        },
    },
};

use crate::{
    wheel::{
        interpret,
        performer_job,
    },
};

pub enum Job {
    BlockPrepareWrite(interpret::BlockPrepareWriteJobArgs),
    BlockProcessRead(interpret::BlockProcessReadJobArgs),
    BlockPrepareDelete(interpret::BlockPrepareDeleteJobArgs),
    PerformerJobRun(performer_job::JobArgs),
}

pub enum JobOutput {
    BlockPrepareWrite(BlockPrepareWriteDone),
    BlockProcessRead(BlockProcessReadDone),
    BlockPrepareDelete(BlockPrepareDeleteDone),
    PerformerJobRun(PerformerJobRunDone),
}

pub static JOB_BLOCK_PREPARE_WRITE: AtomicUsize = AtomicUsize::new(0);
pub static JOB_BLOCK_PROCESS_READ: AtomicUsize = AtomicUsize::new(0);
pub static JOB_BLOCK_PREPARE_DELETE: AtomicUsize = AtomicUsize::new(0);
pub static JOB_PERFORMER_JOB_RUN: AtomicUsize = AtomicUsize::new(0);

impl edeltraud::Job for Job {
    type Output = JobOutput;

    fn run(self) -> Self::Output {
        match self {
            Job::BlockPrepareWrite(args) => {
                JOB_BLOCK_PREPARE_WRITE.fetch_add(1, Ordering::Relaxed);
                JobOutput::BlockPrepareWrite(BlockPrepareWriteDone(interpret::block_prepare_write_job(args)))
            },
            Job::BlockProcessRead(args) => {
                JOB_BLOCK_PROCESS_READ.fetch_add(1, Ordering::Relaxed);
                JobOutput::BlockProcessRead(BlockProcessReadDone(interpret::block_process_read_job(args)))
            },
            Job::BlockPrepareDelete(args) => {
                JOB_BLOCK_PREPARE_DELETE.fetch_add(1, Ordering::Relaxed);
                JobOutput::BlockPrepareDelete(BlockPrepareDeleteDone(interpret::block_prepare_delete_job(args)))
            },
            Job::PerformerJobRun(args) => {
                JOB_PERFORMER_JOB_RUN.fetch_add(1, Ordering::Relaxed);
                JobOutput::PerformerJobRun(PerformerJobRunDone(performer_job::run_job(args)))
            },
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

pub struct PerformerJobRunDone(pub performer_job::Output);

impl From<JobOutput> for PerformerJobRunDone {
    fn from(output: JobOutput) -> Self {
        match output {
            JobOutput::PerformerJobRun(done) =>
                done,
            _other =>
                panic!("expected JobOutput::PerformerJobRun but got other"),
        }
    }
}
