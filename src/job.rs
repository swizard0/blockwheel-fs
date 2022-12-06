use std::{
    sync::{
        atomic::{
            Ordering,
            AtomicUsize,
        },
    },
};

use crate::{
    EchoPolicy,
    wheel::{
        interpret,
        performer_sklave,
    },
};

pub use performer_sklave::SklaveJob;
pub use interpret::BlockPrepareWriteJob;
pub use interpret::BlockPrepareDeleteJob;
pub use interpret::BlockProcessReadJob;

pub enum Job<E> where E: EchoPolicy {
    BlockPrepareWrite(interpret::BlockPrepareWriteJob<E>),
    BlockProcessRead(interpret::BlockProcessReadJob<E>),
    BlockPrepareDelete(interpret::BlockPrepareDeleteJob<E>),
    PerformerSklave(SklaveJob<E>),
}

impl<E> From<interpret::BlockPrepareWriteJob<E>> for Job<E> where E: EchoPolicy {
    fn from(job_args: interpret::BlockPrepareWriteJob<E>) -> Self {
        Self::BlockPrepareWrite(job_args)
    }
}

impl<E> From<interpret::BlockProcessReadJob<E>> for Job<E> where E: EchoPolicy {
    fn from(job_args: interpret::BlockProcessReadJob<E>) -> Self {
        Self::BlockProcessRead(job_args)
    }
}

impl<E> From<interpret::BlockPrepareDeleteJob<E>> for Job<E> where E: EchoPolicy {
    fn from(job_args: interpret::BlockPrepareDeleteJob<E>) -> Self {
        Self::BlockPrepareDelete(job_args)
    }
}

impl<E> From<SklaveJob<E>> for Job<E> where E: EchoPolicy {
    fn from(sklave_job: SklaveJob<E>) -> Self {
        Self::PerformerSklave(sklave_job)
    }
}

pub static JOB_BLOCK_PREPARE_WRITE: AtomicUsize = AtomicUsize::new(0);
pub static JOB_BLOCK_PROCESS_READ: AtomicUsize = AtomicUsize::new(0);
pub static JOB_BLOCK_PREPARE_DELETE: AtomicUsize = AtomicUsize::new(0);
pub static JOB_PERFORMER_SKLAVE: AtomicUsize = AtomicUsize::new(0);

pub struct JobUnit<E, J>(edeltraud::JobUnit<J, Job<E>>) where E: EchoPolicy;

impl<E, J> From<edeltraud::JobUnit<J, Job<E>>> for JobUnit<E, J> where E: EchoPolicy {
    fn from(job_unit: edeltraud::JobUnit<J, Job<E>>) -> Self {
        Self(job_unit)
    }
}

impl<E, J> edeltraud::Job for JobUnit<E, J>
where E: EchoPolicy,
      J: From<SklaveJob<E>>,
      J: From<interpret::BlockPrepareWriteJob<E>>,
      J: From<interpret::BlockPrepareDeleteJob<E>>,
      J: From<interpret::BlockProcessReadJob<E>>,
{
    fn run(self) {
        match self.0.job {
            Job::BlockPrepareWrite(args) => {
                JOB_BLOCK_PREPARE_WRITE.fetch_add(1, Ordering::Relaxed);
                interpret::block_prepare_write_job(args, &self.0.handle);
            },
            Job::BlockProcessRead(args) => {
                JOB_BLOCK_PROCESS_READ.fetch_add(1, Ordering::Relaxed);
                interpret::block_process_read_job(args, &self.0.handle);
            },
            Job::BlockPrepareDelete(args) => {
                JOB_BLOCK_PREPARE_DELETE.fetch_add(1, Ordering::Relaxed);
                interpret::block_prepare_delete_job(args, &self.0.handle);
            },
            Job::PerformerSklave(sklave_job) => {
                JOB_PERFORMER_SKLAVE.fetch_add(1, Ordering::Relaxed);
                performer_sklave::run_job(sklave_job, &self.0.handle);
            },
        }
    }
}
