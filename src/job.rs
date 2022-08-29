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
        performer_sklave,
    },
};

pub enum Job {
    BlockPrepareWrite(interpret::BlockPrepareWriteJobArgs),
    BlockProcessRead(interpret::BlockProcessReadJobArgs),
    BlockPrepareDelete(interpret::BlockPrepareDeleteJobArgs),
    PerformerSklave(performer_sklave::SklaveJob),
}

impl From<interpret::BlockPrepareWriteJobArgs> for Job {
    fn from(job_args: interpret::BlockPrepareWriteJobArgs) -> Job {
        Job::BlockPrepareWrite(job_args)
    }
}

impl From<interpret::BlockProcessReadJobArgs> for Job {
    fn from(job_args: interpret::BlockProcessReadJobArgs) -> Job {
        Job::BlockProcessRead(job_args)
    }
}

impl From<interpret::BlockPrepareDeleteJobArgs> for Job {
    fn from(job_args: interpret::BlockPrepareDeleteJobArgs) -> Job {
        Job::BlockPrepareDelete(job_args)
    }
}

impl From<performer_sklave::SklaveJob> for Job {
    fn from(sklave_job: performer_sklave::SklaveJob) -> Job {
        Job::PerformerSklave(sklave_job)
    }
}

pub static JOB_BLOCK_PREPARE_WRITE: AtomicUsize = AtomicUsize::new(0);
pub static JOB_BLOCK_PROCESS_READ: AtomicUsize = AtomicUsize::new(0);
pub static JOB_BLOCK_PREPARE_DELETE: AtomicUsize = AtomicUsize::new(0);
pub static JOB_PERFORMER_SKLAVE: AtomicUsize = AtomicUsize::new(0);

impl edeltraud::Job for Job {
    type Output = ();

    fn run<P>(self, thread_pool: &P) -> Self::Output where P: edeltraud::ThreadPool<Self> {
        match self {
            Job::BlockPrepareWrite(args) => {
                JOB_BLOCK_PREPARE_WRITE.fetch_add(1, Ordering::Relaxed);
                interpret::block_prepare_write_job(args, thread_pool);
            },
            Job::BlockProcessRead(args) => {
                JOB_BLOCK_PROCESS_READ.fetch_add(1, Ordering::Relaxed);
                interpret::block_process_read_job(args, thread_pool);
            },
            Job::BlockPrepareDelete(args) => {
                JOB_BLOCK_PREPARE_DELETE.fetch_add(1, Ordering::Relaxed);
                interpret::block_prepare_delete_job(args, thread_pool);
            },
            Job::PerformerSklave(sklave_job) => {
                JOB_PERFORMER_SKLAVE.fetch_add(1, Ordering::Relaxed);
                performer_sklave::run_job(sklave_job, thread_pool);
            },
        }
    }
}
