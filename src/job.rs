use std::{
    sync::{
        atomic::{
            Ordering,
            AtomicUsize,
        },
    },
};

use crate::{
    AccessPolicy,
    wheel::{
        interpret,
        performer_sklave,
    },
};

pub enum Job<A> where A: AccessPolicy {
    BlockPrepareWrite(interpret::BlockPrepareWriteJobArgs<A>),
    BlockProcessRead(interpret::BlockProcessReadJobArgs<A>),
    BlockPrepareDelete(interpret::BlockPrepareDeleteJobArgs<A>),
    PerformerSklave(performer_sklave::SklaveJob<A>),
}

impl<A> From<interpret::BlockPrepareWriteJobArgs<A>> for Job<A> where A: AccessPolicy {
    fn from(job_args: interpret::BlockPrepareWriteJobArgs<A>) -> Job<A> {
        Job::BlockPrepareWrite(job_args)
    }
}

impl<A> From<interpret::BlockProcessReadJobArgs<A>> for Job<A> where A: AccessPolicy {
    fn from(job_args: interpret::BlockProcessReadJobArgs<A>) -> Job<A> {
        Job::BlockProcessRead(job_args)
    }
}

impl<A> From<interpret::BlockPrepareDeleteJobArgs<A>> for Job<A> where A: AccessPolicy {
    fn from(job_args: interpret::BlockPrepareDeleteJobArgs<A>) -> Job<A> {
        Job::BlockPrepareDelete(job_args)
    }
}

impl<A> From<performer_sklave::SklaveJob<A>> for Job<A> where A: AccessPolicy {
    fn from(sklave_job: performer_sklave::SklaveJob<A>) -> Job<A> {
        Job::PerformerSklave(sklave_job)
    }
}

pub static JOB_BLOCK_PREPARE_WRITE: AtomicUsize = AtomicUsize::new(0);
pub static JOB_BLOCK_PROCESS_READ: AtomicUsize = AtomicUsize::new(0);
pub static JOB_BLOCK_PREPARE_DELETE: AtomicUsize = AtomicUsize::new(0);
pub static JOB_PERFORMER_SKLAVE: AtomicUsize = AtomicUsize::new(0);

impl<A> edeltraud::Job for Job<A> where A: AccessPolicy {
    fn run<P>(self, thread_pool: &P) where P: edeltraud::ThreadPool<Self> {
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
