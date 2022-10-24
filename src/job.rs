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

pub enum Job<E> where E: EchoPolicy {
    BlockPrepareWrite(interpret::BlockPrepareWriteJobArgs<E>),
    BlockProcessRead(interpret::BlockProcessReadJobArgs<E>),
    BlockPrepareDelete(interpret::BlockPrepareDeleteJobArgs<E>),
    PerformerSklave(performer_sklave::SklaveJob<E>),
}

impl<E> From<interpret::BlockPrepareWriteJobArgs<E>> for Job<E> where E: EchoPolicy {
    fn from(job_args: interpret::BlockPrepareWriteJobArgs<E>) -> Job<E> {
        Job::BlockPrepareWrite(job_args)
    }
}

impl<E> From<interpret::BlockProcessReadJobArgs<E>> for Job<E> where E: EchoPolicy {
    fn from(job_args: interpret::BlockProcessReadJobArgs<E>) -> Job<E> {
        Job::BlockProcessRead(job_args)
    }
}

impl<E> From<interpret::BlockPrepareDeleteJobArgs<E>> for Job<E> where E: EchoPolicy {
    fn from(job_args: interpret::BlockPrepareDeleteJobArgs<E>) -> Job<E> {
        Job::BlockPrepareDelete(job_args)
    }
}

impl<E> From<performer_sklave::SklaveJob<E>> for Job<E> where E: EchoPolicy {
    fn from(sklave_job: performer_sklave::SklaveJob<E>) -> Job<E> {
        Job::PerformerSklave(sklave_job)
    }
}

pub static JOB_BLOCK_PREPARE_WRITE: AtomicUsize = AtomicUsize::new(0);
pub static JOB_BLOCK_PROCESS_READ: AtomicUsize = AtomicUsize::new(0);
pub static JOB_BLOCK_PREPARE_DELETE: AtomicUsize = AtomicUsize::new(0);
pub static JOB_PERFORMER_SKLAVE: AtomicUsize = AtomicUsize::new(0);

impl<E> edeltraud::Job for Job<E> where E: EchoPolicy {
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
