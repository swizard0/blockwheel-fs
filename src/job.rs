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
        performer_actor,
    },
};

pub enum Job {
    BlockPrepareWrite(interpret::BlockPrepareWriteJobArgs),
    BlockProcessRead(interpret::BlockProcessReadJobArgs),
    BlockPrepareDelete(interpret::BlockPrepareDeleteJobArgs),
    PerformerActorRun(performer_actor::Actor),
}

pub static JOB_BLOCK_PREPARE_WRITE: AtomicUsize = AtomicUsize::new(0);
pub static JOB_BLOCK_PROCESS_READ: AtomicUsize = AtomicUsize::new(0);
pub static JOB_BLOCK_PREPARE_DELETE: AtomicUsize = AtomicUsize::new(0);
pub static JOB_PERFORMER_ACTOR_RUN: AtomicUsize = AtomicUsize::new(0);

impl edeltraud::Job for Job {
    type Output = ();

    fn run<P>(self, thread_pool: &P) -> Self::Output where P: edeltraud::ThreadPool<Self> {
        match self {
            Job::BlockPrepareWrite(args) => {
                JOB_BLOCK_PREPARE_WRITE.fetch_add(1, Ordering::Relaxed);
                interpret::block_prepare_write_job(args);
            },
            Job::BlockProcessRead(args) => {
                JOB_BLOCK_PROCESS_READ.fetch_add(1, Ordering::Relaxed);
                interpret::block_process_read_job(args);
            },
            Job::BlockPrepareDelete(args) => {
                JOB_BLOCK_PREPARE_DELETE.fetch_add(1, Ordering::Relaxed);
                interpret::block_prepare_delete_job(args);
            },
            Job::PerformerActorRun(actor) => {
                JOB_PERFORMER_ACTOR_RUN.fetch_add(1, Ordering::Relaxed);
                performer_actor::run_job(actor, thread_pool);
            },
        }
    }
}
