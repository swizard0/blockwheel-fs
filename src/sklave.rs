
use crate::{
    job,
    Welt,
    Order,
    Error,
    AccessPolicy,
};

pub type SklaveJob<A> = arbeitssklave::SklaveJob<Welt, Order<A>>;

pub fn run_job<A, P>(sklave_job: SklaveJob<A>, thread_pool: &P)
where P: edeltraud::ThreadPool<job::Job<A>>,
      A: AccessPolicy,
{
    if let Err(error) = job(sklave_job, thread_pool) {
        log::error!("terminated with an error: {error:?}");
    }
}

fn job<A, P>(
    SklaveJob { mut sklave, mut sklavenwelt, }: SklaveJob<A>,
    thread_pool: &P,
)
    -> Result<(), Error>
where P: edeltraud::ThreadPool<job::Job<A>>,
      A: AccessPolicy,
{

    todo!()
}
