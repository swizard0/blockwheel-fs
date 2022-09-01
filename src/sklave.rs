
use crate::{
    job,
    Welt,
    Order,
    Error,
    ReplyPolicy,
};

pub type SklaveJob<B, R> = arbeitssklave::SklaveJob<Welt, Order<B, R>>;

pub fn run_job<B, R, P>(sklave_job: SklaveJob<B, R>, thread_pool: &P)
where P: edeltraud::ThreadPool<job::Job<B, R>>,
      R: ReplyPolicy<B>,
{
    if let Err(error) = job(sklave_job, thread_pool) {
        log::error!("terminated with an error: {error:?}");
    }
}

fn job<B, R, P>(
    SklaveJob { mut sklave, mut sklavenwelt, }: SklaveJob<B, R>,
    thread_pool: &P,
)
    -> Result<(), Error>
where P: edeltraud::ThreadPool<job::Job<B, R>>,
      R: ReplyPolicy<B>,
{

    todo!()
}
