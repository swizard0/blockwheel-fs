use std::sync::{
    Arc,
    atomic::{
        Ordering,
        AtomicUsize,
    },
};

use super::{
    block,
};

pub struct Blocks {
    pool: Vec<Option<block::Bytes>>,
    free: Vec<usize>,
    release_head: Arc<AtomicUsize>,
}

impl Blocks {
    pub fn new() -> Blocks {
        Blocks {
            pool: Vec::new(),
            free: Vec::new(),
            release_head: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub fn lend(&mut self) -> block::BytesMut {
        let mut release_head = self.release_head.load(Ordering::SeqCst);
        if release_head == 0 {
            // no released blocks yet
            block::BytesMut::new(self.release_head.clone())
        } else {
            // take first released block from the list
            loop {
                let index = release_head - 1;
                let block_bytes = self.pool[index].take().unwrap();
                self.free.push(index);
                let release_prev = block_bytes.release_prev.load(Ordering::SeqCst);
                match self.release_head.compare_exchange(release_head, release_prev, Ordering::SeqCst, Ordering::Relaxed) {
                    Ok(..) =>
                        match block_bytes.into_mut() {
                            Ok(mut block) => {
                                block.clear();
                                return block;
                            },
                            Err(..) =>
                                panic!("expected block with unique strong reference in release list, but got shared"),
                        },
                    Err(modified) =>
                        release_head = modified,
                }
            }
        }
    }

    pub fn repay(&mut self, block_bytes: block::Bytes) {
        // use simple load/store pair instead of compare_exchange
        // because this is the only place where this block field is modified
        if block_bytes.release_key.load(Ordering::SeqCst) != 0 {
            // already repayed
            return;
        }

        let index = if let Some(free_index) = self.free.pop() {
            assert!(self.pool[free_index].is_none());
            self.pool[free_index] = Some(block_bytes);
            free_index
        } else {
            let next_index = self.pool.len();
            self.pool.push(Some(block_bytes));
            next_index
        };

        let block_bytes = self.pool[index].as_ref().unwrap();
        let release_key = index + 1;
        block_bytes.release_key.store(release_key, Ordering::Relaxed);

        // move to release list in case of unique reference
        if Arc::strong_count(&block_bytes.bytes) == 1 {
            let mut release_head = self.release_head.load(Ordering::SeqCst);
            loop {
                block_bytes.release_prev.store(release_head, Ordering::SeqCst);
                match self.release_head.compare_exchange(release_head, release_key, Ordering::SeqCst, Ordering::Relaxed) {
                    Ok(..) =>
                        break,
                    Err(value) =>
                        release_head = value,
                }
            }
        }
    }
}
