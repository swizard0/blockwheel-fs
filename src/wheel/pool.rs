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
                let release_prev = block_bytes.release.prev.load(Ordering::SeqCst);
                match self.release_head.compare_exchange(release_head, release_prev, Ordering::SeqCst, Ordering::Relaxed) {
                    Ok(..) =>
                        match block_bytes.into_mut() {
                            Ok(mut block) => {
                                block.clear();
                                return block;
                            },
                            Err(block_bytes) =>
                                panic!(
                                    "expected block with unique strong reference in release list but got shared (release = {:?})",
                                    block_bytes.release,
                                ),
                        },
                    Err(modified) =>
                        release_head = modified,
                }
            }
        }
    }

    pub fn repay(&mut self, block_bytes: block::Bytes) {
        // use simple `load`/`fetch_add` pair instead of `compare_exchange`
        // because this is the only place where this block field is modified
        let release_key = block_bytes.release.key.load(Ordering::SeqCst);
        let (index_succ, _) = block::split_release_key(release_key);
        if index_succ != 0 {
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
        let index_succ = index + 1;
        let release_key_add = block::make_release_key(index_succ, 0);
        let prev_release_key = block_bytes.release.key.fetch_add(release_key_add, Ordering::SeqCst);
        let (_, refs_count) = block::split_release_key(prev_release_key);

        // move to release list in case of unique reference
        if refs_count == 1 {
            let mut release_head = self.release_head.load(Ordering::SeqCst);
            loop {
                block_bytes.release.prev.store(release_head, Ordering::Relaxed);
                match self.release_head.compare_exchange(release_head, index_succ, Ordering::SeqCst, Ordering::Relaxed) {
                    Ok(..) =>
                        break,
                    Err(value) =>
                        release_head = value,
                }
            }
        }
    }

    #[cfg(test)]
    fn will_reuse(&self) -> bool {
        self.release_head.load(Ordering::SeqCst) != 0
    }
}

#[cfg(test)]
mod tests {
    use super::Blocks;

    #[test]
    fn reusing() {
        let mut blocks = Blocks::new();

        assert!(!blocks.will_reuse());
        let mut block_a = blocks.lend();
        block_a.push(0);
        let ptr_a = block_a.as_ptr();

        assert!(!blocks.will_reuse());
        let mut block_b = blocks.lend();
        block_b.push(1);
        let ptr_b = block_b.as_ptr();
        assert_ne!(ptr_a, ptr_b);

        assert!(!blocks.will_reuse());
        let mut block_c = blocks.lend();
        block_c.push(2);
        let ptr_c = block_c.as_ptr();
        assert_ne!(ptr_b, ptr_c);

        blocks.repay(block_b.freeze());

        assert!(blocks.will_reuse());
        let mut block_d = blocks.lend();
        block_d.push(3);
        assert_eq!(block_d.as_ptr(), ptr_b);

        let block_a_freezed = block_a.freeze();
        let block_e_freezed = block_a_freezed.clone();
        blocks.repay(block_a_freezed);

        assert!(!blocks.will_reuse());
        let mut block_f = blocks.lend();
        block_f.push(4);
        assert_ne!(block_f.as_ptr(), ptr_b);

        std::mem::drop(block_e_freezed);
        assert!(blocks.will_reuse());
        let mut block_g = blocks.lend();
        block_g.push(5);
        assert_eq!(block_g.as_ptr(), ptr_a);

        let block_c_freezed = block_c.freeze();
        let block_h_freezed = block_c_freezed.clone();
        let block_i_freezed = block_h_freezed.clone();
        blocks.repay(block_h_freezed);

        assert!(!blocks.will_reuse());
        let mut block_j = blocks.lend();
        block_j.push(6);
        assert_ne!(block_j.as_ptr(), ptr_c);

        std::mem::drop(block_c_freezed);
        blocks.repay(block_g.freeze());

        assert!(blocks.will_reuse());
        let mut block_k = blocks.lend();
        block_k.push(7);
        assert_ne!(block_k.as_ptr(), ptr_c);
        assert_eq!(block_k.as_ptr(), ptr_a);

        std::mem::drop(block_i_freezed);

        assert!(blocks.will_reuse());
        let mut block_l = blocks.lend();
        block_l.push(8);
        assert_eq!(block_l.as_ptr(), ptr_c);
    }
}
