use super::{
    block,
};

pub struct Blocks {
    pool: Vec<block::Bytes>,
}

impl Blocks {
    pub fn new() -> Blocks {
        Blocks {
            pool: Vec::new(),
        }
    }

    pub fn lend(&mut self) -> block::BytesMut {
        let mut cursor = self.pool.len();
        while cursor > 0 {
            cursor -= 1;
            let frozen_block = self.pool.swap_remove(cursor);
            match frozen_block.into_mut() {
                Ok(mut block) => {
                    block.clear();
                    return block;
                },
                Err(frozen_block) =>
                    self.pool.push(frozen_block),
            }
        }

        block::BytesMut::new()
    }

    pub fn repay(&mut self, block_bytes: block::Bytes) {
        self.pool.push(block_bytes)
    }
}
