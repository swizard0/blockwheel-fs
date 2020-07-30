use std::{
    mem,
    collections::{
        HashMap,
    },
};

use super::{
    block,
};

pub struct Cache {
    entries: HashMap<block::Id, Entry>,
    list_head: Option<block::Id>,
    total_bytes: usize,
    total_bytes_limit: usize,
}

#[derive(Debug)]
struct Entry {
    block_bytes: block::Bytes,
    list_prev: block::Id,
    list_next: block::Id,
}

impl Cache {
    pub fn new(total_bytes_limit: usize) -> Cache {
        Cache {
            entries: HashMap::new(),
            list_head: None,
            total_bytes: 0,
            total_bytes_limit,
        }
    }

    pub fn insert(&mut self, block_id: block::Id, block_bytes: block::Bytes) {
        if self.entries.contains_key(&block_id) {
            return;
        }

        let bytes_required = block_bytes.len();

        while self.total_bytes + bytes_required > self.total_bytes_limit {
            if let Some(head_block_id) = &self.list_head {
                let head_block_entry = self.entries.get(head_block_id).unwrap();
                if &head_block_entry.list_prev == head_block_id && &head_block_entry.list_next == head_block_id {
                    self.entries.remove(head_block_id);
                    self.list_head = None;
                } else {
                    let tail_block_id = head_block_entry.list_prev.clone();
                    let removed_entry = self.entries.remove(&tail_block_id).unwrap();
                    self.entries.get_mut(head_block_id).unwrap().list_prev = removed_entry.list_prev.clone();
                    self.entries.get_mut(&removed_entry.list_prev).unwrap().list_next = removed_entry.list_next;
                    self.total_bytes -= removed_entry.block_bytes.len();
                }
            } else {
                return;
            }
        }

        let block_size = block_bytes.len();
        if let Some(head_block_id) = &self.list_head {
            let head_block_id = head_block_id.clone();
            let head_block_entry = self.entries.get_mut(&head_block_id).unwrap();
            let prev_head_block_id = mem::replace(
                &mut head_block_entry.list_prev,
                block_id.clone(),
            );
            let block_entry = Entry {
                block_bytes,
                list_prev: prev_head_block_id.clone(),
                list_next: head_block_id.clone(),
            };
            self.entries.insert(block_id.clone(), block_entry);
            self.entries.get_mut(&prev_head_block_id).unwrap().list_next = block_id.clone();
        } else {
            let block_entry = Entry {
                block_bytes,
                list_prev: block_id.clone(),
                list_next: block_id.clone(),
            };
            self.entries.insert(block_id.clone(), block_entry);
        }
        self.list_head = Some(block_id);
        self.total_bytes += block_size;
    }

    pub fn get(&mut self, block_id: &block::Id) -> Option<&block::Bytes> {
        self.access(block_id)
            .map(|block_entry| &block_entry.block_bytes)
    }

    pub fn invalidate(&mut self, block_id: &block::Id) {
        if let Some(..) = self.access(block_id) {
            let removed_entry = self.entries.remove(block_id).unwrap();
            let list_prev = removed_entry.list_prev;
            let list_next = removed_entry.list_next;

            assert_eq!(self.list_head.as_ref(), Some(block_id));
            if block_id == &list_prev && block_id == &list_next {
                self.list_head = None;
            } else {
                self.entries.get_mut(&list_prev).unwrap().list_next = list_next.clone();
                self.entries.get_mut(&list_next).unwrap().list_prev = list_prev.clone();
                self.list_head = Some(list_next);
            }

            self.total_bytes -= removed_entry.block_bytes.len();
        }
    }

    fn access(&mut self, block_id: &block::Id) -> Option<&mut Entry> {
        match &mut self.list_head {
            None =>
                None,
            Some(head_block_id) if head_block_id == block_id => {
                let block_entry = self.entries.get_mut(block_id).unwrap();
                Some(block_entry)
            },
            Some(head_block_id) =>
                if let Some(block_entry) = self.entries.get(block_id) {
                    let head_block_id = mem::replace(head_block_id, block_id.clone());
                    let list_prev = block_entry.list_prev.clone();
                    let list_next = block_entry.list_next.clone();
                    self.entries.get_mut(&list_prev).unwrap().list_next = list_next.clone();
                    self.entries.get_mut(&list_next).unwrap().list_prev = list_prev.clone();

                    let head_block_entry = self.entries.get_mut(&head_block_id).unwrap();
                    let prev_head_block_id = mem::replace(
                        &mut head_block_entry.list_prev,
                        block_id.clone(),
                    );
                    self.entries.get_mut(&prev_head_block_id).unwrap().list_next = block_id.clone();

                    let block_entry = self.entries.get_mut(block_id).unwrap();
                    block_entry.list_prev = prev_head_block_id;
                    block_entry.list_next = head_block_id;
                    Some(block_entry)
                } else {
                    None
                },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        block,
        Cache,
    };

    fn sample_hello_world() -> block::Bytes {
        let mut block_bytes_mut = block::BytesMut::new_detached();
        block_bytes_mut.extend("hello, world!".as_bytes().iter().cloned());
        block_bytes_mut.freeze()
    }

    #[test]
    fn lru_basic() {
        let mut lru = Cache::new(32);

        let id_0 = block::Id::init();
        lru.insert(id_0.clone(), sample_hello_world());
        assert_eq!(lru.get(&id_0), Some(&sample_hello_world()));

        let id_1 = id_0.next();
        lru.insert(id_1.clone(), sample_hello_world());
        assert_eq!(lru.get(&id_1), Some(&sample_hello_world()));
        assert_eq!(lru.get(&id_0), Some(&sample_hello_world()));

        let id_2 = id_1.next();
        lru.insert(id_2.clone(), sample_hello_world());
        assert_eq!(lru.get(&id_2), Some(&sample_hello_world()));
        assert_eq!(lru.get(&id_1), None);
        assert_eq!(lru.get(&id_0), Some(&sample_hello_world()));

        lru.invalidate(&id_2);
        assert_eq!(lru.get(&id_2), None);
        assert_eq!(lru.get(&id_1), None);
        assert_eq!(lru.get(&id_0), Some(&sample_hello_world()));

        lru.invalidate(&id_0);
        assert_eq!(lru.get(&id_0), None);

        lru.insert(id_0.clone(), sample_hello_world());
        assert_eq!(lru.get(&id_0), Some(&sample_hello_world()));
    }
}
