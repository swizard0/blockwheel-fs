use std::{
    sync::{
        Arc,
        atomic::{
            Ordering,
            AtomicU64,
            AtomicUsize,
        },
    },
    ops::{
        Deref,
        DerefMut,
    },
};

use serde_derive::{
    Serialize,
    Deserialize,
};

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Debug)]
pub struct Id {
    serial: u64,
}

impl Default for Id {
    fn default() -> Id {
        Id::init()
    }
}

impl Id {
    pub fn init() -> Id {
        Id {
            serial: 0,
        }
    }

    pub fn next(&self) -> Id {
        Id {
            serial: self.serial + 1,
        }
    }
}

#[derive(Clone, Debug)]
pub struct Bytes {
    bytes: Arc<Vec<u8>>,
    pub(crate) release: Release,
}

#[derive(Debug)]
pub(crate) struct Release {
    pub(crate) key: Arc<AtomicU64>,
    pub(crate) prev: Arc<AtomicUsize>,
    pub(crate) head: Arc<AtomicUsize>,
}

pub(crate) fn make_release_key(index_succ: usize, refs_count: usize) -> u64 {
    assert!(index_succ < (u32::MAX as usize));
    assert!(refs_count < (u32::MAX as usize));
    ((index_succ as u64) << 32) | (refs_count as u64)
}

pub(crate) fn split_release_key(release_key: u64) -> (usize, usize) {
    let index_succ = (release_key >> 32) as usize;
    let refs_count = (release_key & (u32::MAX as u64)) as usize;
    (index_succ, refs_count)
}

impl Bytes {
    pub fn into_mut(self) -> Result<BytesMut, Bytes> {
        match Arc::try_unwrap(self.bytes) {
            Ok(bytes) =>
                Ok(BytesMut {
                    bytes,
                    release_head: self.release.head.clone(),
                }),
            Err(bytes) =>
                Err(Bytes {
                    bytes,
                    ..self
                }),
        }
    }
}

impl Drop for Release {
    fn drop(&mut self) {
        let prev_release_key = self.key.fetch_sub(1, Ordering::SeqCst);
        let (index_succ, refs_count) = split_release_key(prev_release_key);
        if index_succ != 0 && refs_count == 2 {
            // move to release list in case of repayed and unique reference
            let mut release_head = self.head.load(Ordering::SeqCst);
            loop {
                self.prev.store(release_head, Ordering::Relaxed);
                match self.head.compare_exchange(release_head, index_succ, Ordering::SeqCst, Ordering::Relaxed) {
                    Ok(..) =>
                        break,
                    Err(value) =>
                        release_head = value,
                }
            }
        }
    }
}

impl Clone for Release {
    fn clone(&self) -> Release {
        self.key.fetch_add(1, Ordering::SeqCst);
        Release {
            key: self.key.clone(),
            prev: self.prev.clone(),
            head: self.head.clone(),
        }
    }
}

impl AsRef<[u8]> for Bytes {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        &*self.bytes
    }
}

impl Deref for Bytes {
    type Target = [u8];

    #[inline]
    fn deref(&self) -> &[u8] {
        self.as_ref()
    }
}

impl PartialEq for Bytes {
    fn eq(&self, other: &Bytes) -> bool {
        self.bytes == other.bytes
    }
}

#[derive(Debug)]
pub struct BytesMut {
    bytes: Vec<u8>,
    pub(crate) release_head: Arc<AtomicUsize>,
}

impl BytesMut {
    pub(crate) fn new(release_head: Arc<AtomicUsize>) -> BytesMut {
        BytesMut {
            bytes: Vec::new(),
            release_head,
        }
    }

    pub(crate) fn new_detached() -> BytesMut {
        Self::new(Arc::new(AtomicUsize::new(0)))
    }

    pub fn freeze(self) -> Bytes {
        Bytes {
            bytes: Arc::new(self.bytes),
            release: Release {
                key: Arc::new(AtomicU64::new(make_release_key(0, 1))),
                prev: Arc::new(AtomicUsize::new(0)),
                head: self.release_head,
            },
        }
    }
}

impl AsRef<[u8]> for BytesMut {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

impl Deref for BytesMut {
    type Target = Vec<u8>;

    #[inline]
    fn deref(&self) -> &Vec<u8> {
        &self.bytes
    }
}

impl AsMut<[u8]> for BytesMut {
    fn as_mut(&mut self) -> &mut [u8] {
        &mut self.bytes
    }
}

impl DerefMut for BytesMut {
    #[inline]
    fn deref_mut(&mut self) -> &mut Vec<u8> {
        &mut self.bytes
    }
}
