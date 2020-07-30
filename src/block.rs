use std::{
    sync::{
        Arc,
        atomic::{
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
    pub(crate) bytes: Arc<Vec<u8>>,
    pub(crate) release_key: Arc<AtomicUsize>,
    pub(crate) release_prev: Arc<AtomicUsize>,
    pub(crate) release_head: Arc<AtomicUsize>,
}

impl Bytes {
    pub fn into_mut(self) -> Result<BytesMut, Bytes> {
        match Arc::try_unwrap(self.bytes) {
            Ok(bytes) =>
                Ok(BytesMut {
                    bytes,
                    release_head: self.release_head,
                }),
            Err(bytes) =>
                Err(Bytes {
                    bytes,
                    ..self
                }),
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

    pub fn freeze(self) -> Bytes {
        Bytes {
            bytes: Arc::new(self.bytes),
            release_key: Arc::new(AtomicUsize::new(0)),
            release_prev: Arc::new(AtomicUsize::new(0)),
            release_head: self.release_head,
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
