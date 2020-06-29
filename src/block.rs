use std::{
    sync::Arc,
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

#[derive(Clone, Hash, Debug)]
pub struct Bytes {
    bytes: Arc<Vec<u8>>,
}

impl Bytes {
    pub fn into_mut(self) -> Result<BytesMut, Bytes> {
        match Arc::try_unwrap(self.bytes) {
            Ok(bytes) =>
                Ok(BytesMut { bytes, }),
            Err(bytes) =>
                Err(Bytes { bytes, }),
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

#[derive(Hash, Debug)]
pub struct BytesMut {
    bytes: Vec<u8>,
}

impl BytesMut {
    pub(crate) fn new() -> BytesMut {
        BytesMut {
            bytes: Vec::new(),
        }
    }

    pub fn freeze(self) -> Bytes {
        Bytes {
            bytes: Arc::new(self.bytes),
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
    type Target = [u8];

    #[inline]
    fn deref(&self) -> &[u8] {
        self.as_ref()
    }
}

impl AsMut<[u8]> for BytesMut {
    fn as_mut(&mut self) -> &mut [u8] {
        &mut self.bytes
    }
}

impl DerefMut for BytesMut {
    #[inline]
    fn deref_mut(&mut self) -> &mut [u8] {
        self.as_mut()
    }
}
