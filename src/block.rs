use alloc_pool::{
    bytes::{
        Bytes,
        BytesMut,
    },
};

use alloc_pool_pack::{
    ReadFromBytes,
};

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
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

pub fn crc(bytes: &[u8]) -> u64 {
    let mut hasher = crc64fast::Digest::new();
    hasher.write(bytes);
    hasher.sum64()
}

impl alloc_pool_pack::WriteToBytesMut for Id {
    fn write_to_bytes_mut(&self, bytes_mut: &mut BytesMut) {
        self.serial.write_to_bytes_mut(bytes_mut);
    }
}

#[derive(Debug)]
pub enum ReadIdError {
    Serial(alloc_pool_pack::integer::ReadIntegerError),
}

impl alloc_pool_pack::ReadFromBytes for Id {
    type Error = ReadIdError;

    fn read_from_bytes(bytes: Bytes) -> Result<(Self, Bytes), Self::Error> {
        let (serial, bytes) = u64::read_from_bytes(bytes)
            .map_err(ReadIdError::Serial)?;
        Ok((Id { serial, }, bytes))
    }
}
