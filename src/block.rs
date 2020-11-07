use serde_derive::{
    Serialize,
    Deserialize,
};

use alloc_pool::bytes::Bytes;

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

pub fn crc(bytes: &[u8]) -> u64 {
    crc::crc64::checksum_ecma(bytes)
}

#[derive(Debug)]
pub enum CrcError {
    CrcTaskJoin(tokio::task::JoinError),
}

pub async fn crc_bytes(bytes: Bytes) -> Result<u64, CrcError> {
    let crc_task = tokio::task::spawn_blocking(move || {
        crc::crc64::checksum_ecma(&bytes)
    });
    crc_task.await.map_err(CrcError::CrcTaskJoin)
}
