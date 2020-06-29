use serde_derive::{
    Serialize,
    Deserialize,
};

use super::{
    block,
};

pub const WHEEL_MAGIC: u64 = 0xc0f124c9f1ba71d5;
pub const WHEEL_VERSION: usize = 1;

#[derive(Serialize, Deserialize, Debug)]
pub struct WheelHeader {
    pub magic: u64,
    pub version: usize,
    pub size_bytes: usize,
}

impl Default for WheelHeader {
    fn default() -> WheelHeader {
        WheelHeader {
            magic: WHEEL_MAGIC,
            version: WHEEL_VERSION,
            size_bytes: 0,
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub enum BlockHeader {
    EndOfFile,
    Regular(BlockHeaderRegular),
}

#[derive(Serialize, Deserialize, Default, Debug)]
pub struct BlockHeaderRegular {
    pub block_id: block::Id,
    pub block_size: usize,
}
