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

#[derive(Clone, PartialEq, Serialize, Deserialize, Debug)]
pub enum BlockHeader {
    EndOfFile,
    Regular(BlockHeaderRegular),
}

#[derive(Clone, PartialEq, Serialize, Deserialize, Default, Debug)]
pub struct BlockHeaderRegular {
    pub block_id: block::Id,
    pub block_size: usize,
}

pub const COMMIT_TAG_MAGIC: u64 = 0xdb68d2d17dfe9811;

#[derive(Clone, PartialEq, Serialize, Deserialize, Debug)]
pub struct CommitTag {
    pub magic: u64,
    pub block_id: block::Id,
}

impl Default for CommitTag {
    fn default() -> CommitTag {
        CommitTag {
            magic: COMMIT_TAG_MAGIC,
            block_id: block::Id::default(),
        }
    }
}

#[derive(Clone, PartialEq, Default, Debug)]
pub struct Layout {
    pub wheel_header_size: usize,
    pub eof_block_header_size: usize,
    pub regular_block_header_size: usize,
    pub commit_tag_size: usize,
}

impl Layout {
    pub fn data_size_service_min(&self) -> usize {
        self.wheel_header_size +
            self.eof_block_header_size
    }

    pub fn data_size_block_min(&self) -> usize {
        self.wheel_header_size +
            self.regular_block_header_size +
            self.commit_tag_size +
            self.eof_block_header_size
    }
}
