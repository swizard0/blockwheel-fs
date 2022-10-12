use serde::{
    Serialize,
    Deserialize,
};

use bincode::{
    Options,
};

use crate::{
    block,
};

pub const WHEEL_MAGIC: u64 = 0xc0f124c9f1ba71d5;
pub const WHEEL_VERSION: usize = 1;

#[derive(Serialize, Deserialize, Debug)]
pub struct WheelHeader {
    pub magic: u64,
    pub version: usize,
    pub size_bytes: u64,
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

pub const BLOCK_MAGIC: u64 = 0x1af107518a38d0cf;

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, Debug)]
pub struct BlockHeader {
    pub magic: u64,
    pub block_id: block::Id,
    pub block_size: usize,
}

impl Default for BlockHeader {
    fn default() -> BlockHeader {
        BlockHeader {
            magic: BLOCK_MAGIC,
            block_id: block::Id::default(),
            block_size: 0,
        }
    }
}

pub const TOMBSTONE_TAG_MAGIC: u64 = 0xce1063910922bdd5;

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, Debug)]
pub struct TombstoneTag {
    pub magic: u64,
}

impl Default for TombstoneTag {
    fn default() -> TombstoneTag {
        TombstoneTag {
            magic: TOMBSTONE_TAG_MAGIC,
        }
    }
}

pub const COMMIT_TAG_MAGIC: u64 = 0xdb68d2d17dfe9811;

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, Debug)]
pub struct CommitTag {
    pub magic: u64,
    pub block_id: block::Id,
    pub crc: u64,
}

impl Default for CommitTag {
    fn default() -> CommitTag {
        CommitTag {
            magic: COMMIT_TAG_MAGIC,
            block_id: block::Id::default(),
            crc: 0,
        }
    }
}

pub const TERMINATOR_TAG_MAGIC: u64 = 0x375d8f85e8daab4a;

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, Debug)]
pub struct TerminatorTag {
    pub magic: u64,
}

impl Default for TerminatorTag {
    fn default() -> TerminatorTag {
        TerminatorTag {
            magic: TERMINATOR_TAG_MAGIC,
        }
    }
}

#[derive(Clone, PartialEq, Eq, Default, Debug)]
pub struct Layout {
    pub wheel_header_size: usize,
    pub block_header_size: usize,
    pub commit_tag_size: usize,
    pub terminator_tag_size: usize,
}

#[derive(Debug)]
pub enum LayoutSerializeError {
    WheelHeader(bincode::Error),
    BlockHeader(bincode::Error),
    CommitTag(bincode::Error),
    TerminatorTag(bincode::Error),
}

impl Layout {
    pub fn calculate(mut work_block: &mut Vec<u8>) -> Result<Layout, LayoutSerializeError> {
        let mut cursor = work_block.len();

        bincode_options()
            .serialize_into(&mut work_block, &WheelHeader::default())
            .map_err(LayoutSerializeError::WheelHeader)?;
        let wheel_header_size = work_block.len() - cursor;
        cursor = work_block.len();

        bincode_options()
            .serialize_into(&mut work_block, &BlockHeader::default())
            .map_err(LayoutSerializeError::BlockHeader)?;
        let block_header_size = work_block.len() - cursor;
        cursor = work_block.len();

        bincode_options()
            .serialize_into(&mut work_block, &CommitTag::default())
            .map_err(LayoutSerializeError::CommitTag)?;
        let commit_tag_size = work_block.len() - cursor;
        cursor = work_block.len();

        bincode_options()
            .serialize_into(&mut work_block, &TerminatorTag::default())
            .map_err(LayoutSerializeError::TerminatorTag)?;
        let terminator_tag_size = work_block.len() - cursor;

        work_block.clear();
        Ok(Layout {
            wheel_header_size,
            block_header_size,
            commit_tag_size,
            terminator_tag_size,
        })
    }

    pub fn service_size_min(&self) -> usize {
        self.wheel_header_size
            + self.terminator_tag_size
    }

    pub fn data_size_block_min(&self) -> usize {
        self.block_header_size
            + self.commit_tag_size
    }
}

pub fn bincode_options() -> impl Options {
    bincode::DefaultOptions::new()
        .with_no_limit()
        .with_big_endian()
        .with_fixint_encoding()
        .allow_trailing_bytes()
}
