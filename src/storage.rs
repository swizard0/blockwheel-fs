use alloc_pool::{
    bytes::{
        BytesMut,
    },
};

use alloc_pool_pack::{
    integer,
    Source,
    ReadFromSource,
    WriteToBytesMut,
};

use crate::{
    block,
};

const WHEEL_MAGIC: u64 = 0xc0f124c9f1ba71d5;
const WHEEL_VERSION: u16 = 2;

// WheelHeader

#[derive(Debug)]
pub struct WheelHeader {
    pub magic: u64,
    pub version: u16,
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

impl WriteToBytesMut for WheelHeader {
    fn write_to_bytes_mut(&self, bytes_mut: &mut BytesMut) {
        self.magic.write_to_bytes_mut(bytes_mut);
        self.version.write_to_bytes_mut(bytes_mut);
        self.size_bytes.write_to_bytes_mut(bytes_mut);
    }
}

#[derive(Debug)]
pub enum ReadWheelHeaderError {
    Magic(integer::ReadIntegerError),
    Version(integer::ReadIntegerError),
    SizeBytes(integer::ReadIntegerError),
    InvalidMagic { expected: u64, provided: u64, },
    InvalidVersion { expected: u16, provided: u16, },
}

impl ReadFromSource for WheelHeader {
    type Error = ReadWheelHeaderError;

    fn read_from_source<S>(source: &mut S) -> Result<Self, Self::Error> where S: Source {
        let magic = u64::read_from_source(source)
            .map_err(ReadWheelHeaderError::Magic)?;
        if magic != WHEEL_MAGIC {
            return Err(ReadWheelHeaderError::InvalidMagic {
                expected: WHEEL_MAGIC,
                provided: magic,
            });
        }
        let version = u16::read_from_source(source)
            .map_err(ReadWheelHeaderError::Version)?;
        if version != WHEEL_VERSION {
            return Err(ReadWheelHeaderError::InvalidVersion {
                expected: WHEEL_VERSION,
                provided: version,
            });
        }
        let size_bytes = u64::read_from_source(source)
            .map_err(ReadWheelHeaderError::SizeBytes)?;
        Ok(WheelHeader { magic, version, size_bytes, })
    }
}

// BlockHeader

const BLOCK_MAGIC: u64 = 0x1af107518a38d0cf;

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct BlockHeader {
    pub magic: u64,
    pub block_id: block::Id,
    pub block_size: u64,
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

impl WriteToBytesMut for BlockHeader {
    fn write_to_bytes_mut(&self, bytes_mut: &mut BytesMut) {
        self.magic.write_to_bytes_mut(bytes_mut);
        self.block_id.write_to_bytes_mut(bytes_mut);
        self.block_size.write_to_bytes_mut(bytes_mut);
    }
}

#[derive(Debug)]
pub enum ReadBlockHeaderError {
    Magic(integer::ReadIntegerError),
    BlockId(block::ReadIdError),
    BlockSize(integer::ReadIntegerError),
    InvalidMagic { expected: u64, provided: u64, },
}

impl ReadFromSource for BlockHeader {
    type Error = ReadBlockHeaderError;

    fn read_from_source<S>(source: &mut S) -> Result<Self, Self::Error> where S: Source {
        let magic = u64::read_from_source(source)
            .map_err(ReadBlockHeaderError::Magic)?;
        if magic != BLOCK_MAGIC {
            return Err(ReadBlockHeaderError::InvalidMagic {
                expected: BLOCK_MAGIC,
                provided: magic,
            });
        }
        let block_id = block::Id::read_from_source(source)
            .map_err(ReadBlockHeaderError::BlockId)?;
        let block_size = u64::read_from_source(source)
            .map_err(ReadBlockHeaderError::BlockSize)?;
        Ok(BlockHeader { magic, block_id, block_size, })
    }
}

// TombstoneTag

const TOMBSTONE_TAG_MAGIC: u64 = 0xce1063910922bdd5;

#[derive(Clone, PartialEq, Eq,  Debug)]
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

impl WriteToBytesMut for TombstoneTag {
    fn write_to_bytes_mut(&self, bytes_mut: &mut BytesMut) {
        self.magic.write_to_bytes_mut(bytes_mut);
    }
}

#[derive(Debug)]
pub enum ReadTombstoneTagError {
    Magic(integer::ReadIntegerError),
    InvalidMagic { expected: u64, provided: u64, },
}

impl ReadFromSource for TombstoneTag {
    type Error = ReadTombstoneTagError;

    fn read_from_source<S>(source: &mut S) -> Result<Self, Self::Error> where S: Source {
        let magic = u64::read_from_source(source)
            .map_err(ReadTombstoneTagError::Magic)?;
        if magic != TOMBSTONE_TAG_MAGIC {
            return Err(ReadTombstoneTagError::InvalidMagic {
                expected: TOMBSTONE_TAG_MAGIC,
                provided: magic,
            });
        }
        Ok(TombstoneTag { magic, })
    }
}

// CommitTag

const COMMIT_TAG_MAGIC: u64 = 0xdb68d2d17dfe9811;

#[derive(Clone, PartialEq, Eq, Debug)]
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

impl WriteToBytesMut for CommitTag {
    fn write_to_bytes_mut(&self, bytes_mut: &mut BytesMut) {
        self.magic.write_to_bytes_mut(bytes_mut);
        self.block_id.write_to_bytes_mut(bytes_mut);
        self.crc.write_to_bytes_mut(bytes_mut);
    }
}

#[derive(Debug)]
pub enum ReadCommitTagError {
    Magic(integer::ReadIntegerError),
    BlockId(block::ReadIdError),
    Crc(integer::ReadIntegerError),
    InvalidMagic { expected: u64, provided: u64, },
}

impl ReadFromSource for CommitTag {
    type Error = ReadCommitTagError;

    fn read_from_source<S>(source: &mut S) -> Result<Self, Self::Error> where S: Source {
        let magic = u64::read_from_source(source)
            .map_err(ReadCommitTagError::Magic)?;
        if magic != COMMIT_TAG_MAGIC {
            return Err(ReadCommitTagError::InvalidMagic {
                expected: COMMIT_TAG_MAGIC,
                provided: magic,
            });
        }
        let block_id = block::Id::read_from_source(source)
            .map_err(ReadCommitTagError::BlockId)?;
        let crc = u64::read_from_source(source)
            .map_err(ReadCommitTagError::Crc)?;
        Ok(CommitTag { magic, block_id, crc, })
    }
}

// TerminatorTag

const TERMINATOR_TAG_MAGIC: u64 = 0x375d8f85e8daab4a;

#[derive(Clone, PartialEq, Eq, Debug)]
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

impl WriteToBytesMut for TerminatorTag {
    fn write_to_bytes_mut(&self, bytes_mut: &mut BytesMut) {
        self.magic.write_to_bytes_mut(bytes_mut);
    }
}

#[derive(Debug)]
pub enum ReadTerminatorTagError {
    Magic(integer::ReadIntegerError),
    InvalidMagic { expected: u64, provided: u64, },
}

impl ReadFromSource for TerminatorTag {
    type Error = ReadTerminatorTagError;

    fn read_from_source<S>(source: &mut S) -> Result<Self, Self::Error> where S: Source {
        let magic = u64::read_from_source(source)
            .map_err(ReadTerminatorTagError::Magic)?;
        if magic != TERMINATOR_TAG_MAGIC {
            return Err(ReadTerminatorTagError::InvalidMagic {
                expected: TERMINATOR_TAG_MAGIC,
                provided: magic,
            });
        }
        Ok(TerminatorTag { magic, })
    }
}

// Layout

#[derive(Clone, PartialEq, Eq, Default, Debug)]
pub struct Layout {
    pub wheel_header_size: usize,
    pub block_header_size: usize,
    pub commit_tag_size: usize,
    pub terminator_tag_size: usize,
}

impl Layout {
    pub fn calculate(work_block: &mut BytesMut) -> Layout {
        let mut cursor = work_block.len();

        WheelHeader::default().write_to_bytes_mut(work_block);
        let wheel_header_size = work_block.len() - cursor;
        cursor = work_block.len();

        BlockHeader::default().write_to_bytes_mut(work_block);
        let block_header_size = work_block.len() - cursor;
        cursor = work_block.len();

        CommitTag::default().write_to_bytes_mut(work_block);
        let commit_tag_size = work_block.len() - cursor;
        cursor = work_block.len();

        TerminatorTag::default().write_to_bytes_mut(work_block);
        let terminator_tag_size = work_block.len() - cursor;
        work_block.clear();

        Layout {
            wheel_header_size,
            block_header_size,
            commit_tag_size,
            terminator_tag_size,
        }
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
