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
    fn write_to_bytes_mut<T>(&self, target: &mut T) where T: alloc_pool_pack::Target {
        self.serial.write_to_bytes_mut(target);
    }
}

#[derive(Debug)]
pub enum ReadIdError {
    Serial(alloc_pool_pack::integer::ReadIntegerError),
}

impl alloc_pool_pack::ReadFromSource for Id {
    type Error = ReadIdError;

    fn read_from_source<S>(source: &mut S) -> Result<Self, Self::Error> where S: alloc_pool_pack::Source {
        let serial = u64::read_from_source(source)
            .map_err(ReadIdError::Serial)?;
        Ok(Id { serial, })
    }
}
