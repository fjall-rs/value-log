use crate::serde::{DeserializeError, Serializable, SerializeError};
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use std::{
    fs::File,
    io::{BufReader, Read, Seek, Write},
    path::Path,
    sync::atomic::AtomicU64,
};

pub const TRAILER_MAGIC: &[u8] = &[b'F', b'J', b'L', b'L', b'T', b'R', b'L', b'1'];
pub const TRAILER_SIZE: usize = 256;

#[derive(Debug)]
pub struct SegmentFileTrailer {
    pub item_count: u64,
    pub total_bytes: u64,
    pub total_uncompressed_bytes: u64,
    // TODO: key range
}

impl SegmentFileTrailer {
    pub fn from_file<P: AsRef<Path>>(path: P) -> crate::Result<Self> {
        let file = File::open(path)?;
        let mut reader = BufReader::new(file);
        reader.seek(std::io::SeekFrom::End(-(TRAILER_SIZE as i64)))?;

        let item_count = reader.read_u64::<BigEndian>()?;
        let total_bytes = reader.read_u64::<BigEndian>()?;
        let total_uncompressed_bytes = reader.read_u64::<BigEndian>()?;

        let remaining_padding = TRAILER_SIZE - 3 * std::mem::size_of::<u64>() - TRAILER_MAGIC.len();
        reader.seek_relative(remaining_padding as i64)?;

        // Check trailer magic
        let mut magic = [0u8; TRAILER_MAGIC.len()];
        reader.read_exact(&mut magic)?;

        if magic != TRAILER_MAGIC {
            return Err(crate::Error::Deserialize(DeserializeError::InvalidHeader(
                "SegmentMetadata",
            )));
        }

        Ok(Self {
            item_count,
            total_bytes,
            total_uncompressed_bytes,
        })
    }
}

impl Serializable for SegmentFileTrailer {
    fn serialize<W: Write>(&self, writer: &mut W) -> Result<(), SerializeError> {
        let mut v = Vec::with_capacity(TRAILER_SIZE);

        v.write_u64::<BigEndian>(self.item_count)?;
        v.write_u64::<BigEndian>(self.total_bytes)?;
        v.write_u64::<BigEndian>(self.total_uncompressed_bytes)?;
        // self.key_range.serialize(writer)?; // TODO:

        // Pad with remaining bytes
        v.resize(TRAILER_SIZE - TRAILER_MAGIC.len(), 0);

        v.write_all(TRAILER_MAGIC)?;

        assert_eq!(
            v.len(),
            TRAILER_SIZE,
            "segment file trailer has invalid size"
        );

        writer.write_all(&v)?;

        Ok(())
    }
}

#[derive(Debug)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
pub struct Stats {
    pub(crate) persisted: SegmentFileTrailer,

    pub(crate) stale_items: AtomicU64,
    pub(crate) stale_bytes: AtomicU64,
}

impl std::ops::Deref for Stats {
    type Target = SegmentFileTrailer;

    fn deref(&self) -> &Self::Target {
        &self.persisted
    }
}

impl Stats {
    pub(crate) fn mark_as_stale(&self) {
        self.stale_items
            .store(self.item_count, std::sync::atomic::Ordering::Release);

        self.stale_bytes.store(
            self.total_uncompressed_bytes,
            std::sync::atomic::Ordering::Release,
        );
    }

    pub fn is_stale(&self) -> bool {
        self.stale_items() == self.item_count
    }

    /// Returns the percent of dead items in the segment
    pub fn stale_ratio(&self) -> f32 {
        let dead = self.stale_items() as f32;
        if dead == 0.0 {
            return 0.0;
        }

        dead / self.item_count as f32
    }

    /// Returns the amount of dead items in the segment
    pub fn stale_items(&self) -> u64 {
        self.stale_items.load(std::sync::atomic::Ordering::Acquire)
    }

    /// Returns the amount of dead bytes in the segment
    pub fn stale_bytes(&self) -> u64 {
        self.stale_bytes.load(std::sync::atomic::Ordering::Acquire)
    }
}
