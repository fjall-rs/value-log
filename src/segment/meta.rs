use crate::{
    key_range::KeyRange,
    serde::{Deserializable, DeserializeError, Serializable, SerializeError},
    CompressionType,
};
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use std::io::{Read, Write};

pub const METADATA_HEADER_MAGIC: &[u8] = &[b'V', b'L', b'O', b'G', b'S', b'M', b'D', b'1'];

#[derive(Debug)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
pub struct Metadata {
    /// Number of KV-pairs in the segment
    pub item_count: u64,

    /// compressed size in bytes (on disk) (without the fixed size trailer)
    pub compressed_bytes: u64,

    /// true size in bytes (if no compression were used)
    pub total_uncompressed_bytes: u64,

    /// What type of compression is used
    pub compression: CompressionType,

    /// Key range
    pub key_range: KeyRange,
}

impl Serializable for Metadata {
    fn serialize<W: Write>(&self, writer: &mut W) -> Result<(), SerializeError> {
        // Write header
        writer.write_all(METADATA_HEADER_MAGIC)?;

        writer.write_u64::<BigEndian>(self.item_count)?;
        writer.write_u64::<BigEndian>(self.compressed_bytes)?;
        writer.write_u64::<BigEndian>(self.total_uncompressed_bytes)?;

        self.compression.serialize(writer)?;

        self.key_range.serialize(writer)?;

        Ok(())
    }
}

impl Deserializable for Metadata {
    fn deserialize<R: Read>(reader: &mut R) -> Result<Self, DeserializeError> {
        // Check header
        let mut magic = [0u8; METADATA_HEADER_MAGIC.len()];
        reader.read_exact(&mut magic)?;

        if magic != METADATA_HEADER_MAGIC {
            return Err(DeserializeError::InvalidHeader("SegmentMetadata"));
        }

        let item_count = reader.read_u64::<BigEndian>()?;
        let compressed_bytes = reader.read_u64::<BigEndian>()?;
        let total_uncompressed_bytes = reader.read_u64::<BigEndian>()?;

        let compression = CompressionType::deserialize(reader)?;

        let key_range = KeyRange::deserialize(reader)?;

        Ok(Self {
            item_count,
            compressed_bytes,
            total_uncompressed_bytes,
            compression,
            key_range,
        })
    }
}
