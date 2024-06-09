use super::{meta::Metadata, trailer::SegmentFileTrailer};
use crate::{id::SegmentId, key_range::KeyRange, serde::Serializable, value::UserKey};
use byteorder::{BigEndian, WriteBytesExt};
use std::{
    fs::File,
    io::{BufWriter, Seek, Write},
    path::{Path, PathBuf},
};

/// Segment writer
pub struct Writer {
    pub(crate) path: PathBuf,
    pub(crate) segment_id: SegmentId,

    writer: BufWriter<File>,

    offset: u64,

    pub(crate) item_count: u64,
    pub(crate) written_blob_bytes: u64,
    pub(crate) uncompressed_bytes: u64,

    pub first_key: Option<UserKey>,
    pub last_key: Option<UserKey>,
}

impl Writer {
    /// Initializes a new segment writer.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    #[doc(hidden)]
    pub fn new<P: AsRef<Path>>(path: P, segment_id: SegmentId) -> std::io::Result<Self> {
        let path = path.as_ref();

        let file = File::create(path)?;

        Ok(Self {
            path: path.into(),
            segment_id,
            writer: BufWriter::new(file),
            offset: 0,
            item_count: 0,
            written_blob_bytes: 0,
            uncompressed_bytes: 0,

            first_key: None,
            last_key: None,
        })
    }

    /// Returns the current offset in the file.
    ///
    /// This can be used to index an item into an external `Index`.
    #[must_use]
    pub fn offset(&self) -> u64 {
        self.offset
    }

    /// Returns the segment ID
    #[must_use]
    pub fn segment_id(&self) -> SegmentId {
        self.segment_id
    }

    /// Writes an item into the file
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    ///
    /// # Panics
    ///
    /// Panics if the key length is empty or greater than 2^16, or the value length is greater than 2^32.
    pub fn write(&mut self, key: &[u8], value: &[u8]) -> std::io::Result<u32> {
        assert!(!key.is_empty());
        assert!(key.len() <= u16::MAX.into());
        assert!(u32::try_from(value.len()).is_ok());

        if self.first_key.is_none() {
            self.first_key = Some(key.into());
        }
        self.last_key = Some(key.into());

        self.uncompressed_bytes += value.len() as u64;

        #[cfg(feature = "lz4")]
        let value = lz4_flex::compress_prepend_size(value);

        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&value);
        let crc = hasher.finalize();

        // NOTE: Truncation is okay and actually needed
        #[allow(clippy::cast_possible_truncation)]
        self.writer.write_u16::<BigEndian>(key.len() as u16)?;
        self.writer.write_all(key)?;

        self.writer.write_u32::<BigEndian>(crc)?;

        // NOTE: Truncation is okay and actually needed
        #[allow(clippy::cast_possible_truncation)]
        self.writer.write_u32::<BigEndian>(value.len() as u32)?;
        self.writer.write_all(&value)?;

        self.written_blob_bytes += value.len() as u64;

        // Key
        self.offset += std::mem::size_of::<u16>() as u64;
        self.offset += key.len() as u64;

        // CRC
        self.offset += std::mem::size_of::<u32>() as u64;

        // Value
        self.offset += std::mem::size_of::<u32>() as u64;
        self.offset += value.len() as u64;

        self.item_count += 1;

        // NOTE: Truncation is okay
        #[allow(clippy::cast_possible_truncation)]
        Ok(value.len() as u32)
    }

    pub(crate) fn flush(&mut self) -> crate::Result<()> {
        let metadata_ptr = self.writer.stream_position()?;

        // Write metadata
        let metadata = Metadata {
            item_count: self.item_count,
            compressed_bytes: self.written_blob_bytes,
            total_uncompressed_bytes: self.uncompressed_bytes,
            key_range: KeyRange::new((
                self.first_key
                    .clone()
                    .expect("should have written at least 1 item"),
                self.last_key
                    .clone()
                    .expect("should have written at least 1 item"),
            )),
        };
        metadata.serialize(&mut self.writer)?;

        SegmentFileTrailer {
            metadata,
            metadata_ptr,
        }
        .serialize(&mut self.writer)?;

        self.writer.flush()?;
        self.writer.get_mut().sync_all()?;

        Ok(())
    }
}
