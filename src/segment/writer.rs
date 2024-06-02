use crate::id::SegmentId;
use byteorder::{BigEndian, WriteBytesExt};
use std::{
    fs::File,
    io::{BufWriter, Write},
    path::{Path, PathBuf},
};

/// Segment writer
pub struct Writer {
    pub(crate) folder: PathBuf,
    pub(crate) segment_id: SegmentId,

    inner: BufWriter<File>,

    offset: u64,
    pub(crate) item_count: u64,

    pub(crate) written_blob_bytes: u64,
    pub(crate) uncompressed_bytes: u64,
}

impl Writer {
    /// Initializes a new segment writer.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    #[doc(hidden)]
    pub fn new<P: AsRef<Path>>(segment_id: SegmentId, path: P) -> std::io::Result<Self> {
        let path = path.as_ref();
        let folder = path.parent().expect("should have parent directory");

        std::fs::create_dir_all(folder)?;
        let file = File::create(path)?;

        Ok(Self {
            folder: folder.into(),
            segment_id,
            inner: BufWriter::new(file),
            offset: 0,
            item_count: 0,
            written_blob_bytes: 0,
            uncompressed_bytes: 0,
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

        self.uncompressed_bytes += value.len() as u64;

        #[cfg(feature = "lz4")]
        let value = lz4_flex::compress_prepend_size(value);

        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&value);
        let crc = hasher.finalize();

        // NOTE: Truncation is okay and actually needed
        #[allow(clippy::cast_possible_truncation)]
        self.inner.write_u16::<BigEndian>(key.len() as u16)?;
        self.inner.write_all(key)?;
        self.inner.write_u32::<BigEndian>(crc)?;

        // NOTE: Truncation is okay and actually needed
        #[allow(clippy::cast_possible_truncation)]
        self.inner.write_u32::<BigEndian>(value.len() as u32)?;
        self.inner.write_all(&value)?;

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

    pub(crate) fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()?;
        self.inner.get_mut().sync_all()?;
        Ok(())
    }
}
