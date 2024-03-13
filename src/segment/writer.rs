use byteorder::{BigEndian, WriteBytesExt};
use std::{
    fs::File,
    io::{BufWriter, Write},
    path::{Path, PathBuf},
    sync::Arc,
};

/// Segment writer
pub struct Writer {
    pub(crate) path: PathBuf,
    pub(crate) segment_id: Arc<str>,

    inner: BufWriter<File>,

    offset: u64,
    pub(crate) item_count: u64,
}

impl Writer {
    /// Initializes a new segment writer.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    #[doc(hidden)]
    pub fn new<P: AsRef<Path>>(segment_id: Arc<str>, path: P) -> std::io::Result<Self> {
        let path = path.as_ref();
        let folder = path.parent().expect("should have parent directory");

        std::fs::create_dir_all(folder)?;
        let file = File::create(path)?;

        Ok(Self {
            path: path.to_owned(),
            segment_id,
            inner: BufWriter::new(file),
            offset: 0,
            item_count: 0,
        })
    }

    /// Returns the current offset in the file.
    ///
    /// This can be used to index an item into an external `Index`.
    ///
    /// # Examples
    ///
    /// ```
    /// # use value_log::SegmentWriter;
    /// # use std::collections::HashMap;
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # std::fs::create_dir_all(folder.path().join("segments"))?;
    /// # let mut writer = SegmentWriter::new(1_000, folder)?;
    /// # let mut index = HashMap::new();
    /// #
    /// # let items = [(b"1", b"1"), (b"2", b"2")];
    /// #
    /// for (key, value) in items {  
    ///     index.insert(key, writer.offset(key));
    ///     writer.write(key, value)?;
    /// }
    /// #
    /// # Ok::<(), value_log::Error>(())
    /// ```
    #[must_use]
    pub fn offset(&self) -> u64 {
        self.offset
    }

    /// Returns the segment ID
    #[must_use]
    pub fn segment_id(&self) -> Arc<str> {
        self.segment_id.clone()
    }

    /// Writes an item into the file
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn write(&mut self, key: &[u8], value: &[u8]) -> std::io::Result<()> {
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(value);
        let crc = hasher.finalize();

        self.inner.write_u16::<BigEndian>(key.len() as u16)?;
        self.inner.write_all(key)?;
        self.inner.write_u32::<BigEndian>(crc)?;
        self.inner.write_u32::<BigEndian>(value.len() as u32)?;
        self.inner.write_all(value)?;

        // Key
        self.offset += std::mem::size_of::<u16>() as u64;
        self.offset += key.len() as u64;

        // CRC
        self.offset += std::mem::size_of::<u32>() as u64;

        // TODO: compress

        // Value
        self.offset += std::mem::size_of::<u32>() as u64;
        self.offset += value.len() as u64;

        self.item_count += 1;

        Ok(())
    }

    pub(crate) fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()?;
        self.inner.get_mut().sync_all()?;
        Ok(())
    }
}
