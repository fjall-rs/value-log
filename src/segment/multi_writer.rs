use super::writer::Writer;
use crate::id::generate_segment_id;
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

/// Segment writer, may write multiple segments
pub struct MultiWriter {
    folder: PathBuf,
    target_size: u64,
    writers: Vec<Writer>,
}

impl MultiWriter {
    /// Initializes a new segment writer.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    #[doc(hidden)]
    pub fn new<P: AsRef<Path>>(target_size: u64, folder: P) -> std::io::Result<Self> {
        let folder = folder.as_ref();
        let segment_id = generate_segment_id();
        let path = folder.join("segments").join(&*segment_id);

        Ok(Self {
            folder: folder.into(),
            target_size,
            writers: vec![Writer::new(segment_id, path)?],
        })
    }

    fn get_active_writer(&self) -> &Writer {
        self.writers.last().expect("should exist")
    }

    fn get_active_writer_mut(&mut self) -> &mut Writer {
        self.writers.last_mut().expect("should exist")
    }

    /// Returns the current offset in the file.
    ///
    /// This can be used to index an item into an external `Index`.
    #[must_use]
    pub fn offset(&self, key: &[u8]) -> u64 {
        self.get_active_writer().offset() 
        // NOTE: Point to the value record, not the key
        // The key is not really needed when dereferencing a value handle
        + std::mem::size_of::<u16>() as u64 + key.len() as u64
    }

    /// Returns the segment ID
    #[must_use]
    pub fn segment_id(&self) -> Arc<str> {
        self.get_active_writer().segment_id()
    }

    /// Sets up a new writer for the next segment
    fn rotate(&mut self) -> crate::Result<()> {
        log::debug!("Rotating segment writer");

        let new_segment_id = generate_segment_id();
        let path = self.folder.join("segments").join(&*new_segment_id);
        self.writers.push(Writer::new(new_segment_id, path)?);

        Ok(())
    }

    /// Writes an item
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn write(&mut self, key: &[u8], value: &[u8]) -> crate::Result<()> {
        let writer = self.get_active_writer_mut();
        writer.write(key, value)?;

        if writer.offset() >= self.target_size {
            self.rotate()?;
        }

        Ok(())
    }

    pub(crate) fn finish(mut self) -> crate::Result<Vec<Writer>> {
        let writer = self.get_active_writer_mut();
        writer.flush()?;
        Ok(self.writers)
    }
}
