use super::writer::Writer;
use crate::id::{IdGenerator, SegmentId};
use std::path::{Path, PathBuf};

/// Segment writer, may write multiple segments
pub struct MultiWriter {
    folder: PathBuf,
    target_size: u64,
    writers: Vec<Writer>,
    id_generator: IdGenerator,
}

impl MultiWriter {
    /// Initializes a new segment writer.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    #[doc(hidden)]
    pub fn new<P: AsRef<Path>>(
        id_generator: IdGenerator,
        target_size: u64,
        folder: P,
    ) -> std::io::Result<Self> {
        let folder = folder.as_ref();

        let segment_id = id_generator.next();
        let segment_path = folder.join(segment_id.to_string());

        Ok(Self {
            id_generator,
            folder: folder.into(),
            target_size,
            writers: vec![Writer::new(segment_path, segment_id)?],
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
    pub fn segment_id(&self) -> SegmentId {
        self.get_active_writer().segment_id()
    }

    /// Sets up a new writer for the next segment
    fn rotate(&mut self) -> crate::Result<()> {
        log::debug!("Rotating segment writer");

        let new_segment_id = self.id_generator.next();

        self.writers
            .push(Writer::new(&self.folder, new_segment_id)?);

        Ok(())
    }

    /// Writes an item
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn write(&mut self, key: &[u8], value: &[u8]) -> crate::Result<u32> {
        let target_size = self.target_size;

        let writer = self.get_active_writer_mut();
        let bytes_written = writer.write(key, value)?;

        if writer.offset() >= target_size {
            writer.flush()?;
            self.rotate()?;
        }

        Ok(bytes_written)
    }

    pub(crate) fn finish(mut self) -> crate::Result<Vec<Writer>> {
        let writer = self.get_active_writer_mut();
        writer.flush()?;
        Ok(self.writers)
    }
}
