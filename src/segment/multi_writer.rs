use super::writer::{Writer, BLOB_HEADER_MAGIC};
use crate::{
    compression::Compressor,
    id::{IdGenerator, SegmentId},
    ValueHandle,
};
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

/// Segment writer, may write multiple segments
pub struct MultiWriter {
    folder: PathBuf,
    target_size: u64,

    writers: Vec<Writer>,

    id_generator: IdGenerator,

    compression: Option<Arc<dyn Compressor>>,
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

            compression: None,
        })
    }

    /// Sets the compression method
    #[must_use]
    pub fn use_compression(mut self, compressor: Arc<dyn Compressor>) -> Self {
        self.compression = Some(compressor.clone());
        self.get_active_writer_mut().compression = Some(compressor);
        self
    }

    #[doc(hidden)]
    #[must_use]
    pub fn get_active_writer(&self) -> &Writer {
        // NOTE: initialized in constructor
        #[allow(clippy::expect_used)]
        self.writers.last().expect("should exist")
    }

    fn get_active_writer_mut(&mut self) -> &mut Writer {
        // NOTE: initialized in constructor
        #[allow(clippy::expect_used)]
        self.writers.last_mut().expect("should exist")
    }

    /// Returns the [`ValueHandle`] for the next written blob.
    ///
    /// This can be used to index an item into an external `Index`.
    #[must_use]
    pub fn get_next_value_handle(&self, key: &[u8]) -> ValueHandle {
        ValueHandle {
            offset: self.offset(key),
            segment_id: self.segment_id(),
        }
    }

    #[must_use]
    fn offset(&self, key: &[u8]) -> u64 {
        self.get_active_writer().offset()
        // NOTE: Point to the value record, not the key
        // The key is not really needed when dereferencing a value handle
            + (BLOB_HEADER_MAGIC.len()
            + std::mem::size_of::<u16>()
            + key.len()
        ) as u64
    }

    #[must_use]
    fn segment_id(&self) -> SegmentId {
        self.get_active_writer().segment_id()
    }

    /// Sets up a new writer for the next segment
    fn rotate(&mut self) -> crate::Result<()> {
        log::debug!("Rotating segment writer");

        let new_segment_id = self.id_generator.next();
        let segment_path = self.folder.join(new_segment_id.to_string());

        let mut new_writer = Writer::new(segment_path, new_segment_id)?;

        if let Some(compressor) = &self.compression {
            new_writer = new_writer.use_compression(compressor.clone());
        }

        self.writers.push(new_writer);

        Ok(())
    }

    /// Writes an item
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn write<K: AsRef<[u8]>, V: AsRef<[u8]>>(
        &mut self,
        key: K,
        value: V,
    ) -> crate::Result<u32> {
        let key = key.as_ref();
        let value = value.as_ref();

        let target_size = self.target_size;

        // Write actual value into segment
        let writer = self.get_active_writer_mut();
        let bytes_written = writer.write(key, value)?;

        // Check for segment size target, maybe rotate to next writer
        if writer.offset() >= target_size {
            writer.flush()?;
            self.rotate()?;
        }

        Ok(bytes_written)
    }

    pub(crate) fn finish(mut self) -> crate::Result<Vec<Writer>> {
        let writer = self.get_active_writer_mut();

        if writer.item_count > 0 {
            writer.flush()?;
        }

        // IMPORTANT: We cannot finish the index writer here
        // The writers first need to be registered into the value log

        Ok(self.writers)
    }
}
