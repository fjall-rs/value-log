use super::writer::Writer;
use crate::{
    compression::Compressor,
    id::{IdGenerator, SegmentId},
    IndexWriter, ValueHandle,
};
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

/// Segment writer, may write multiple segments
pub struct MultiWriter<W: IndexWriter> {
    folder: PathBuf,
    target_size: u64,

    writers: Vec<Writer>,
    pub(crate) index_writer: W, // TODO: only need a (mutable) reference??

    id_generator: IdGenerator,

    compression: Option<Arc<dyn Compressor>>,

    blob_separation_size: usize,
}

impl<W: IndexWriter> MultiWriter<W> {
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
        index_writer: W,
    ) -> std::io::Result<Self> {
        let folder = folder.as_ref();

        let segment_id = id_generator.next();
        let segment_path = folder.join(segment_id.to_string());

        Ok(Self {
            id_generator,
            folder: folder.into(),
            target_size,

            writers: vec![Writer::new(segment_path, segment_id)?],
            index_writer,

            compression: None,
            blob_separation_size: 2_048,
        })
    }

    /// Sets the separation threshold for blobs
    #[must_use]
    pub fn blob_separation_size(mut self, bytes: usize) -> Self {
        self.blob_separation_size = bytes;
        self
    }

    /// Sets the compression method
    #[must_use]
    pub fn use_compression(mut self, compressor: Arc<dyn Compressor>) -> Self {
        self.compression = Some(compressor.clone());
        self.get_active_writer_mut().compression = Some(compressor);
        self
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
    pub(crate) fn offset(&self, key: &[u8]) -> u64 {
        self.get_active_writer().offset()
        // NOTE: Point to the value record, not the key
        // The key is not really needed when dereferencing a value handle
        + std::mem::size_of::<u16>() as u64 + key.len() as u64
    }

    /// Returns the segment ID
    #[must_use]
    pub(crate) fn segment_id(&self) -> SegmentId {
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

        // Give value handle to index writer
        let bytes_written = if value.len() >= self.blob_separation_size {
            let segment_id = self.segment_id();
            let offset = self.offset(key);
            let vhandle = ValueHandle { segment_id, offset };

            log::trace!(
                "inserting indirection: {vhandle:?} => {:?}",
                String::from_utf8_lossy(key)
            );

            self.index_writer
                .insert_indirect(key, vhandle, value.len() as u32)?;

            // Write actual value into segment
            let writer = self.get_active_writer_mut();

            writer.write(key, value)?
        } else {
            log::trace!("GC: inserting direct: {:?}", String::from_utf8_lossy(key));
            self.index_writer.insert_direct(key, value)?;

            0
        };

        // Write actual value into segment
        let writer = self.get_active_writer_mut();

        // Check for segment size target, maybe rotate to next writer
        if writer.offset() >= target_size {
            writer.flush()?;
            self.rotate()?;
        }

        Ok(bytes_written)
    }

    pub(crate) fn finish(mut self) -> crate::Result<(Vec<Writer>, W)> {
        let writer = self.get_active_writer_mut();

        if writer.item_count > 0 {
            writer.flush()?;
        }

        // IMPORTANT: We cannot finish the index writer here
        // The writers first need to be registered into the value log

        Ok((self.writers, self.index_writer))
    }
}
