use crate::{
    blob_cache::BlobCache,
    id::{IdGenerator, SegmentId},
    index::Writer as IndexWriter,
    manifest::{SegmentManifest, SEGMENTS_FOLDER, VLOG_MARKER},
    path::absolute_path,
    segment::merge::MergeReader,
    version::Version,
    Config, ExternalIndex, SegmentWriter, ValueHandle,
};
use byteorder::{BigEndian, ReadBytesExt};
use std::{
    fs::File,
    io::{BufReader, Read, Seek},
    path::PathBuf,
    sync::{Arc, Mutex},
};

/// A disk-resident value log
#[derive(Clone)]
pub struct ValueLog<I: ExternalIndex + Clone + Send + Sync>(Arc<ValueLogInner<I>>);

impl<I: ExternalIndex + Clone + Send + Sync> std::ops::Deref for ValueLog<I> {
    type Target = ValueLogInner<I>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[allow(clippy::module_name_repetitions)]
pub struct ValueLogInner<I: ExternalIndex + Clone + Send + Sync> {
    config: Config,

    path: PathBuf,

    /// External index
    pub index: I,

    /// In-memory blob cache
    blob_cache: Arc<BlobCache>,

    /// Segment manifest
    pub manifest: SegmentManifest,

    id_generator: IdGenerator,

    rollover_guard: Mutex<()>,
}

impl<I: ExternalIndex + Clone + Send + Sync> ValueLog<I> {
    /// Creates or recovers a value log in the given directory.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn open<P: Into<PathBuf>>(
        path: P, // TODO: move path into config?
        config: Config,
        index: I,
    ) -> crate::Result<Self> {
        let path = path.into();

        if path.join(VLOG_MARKER).try_exists()? {
            Self::recover(path, config, index)
        } else {
            Self::create_new(path, config, index)
        }
    }

    /// Creates a new empty value log in a directory.
    pub(crate) fn create_new<P: Into<PathBuf>>(
        path: P,
        config: Config,
        index: I,
    ) -> crate::Result<Self> {
        let path = absolute_path(path.into());
        log::trace!("Creating value-log at {}", path.display());

        std::fs::create_dir_all(&path)?;

        let marker_path = path.join(VLOG_MARKER);
        assert!(!marker_path.try_exists()?);

        std::fs::create_dir_all(path.join(SEGMENTS_FOLDER))?;

        // NOTE: Lastly, fsync .vlog marker, which contains the version
        // -> the V-log is fully initialized

        let mut file = std::fs::File::create(marker_path)?;
        Version::V0.write_file_header(&mut file)?;
        file.sync_all()?;

        #[cfg(not(target_os = "windows"))]
        {
            // fsync folders on Unix

            let folder = std::fs::File::open(path.join(SEGMENTS_FOLDER))?;
            folder.sync_all()?;

            let folder = std::fs::File::open(&path)?;
            folder.sync_all()?;
        }

        let blob_cache = config.blob_cache.clone();
        let manifest = SegmentManifest::create_new(&path)?;

        Ok(Self(Arc::new(ValueLogInner {
            config,
            path,
            blob_cache,
            index,
            manifest,
            id_generator: IdGenerator::default(),
            rollover_guard: Mutex::new(()),
        })))
    }

    pub(crate) fn recover<P: Into<PathBuf>>(
        path: P,
        config: Config,
        index: I,
    ) -> crate::Result<Self> {
        let path = path.into();
        log::info!("Recovering value-log at {}", path.display());

        {
            let bytes = std::fs::read(path.join(VLOG_MARKER))?;

            if let Some(version) = Version::parse_file_header(&bytes) {
                if version != Version::V0 {
                    return Err(crate::Error::InvalidVersion(Some(version)));
                }
            } else {
                return Err(crate::Error::InvalidVersion(None));
            }
        }

        let blob_cache = config.blob_cache.clone();
        let manifest = SegmentManifest::recover(&path)?;

        Ok(Self(Arc::new(ValueLogInner {
            config,
            path,
            blob_cache,
            index,
            manifest,
            // TODO: recover ID, test!!!, maybe store next ID in manifest as u64
            id_generator: IdGenerator::default(),
            rollover_guard: Mutex::new(()),
        })))
    }

    /// Registers writer
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn register(&self, writer: SegmentWriter) -> crate::Result<()> {
        self.manifest.register(writer)
    }

    /// Returns segment count
    #[must_use]
    pub fn segment_count(&self) -> usize {
        self.manifest.len()
    }

    /// Resolves a value handle
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn get(&self, handle: &ValueHandle) -> crate::Result<Option<Arc<[u8]>>> {
        let Some(segment) = self.manifest.get_segment(handle.segment_id) else {
            return Ok(None);
        };

        if let Some(value) = self.blob_cache.get(handle) {
            return Ok(Some(value));
        }

        let mut reader = BufReader::new(File::open(segment.path.join("data"))?);
        reader.seek(std::io::SeekFrom::Start(handle.offset))?;

        let _crc = reader.read_u32::<BigEndian>()?;

        let val_len = reader.read_u32::<BigEndian>()?;

        let mut val = vec![0; val_len as usize];
        reader.read_exact(&mut val)?;

        #[cfg(feature = "lz4")]
        let val = lz4_flex::decompress_size_prepended(&val).expect("should decompress");

        // TODO: handle CRC

        let val: Arc<[u8]> = val.into();

        self.blob_cache.insert(handle.clone(), val.clone());

        Ok(Some(val))
    }

    /// Initializes a new segment writer
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn get_writer(&self) -> crate::Result<SegmentWriter> {
        Ok(SegmentWriter::new(
            self.id_generator.clone(),
            self.config.segment_size_bytes,
            &self.path,
        )?)
    }

    /// Scans through a segment, refreshing its statistics
    ///
    /// This function is blocking.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn refresh_stats(&self, segment_id: SegmentId) -> std::io::Result<()> {
        let Some(segment) = self.manifest.get_segment(segment_id) else {
            return Ok(());
        };

        // Scan segment
        let scanner = segment.scan()?;

        let mut item_count = 0;
        let mut total_bytes = 0;

        let mut stale_items = 0;
        let mut stale_bytes = 0;

        for item in scanner {
            let (key, val) = item?;
            item_count += 1;
            total_bytes += val.len() as u64;

            if let Some(item) = self.index.get(&key)? {
                // NOTE: Segment IDs are monotonically increasing
                if item.segment_id > segment_id {
                    stale_items += 1;
                    stale_bytes += val.len() as u64;
                }
            } else {
                stale_items += 1;
                stale_bytes += val.len() as u64;
            }
        }

        segment
            .stats
            .item_count
            .store(item_count, std::sync::atomic::Ordering::Release);

        segment
            .stats
            .total_bytes
            .store(total_bytes, std::sync::atomic::Ordering::Release);

        segment
            .stats
            .stale_items
            .store(stale_items, std::sync::atomic::Ordering::Release);

        segment
            .stats
            .stale_bytes
            .store(stale_bytes, std::sync::atomic::Ordering::Release);

        // TODO: need to store stats atomically, to make recovery fast
        // TODO: changing stats doesn't happen **too** often, so the I/O is fine

        Ok(())
    }

    /// Drops stale segments
    pub fn drop_stale_segments(&self) -> crate::Result<()> {
        let ids = self
            .manifest
            .segments
            .read()
            .expect("lock is poisoned")
            .values()
            .filter(|x| x.stats.is_stale())
            .map(|x| x.id)
            .collect::<Vec<_>>();

        self.manifest.drop_segments(&ids)?;

        Ok(())
    }

    fn mark_as_stale(&self, ids: &[SegmentId]) -> crate::Result<()> {
        let segments = self.manifest.segments.write().expect("lock is poisoned");

        for id in ids {
            let Some(segment) = segments.get(id) else {
                continue;
            };

            segment.stats.mark_as_stale();

            // TODO: need to store stats atomically, to make recovery fast
            // TODO: changing stats doesn't happen **too** often, so the I/O is fine
        }

        Ok(())
    }

    /// Rewrites some segments into new segment(s), blocking the caller
    /// until the operation is completely done.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn rollover<W: IndexWriter>(&self, ids: &[u64], index_writer: &W) -> crate::Result<()> {
        // IMPORTANT: Only allow 1 rollover at any given time
        let _guard = self.rollover_guard.lock().expect("lock is poisoned");

        let segments = ids
            .iter()
            .map(|&x| self.manifest.get_segment(x))
            .collect::<Option<Vec<_>>>();

        let Some(segments) = segments else {
            return Ok(());
        };

        let readers = segments
            .into_iter()
            .map(|x| x.scan())
            .collect::<std::io::Result<Vec<_>>>()?;

        let reader = MergeReader::new(readers);

        let mut writer = self.get_writer()?;

        for item in reader {
            let (k, v, _) = item?;

            let segment_id = writer.segment_id();
            let offset = writer.offset(&k);

            log::trace!(
                "GC: inserting indirection: {segment_id:?}:{offset:?} => {:?}",
                String::from_utf8_lossy(&k)
            );

            index_writer.insert_indirection(&k, ValueHandle { segment_id, offset })?;
            writer.write(&k, &v)?;
        }

        // IMPORTANT: New segments need to be persisted before adding to index
        // to avoid dangling pointers
        self.manifest.register(writer)?;

        // NOTE: If we crash before before finishing the index write, it's fine
        // because all new segments will be unreferenced, and thus can be deleted
        index_writer.finish()?;

        // IMPORTANT: We only mark the segments as definitely stale
        // The external index needs to decide when it is safe to drop
        // the old segments, as some reads may still be performed
        self.mark_as_stale(ids)?;

        Ok(())
    }
}
