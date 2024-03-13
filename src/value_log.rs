use crate::{
    blob_cache::BlobCache,
    index::Writer as IndexWriter,
    segment::{merge::MergeReader, multi_writer::MultiWriter, stats::Stats},
    version::Version,
    Config, Index, Segment, SegmentWriter, ValueHandle,
};
use byteorder::{BigEndian, ReadBytesExt};
use std::{
    collections::BTreeMap,
    fs::File,
    io::{BufReader, Read, Seek},
    path::PathBuf,
    sync::{atomic::AtomicU64, Arc, Mutex, RwLock},
};

/// A disk-resident value log.
#[derive(Clone)]
pub struct ValueLog(Arc<ValueLogInner>);

impl std::ops::Deref for ValueLog {
    type Target = ValueLogInner;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[allow(clippy::module_name_repetitions)]
pub struct ValueLogInner {
    config: Config,

    path: PathBuf,

    /// External index
    pub index: Arc<dyn Index + Send + Sync>,

    /// In-memory blob cache
    blob_cache: Arc<BlobCache>,

    /// Segment manifest
    pub segments: RwLock<BTreeMap<Arc<str>, Arc<Segment>>>,

    semaphore: Mutex<()>,
}

impl ValueLog {
    /// Creates or recovers a value log
    pub fn new<P: Into<PathBuf>>(
        path: P,
        config: Config,
        index: Arc<dyn Index + Send + Sync>,
    ) -> crate::Result<Self> {
        Self::create_new(path, config, index)
        // TODO: recover if exists
    }

    /// Creates a new empty value log in a folder
    pub(crate) fn create_new<P: Into<PathBuf>>(
        path: P,
        config: Config,
        index: Arc<dyn Index + Send + Sync>,
    ) -> crate::Result<Self> {
        let path = path.into();
        log::trace!("Creating value-log at {}", path.display());

        std::fs::create_dir_all(&path)?;

        let marker_path = path.join(".vlog");
        assert!(!marker_path.try_exists()?);

        std::fs::create_dir_all(path.join("segments"))?;

        // NOTE: Lastly, fsync .vlog marker, which contains the version
        // -> the V-log is fully initialized

        let mut file = std::fs::File::create(marker_path)?;
        Version::V1.write_file_header(&mut file)?;
        file.sync_all()?;

        #[cfg(not(target_os = "windows"))]
        {
            // fsync folders on Unix

            let folder = std::fs::File::open(path.join("segments"))?;
            folder.sync_all()?;

            let folder = std::fs::File::open(&path)?;
            folder.sync_all()?;
        }

        let blob_cache = config.blob_cache.clone();

        Ok(Self(Arc::new(ValueLogInner {
            config,
            path, // TODO: absolute path
            blob_cache,
            index,
            segments: RwLock::new(BTreeMap::default()),
            semaphore: Mutex::new(()),
        })))
    }

    /// Gets a segment
    #[must_use]
    pub fn get_segment(&self, id: &Arc<str>) -> Option<Arc<Segment>> {
        self.segments
            .read()
            .expect("lock is poisoned")
            .get(id)
            .cloned()
    }

    /// Lists all segment IDs
    #[must_use]
    pub fn list_segment_ids(&self) -> Vec<Arc<str>> {
        self.segments
            .read()
            .expect("lock is poisoned")
            .keys()
            .cloned()
            .collect()
    }

    /// Lists all segments
    #[must_use]
    pub fn list_segments(&self) -> Vec<Arc<Segment>> {
        self.segments
            .read()
            .expect("lock is poisoned")
            .values()
            .cloned()
            .collect()
    }

    pub(crate) fn recover<P: Into<PathBuf>>(
        path: P,
        _index: Arc<dyn Index + Send + Sync>,
    ) -> crate::Result<()> {
        let path = path.into();
        log::info!("Recovering value-log at {}", path.display());

        {
            let bytes = std::fs::read(path.join(".vlog"))?;

            if let Some(version) = Version::parse_file_header(&bytes) {
                if version != Version::V1 {
                    return Err(crate::Error::InvalidVersion(Some(version)));
                }
            } else {
                return Err(crate::Error::InvalidVersion(None));
            }
        }

        todo!()
    }

    /// Resolves a value handle
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn get(&self, handle: &ValueHandle) -> crate::Result<Option<Arc<[u8]>>> {
        let Some(segment) = self
            .segments
            .read()
            .expect("lock is poisoned")
            .get(&handle.segment_id)
            .cloned()
        else {
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

    /* pub fn get_multiple(
        &self,
        handles: &[ValueHandle],
    ) -> crate::Result<Vec<Option<Vec<u8>>>> {
        handles.iter().map(|vr| self.get(vr)).collect()
    } */

    /// Initializes a new segment writer
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn get_writer(&self) -> crate::Result<SegmentWriter> {
        Ok(SegmentWriter::new(
            self.config.segment_size_bytes,
            &self.path,
        )?)
    }

    /// Registers a new segment (blob file) by consuming a writer
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn register(&self, writer: MultiWriter) -> crate::Result<()> {
        let writers = writer.finish()?;

        let mut lock = self.segments.write().expect("lock is poisoned");

        for writer in writers {
            let segment_id = writer.segment_id.clone();
            let segment_folder = writer.folder.clone();

            lock.insert(
                segment_id.clone(),
                Arc::new(Segment {
                    id: segment_id,
                    path: segment_folder,
                    stats: Stats {
                        item_count: writer.item_count,
                        total_bytes: writer.written_blob_bytes,
                        dead_items: AtomicU64::default(),
                        dead_bytes: AtomicU64::default(),
                    },
                }),
            );
        }

        Ok(())
    }

    /// Returns the amount of bytes that can be freed on disk
    /// if all segments were to be defragmented
    ///
    /// This value may not be fresh, as it is only set after running [`ValueLog::refresh_stats`].
    #[must_use]
    pub fn reclaimable_bytes(&self) -> u64 {
        let segments = self.segments.read().expect("lock is poisoned");

        let dead_bytes = segments
            .values()
            .map(|x| x.stats.get_dead_bytes())
            .sum::<u64>();
        drop(segments);

        dead_bytes
    }

    /// Returns the percent of dead bytes in the value log
    ///
    /// This value may not be fresh, as it is only set after running [`ValueLog::refresh_stats`].
    #[must_use]
    pub fn dead_ratio(&self) -> f32 {
        let segments = self.segments.read().expect("lock is poisoned");

        let used_bytes = segments.values().map(|x| x.stats.total_bytes).sum::<u64>();
        if used_bytes == 0 {
            return 0.0;
        }

        let dead_bytes = segments
            .values()
            .map(|x| x.stats.get_dead_bytes())
            .sum::<u64>();
        if dead_bytes == 0 {
            return 0.0;
        }

        drop(segments);

        dead_bytes as f32 / used_bytes as f32
    }

    /// Returns the approximate space amplification
    ///
    /// Returns 0.0 if there are no items.
    ///
    /// This value may not be fresh, as it is only set after running [`ValueLog::refresh_stats`].
    #[must_use]
    pub fn space_amp(&self) -> f32 {
        let segments = self.segments.read().expect("lock is poisoned");

        let used_bytes = segments.values().map(|x| x.stats.total_bytes).sum::<u64>();
        if used_bytes == 0 {
            return 0.0;
        }

        let dead_bytes = segments
            .values()
            .map(|x| x.stats.get_dead_bytes())
            .sum::<u64>();

        drop(segments);

        let alive_bytes = used_bytes - dead_bytes;
        if alive_bytes == 0 {
            return 0.0;
        }

        used_bytes as f32 / alive_bytes as f32
    }

    /// Scans through a segment, refreshing its statistics
    ///
    /// This function is blocking.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn refresh_stats(&self, segment_id: &Arc<str>) -> std::io::Result<()> {
        let Some(segment) = self
            .segments
            .read()
            .expect("lock is poisoned")
            .get(segment_id)
            .cloned()
        else {
            return Ok(());
        };

        // Scan segment
        let scanner = segment.scan()?;

        let mut dead_items = 0;
        let mut dead_bytes = 0;

        for item in scanner {
            let (key, val) = item?;

            if let Some(item) = self.index.get(&key)? {
                // NOTE: Segment IDs are monotonically increasing
                if item.segment_id > *segment_id {
                    dead_items += 1;
                    dead_bytes += val.len() as u64;
                }
            } else {
                dead_items += 1;
                dead_bytes += val.len() as u64;
            }
        }

        segment
            .stats
            .dead_items
            .store(dead_items, std::sync::atomic::Ordering::Release);

        segment
            .stats
            .dead_bytes
            .store(dead_bytes, std::sync::atomic::Ordering::Release);

        Ok(())
    }

    /// Rewrites some segments into new segment(s), blocking the caller
    /// until the operation is completely done.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn rollover<W: IndexWriter + Send + Sync>(
        &self,
        ids: &[Arc<str>],
        index_writer: &W,
    ) -> crate::Result<()> {
        // IMPORTANT: Only allow 1 rollover at any given time
        let _guard = self.semaphore.lock().expect("lock is poisoned");

        let lock = self.segments.read().expect("lock is poisoned");

        let segments = ids
            .iter()
            .map(|x| lock.get(&**x).cloned())
            .collect::<Option<Vec<_>>>();

        drop(lock);

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
            eprintln!("{k:?} => {:?}", String::from_utf8_lossy(&v));

            let segment_id = writer.segment_id();
            let offset = writer.offset(&k);

            log::trace!(
                "GC: inserting indirection: {segment_id:?}:{offset:?} => {:?}",
                String::from_utf8_lossy(&k)
            );

            index_writer.insert_indirection(&k, ValueHandle { segment_id, offset })?;
            writer.write(&k, &v)?;
        }

        self.register(writer)?;
        index_writer.finish()?;

        let mut lock = self.segments.write().expect("lock is poisoned");
        for id in ids {
            std::fs::remove_dir_all(self.path.join("segments").join(&**id))?;
            lock.remove(id);
        }

        Ok(())
    }
}
