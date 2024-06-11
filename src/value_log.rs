use crate::{
    blob_cache::BlobCache,
    id::{IdGenerator, SegmentId},
    index::Writer as IndexWriter,
    manifest::{SegmentManifest, SEGMENTS_FOLDER, VLOG_MARKER},
    path::absolute_path,
    segment::merge::MergeReader,
    value::UserValue,
    version::Version,
    Config, ExternalIndex, SegmentWriter, ValueHandle,
};
use byteorder::{BigEndian, ReadBytesExt};
use std::{
    collections::BTreeMap,
    fs::File,
    io::{BufReader, Read, Seek},
    path::PathBuf,
    sync::{atomic::AtomicU64, Arc, Mutex},
};

/// Unique value log ID
#[allow(clippy::module_name_repetitions)]
pub type ValueLogId = u64;

/// Hands out a unique (monotonically increasing) value log ID
pub fn get_next_vlog_id() -> ValueLogId {
    static VLOG_ID_COUNTER: AtomicU64 = AtomicU64::new(0);
    VLOG_ID_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
}

/// A disk-resident value log
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
    id: u64,

    config: Config,

    path: PathBuf,

    /// In-memory blob cache
    blob_cache: Arc<BlobCache>,

    /// Segment manifest
    pub manifest: SegmentManifest,

    id_generator: IdGenerator,

    rollover_guard: Mutex<()>,
}

impl ValueLog {
    /// Creates or recovers a value log in the given directory.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn open<P: Into<PathBuf>>(
        path: P, // TODO: move path into config?
        config: Config,
    ) -> crate::Result<Self> {
        let path = path.into();

        if path.join(VLOG_MARKER).try_exists()? {
            Self::recover(path, config)
        } else {
            Self::create_new(path, config)
        }
    }

    /// Creates a new empty value log in a directory.
    pub(crate) fn create_new<P: Into<PathBuf>>(path: P, config: Config) -> crate::Result<Self> {
        let path = absolute_path(path.into());
        log::trace!("Creating value-log at {}", path.display());

        std::fs::create_dir_all(&path)?;

        let marker_path = path.join(VLOG_MARKER);
        assert!(!marker_path.try_exists()?);

        std::fs::create_dir_all(path.join(SEGMENTS_FOLDER))?;

        // NOTE: Lastly, fsync .vlog marker, which contains the version
        // -> the V-log is fully initialized

        let mut file = std::fs::File::create(marker_path)?;
        Version::V1.write_file_header(&mut file)?;
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
            id: get_next_vlog_id(),
            config,
            path,
            blob_cache,
            manifest,
            id_generator: IdGenerator::default(),
            rollover_guard: Mutex::new(()),
        })))
    }

    pub(crate) fn recover<P: Into<PathBuf>>(path: P, config: Config) -> crate::Result<Self> {
        let path = path.into();
        log::info!("Recovering vLog at {}", path.display());

        {
            let bytes = std::fs::read(path.join(VLOG_MARKER))?;

            if let Some(version) = Version::parse_file_header(&bytes) {
                if version != Version::V1 {
                    return Err(crate::Error::InvalidVersion(Some(version)));
                }
            } else {
                return Err(crate::Error::InvalidVersion(None));
            }
        }

        let blob_cache = config.blob_cache.clone();
        let manifest = SegmentManifest::recover(&path)?;

        let highest_id = manifest
            .segments
            .read()
            .expect("lock is poisoned")
            .values()
            .map(|x| x.id)
            .max()
            .unwrap_or_default();

        Ok(Self(Arc::new(ValueLogInner {
            id: get_next_vlog_id(),
            config,
            path,
            blob_cache,
            manifest,
            id_generator: IdGenerator::new(highest_id + 1),
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
    pub fn get(&self, handle: &ValueHandle) -> crate::Result<Option<UserValue>> {
        let Some(segment) = self.manifest.get_segment(handle.segment_id) else {
            return Ok(None);
        };

        if let Some(value) = self.blob_cache.get(&((self.id, handle.clone()).into())) {
            return Ok(Some(value));
        }

        let mut reader = BufReader::new(File::open(&segment.path)?);
        reader.seek(std::io::SeekFrom::Start(handle.offset))?;

        let _crc = reader.read_u32::<BigEndian>()?;

        let val_len = reader.read_u32::<BigEndian>()?;

        let mut value = vec![0; val_len as usize];
        reader.read_exact(&mut value)?;

        let value = match segment.meta.compression {
            crate::CompressionType::None => value,

            #[cfg(feature = "lz4")]
            crate::CompressionType::Lz4 => lz4_flex::decompress_size_prepended(&value)
                .map_err(|_| crate::Error::Decompress(segment.meta.compression))?,

            #[cfg(feature = "miniz")]
            crate::CompressionType::Miniz(_) => miniz_oxide::inflate::decompress_to_vec(&value)
                .map_err(|_| crate::Error::Decompress(segment.meta.compression))?,
        };

        // TODO: handle CRC

        let val: UserValue = value.into();

        self.blob_cache
            .insert((self.id, handle.clone()).into(), val.clone());

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
            self.path.join(SEGMENTS_FOLDER),
            self.config.compression,
        )?)
    }

    /// Tries to find a least-effort-selection of segments to
    /// merge to react a certain space amplification.
    #[must_use]
    pub fn select_segments_for_space_amp_reduction(&self, space_amp_target: f32) -> Vec<SegmentId> {
        let current_space_amp = self.manifest.space_amp();

        if current_space_amp < space_amp_target {
            log::trace!("Space amp is <= target {space_amp_target}, nothing to do");
            vec![]
        } else {
            log::debug!("Selecting segments to GC, space_amp_target={space_amp_target}");

            let lock = self.manifest.segments.read().expect("lock is poisoned");
            let mut segments = lock.values().collect::<Vec<_>>();

            segments.sort_by(|a, b| {
                b.stale_ratio()
                    .partial_cmp(&a.stale_ratio())
                    .unwrap_or(std::cmp::Ordering::Equal)
            });

            let mut selection = vec![];

            let mut total_bytes = self.manifest.total_bytes();
            let mut stale_bytes = self.manifest.stale_bytes();

            for segment in segments {
                let segment_stale_bytes = segment.gc_stats.stale_bytes();
                stale_bytes -= segment_stale_bytes;
                total_bytes -= segment_stale_bytes;

                selection.push(segment.id);

                let space_amp_after_gc =
                    total_bytes as f32 / (total_bytes as f32 - stale_bytes as f32);

                log::debug!(
                    "Selected segment #{} for GC: will reduce space amp to {space_amp_after_gc}",
                    segment.id
                );

                if space_amp_after_gc <= space_amp_target {
                    break;
                }
            }

            selection
        }
    }

    /// Finds segment IDs that have reached a stale threshold.
    #[must_use]
    pub fn find_segments_with_stale_threshold(&self, threshold: f32) -> Vec<SegmentId> {
        self.manifest
            .segments
            .read()
            .expect("lock is poisoned")
            .values()
            .filter(|x| x.stale_ratio() >= threshold)
            .map(|x| x.id)
            .collect::<Vec<_>>()
    }

    /// Drops stale segments.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn drop_stale_segments(&self) -> crate::Result<()> {
        let ids = self
            .manifest
            .segments
            .read()
            .expect("lock is poisoned")
            .values()
            .filter(|x| x.is_stale())
            .map(|x| x.id)
            .collect::<Vec<_>>();

        log::debug!("Dropping blob files: {ids:?}");
        self.manifest.drop_segments(&ids)?;

        Ok(())
    }

    /// Marks some segments as stale.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    fn mark_as_stale(&self, ids: &[SegmentId]) {
        // NOTE: Read-locking is fine because we are dealing with an atomic bool
        let segments = self.manifest.segments.read().expect("lock is poisoned");

        for id in ids {
            let Some(segment) = segments.get(id) else {
                continue;
            };

            segment.mark_as_stale();
        }
    }

    /// Scans the given index and collecting GC statistics.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    #[allow(clippy::result_unit_err)]
    pub fn scan_for_stats(
        &self,
        iter: impl Iterator<Item = std::io::Result<(ValueHandle, u32)>>,
    ) -> crate::Result<()> {
        struct SegmentCounter {
            size: u64,
            item_count: u64,
        }

        // IMPORTANT: Only allow 1 rollover or GC at any given time
        let _guard = self.rollover_guard.lock().expect("lock is poisoned");

        log::info!("--- GC report for vLog @ {:?} ---", self.path);

        let mut size_map = BTreeMap::<SegmentId, SegmentCounter>::new();

        for handle in iter {
            let (handle, size) = handle.map_err(|_| {
                crate::Error::Io(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "Index returned error",
                ))
            })?;
            let size = u64::from(size);

            size_map
                .entry(handle.segment_id)
                .and_modify(|x| {
                    x.item_count += 1;
                    x.size += size;
                })
                .or_insert_with(|| SegmentCounter {
                    size,
                    item_count: 1,
                });
        }

        for (&id, counter) in &size_map {
            let used_size = counter.size;
            let alive_item_count = counter.item_count;

            let segment = self.manifest.get_segment(id).expect("segment should exist");

            let total_bytes = segment.meta.total_uncompressed_bytes;
            let stale_bytes = total_bytes - used_size;

            let total_items = segment.meta.item_count;
            let stale_items = total_items - alive_item_count;

            let space_amp = total_bytes as f64 / used_size as f64;
            let stale_ratio = stale_bytes as f64 / total_bytes as f64;

            log::info!(
                "Blob file #{id} has {}/{} stale MiB ({:.1}% stale, {stale_items}/{total_items} items) - space amp: {space_amp})",
                stale_bytes / 1_024 / 1_024,
                total_bytes / 1_024 / 1_024,
                stale_ratio * 100.0
            );

            segment.gc_stats.set_stale_bytes(stale_bytes);
            segment.gc_stats.set_stale_items(stale_items);
        }

        for id in self
            .manifest
            .segments
            .read()
            .expect("lock is poisoned")
            .keys()
        {
            let segment = self
                .manifest
                .get_segment(*id)
                .expect("segment should exist");

            if !size_map.contains_key(id) {
                log::info!(
                    "Blob file #{id} has no incoming references - can be dropped, freeing {} KiB on disk (userdata={} MiB)",
                    segment.meta.compressed_bytes / 1_024,
                    segment.meta.total_uncompressed_bytes / 1_024/ 1_024
                );
                self.mark_as_stale(&[*id]);
            }
        }

        log::info!("Total bytes: {}", self.manifest.total_bytes());
        log::info!("Stale bytes: {}", self.manifest.stale_bytes());
        log::info!("Space amp: {}", self.manifest.space_amp());
        log::info!("--- GC report done ---");

        Ok(())
    }

    /// Rewrites some segments into new segment(s), blocking the caller
    /// until the operation is completely done.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn rollover<R: ExternalIndex, W: IndexWriter>(
        &self,
        ids: &[u64],
        index_reader: &R,
        mut index_writer: W,
    ) -> crate::Result<()> {
        if ids.is_empty() {
            return Ok(());
        }

        // IMPORTANT: Only allow 1 rollover or GC at any given time
        let _guard = self.rollover_guard.lock().expect("lock is poisoned");

        log::info!("Rollover segments {ids:?}");

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
            let (k, v, segment_id) = item?;

            match index_reader.get(&k)? {
                // If this value is in an older segment, we can discard it
                Some(x) if segment_id < x.segment_id => continue,
                None => continue,
                _ => {}
            }

            let segment_id = writer.segment_id();
            let offset = writer.offset(&k);

            log::trace!(
                "GC: inserting indirection: {segment_id:?}:{offset:?} => {:?}",
                String::from_utf8_lossy(&k)
            );

            index_writer.insert_indirection(
                &k,
                ValueHandle { segment_id, offset },
                v.len() as u32,
            )?;
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
        self.mark_as_stale(ids);

        Ok(())
    }
}
