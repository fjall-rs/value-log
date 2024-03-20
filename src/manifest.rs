use crate::{id::SegmentId, segment::stats::Stats, Segment, SegmentWriter as MultiWriter};
use std::{
    collections::HashMap,
    fs::File,
    io::Write,
    path::{Path, PathBuf},
    sync::{atomic::AtomicU64, Arc},
};

pub const VLOG_MARKER: &str = ".vlog";
pub const SEGMENTS_FOLDER: &str = "segments";

/// Atomically rewrites a file
fn rewrite_atomic<P: AsRef<Path>>(path: P, content: &[u8]) -> std::io::Result<()> {
    let path = path.as_ref();
    let folder = path.parent().expect("should have a parent");

    let mut temp_file = tempfile::NamedTempFile::new_in(folder)?;
    temp_file.write_all(content)?;
    temp_file.persist(path)?;

    #[cfg(not(target_os = "windows"))]
    {
        // TODO: Not sure if the fsync is really required, but just for the sake of it...
        // TODO: also not sure why it fails on Windows...
        let file = File::open(path)?;
        file.sync_all()?;
    }

    Ok(())
}

#[allow(clippy::module_name_repetitions)]
pub struct SegmentManifest {
    path: PathBuf,
    pub(crate) segments: HashMap<SegmentId, Arc<Segment>>,
}

impl SegmentManifest {
    fn remove_unfinished_segments<P: AsRef<Path>>(
        folder: P,
        registered_ids: &[u64],
    ) -> crate::Result<()> {
        // TODO:
        Ok(())
    }

    pub(crate) fn recover<P: AsRef<Path>>(folder: P) -> crate::Result<Self> {
        let folder = folder.as_ref();
        let path = folder.join("segments.json");
        log::debug!("Loading value log manifest from {}", path.display());

        let str = std::fs::read_to_string(&path)?;
        let ids: Vec<u64> = serde_json::from_str(&str).expect("deserialize error");

        Self::remove_unfinished_segments(folder, &ids)?;

        let segments = {
            let mut map = HashMap::default();

            for id in ids {
                map.insert(
                    id,
                    Arc::new(Segment {
                        id,
                        path: folder.join(SEGMENTS_FOLDER).join(id.to_string()),
                        stats: Stats::default(),
                    }),
                );
            }

            map
        };

        Ok(Self { path, segments })
    }

    pub(crate) fn create_new<P: AsRef<Path>>(folder: P) -> crate::Result<Self> {
        let path = folder.as_ref().join("segments.json");

        let mut m = Self {
            path,
            segments: HashMap::default(),
        };
        m.write_to_disk()?;

        Ok(m)
    }

    pub fn drop_segments(&mut self, ids: &[u64]) -> crate::Result<()> {
        self.segments.retain(|x, _| !ids.contains(x));
        self.write_to_disk()
    }

    pub fn register(&mut self, writer: MultiWriter) -> crate::Result<()> {
        let writers = writer.finish()?;

        for writer in writers {
            let segment_id = writer.segment_id;
            let segment_folder = writer.folder.clone();

            self.segments.insert(
                segment_id,
                Arc::new(Segment {
                    id: segment_id,
                    path: segment_folder,
                    stats: Stats {
                        item_count: writer.item_count.into(),
                        total_bytes: writer.written_blob_bytes.into(),
                        stale_items: AtomicU64::default(),
                        stale_bytes: AtomicU64::default(),
                    },
                }),
            );
        }

        self.write_to_disk()
    }

    pub(crate) fn write_to_disk(&mut self) -> crate::Result<()> {
        log::trace!("Writing segment manifest to {}", self.path.display());

        let keys: Vec<u64> = self.segments.keys().copied().collect();

        // NOTE: Serialization can't fail here
        #[allow(clippy::expect_used)]
        let json = serde_json::to_string_pretty(&keys).expect("should serialize");
        rewrite_atomic(&self.path, json.as_bytes())?;

        Ok(())
    }

    /// Gets a segment
    #[must_use]
    pub fn get_segment(&self, id: SegmentId) -> Option<Arc<Segment>> {
        self.segments.get(&id).cloned()
    }

    /// Lists all segment IDs
    #[must_use]
    pub fn list_segment_ids(&self) -> Vec<SegmentId> {
        self.segments.keys().copied().collect()
    }

    /// Lists all segments
    #[must_use]
    pub fn list_segments(&self) -> Vec<Arc<Segment>> {
        self.segments.values().cloned().collect()
    }

    /// Returns the amount of bytes on disk that are occupied by blobs.
    ///
    /// This value may not be fresh, as it is only set after running [`ValueLog::refresh_stats`].
    #[must_use]
    pub fn disk_space_used(&self) -> u64 {
        self.segments
            .values()
            .map(|x| x.stats.total_bytes())
            .sum::<u64>()
    }

    /// Returns the amount of bytes that can be freed on disk
    /// if all segments were to be defragmented
    ///
    /// This value may not be fresh, as it is only set after running [`ValueLog::refresh_stats`].
    #[must_use]
    pub fn reclaimable_bytes(&self) -> u64 {
        self.segments
            .values()
            .map(|x| x.stats.get_stale_bytes())
            .sum::<u64>()
    }

    /// Returns the amount of stale items
    ///
    /// This value may not be fresh, as it is only set after running [`ValueLog::refresh_stats`].
    #[must_use]
    pub fn stale_items_count(&self) -> u64 {
        self.segments
            .values()
            .map(|x| x.stats.get_stale_items())
            .sum::<u64>()
    }

    /// Returns the percent of dead bytes in the value log
    ///
    /// This value may not be fresh, as it is only set after running [`ValueLog::refresh_stats`].
    #[must_use]
    pub fn stale_ratio(&self) -> f32 {
        let used_bytes = self
            .segments
            .values()
            .map(|x| x.stats.total_bytes())
            .sum::<u64>();
        if used_bytes == 0 {
            return 0.0;
        }

        let stale_bytes = self
            .segments
            .values()
            .map(|x| x.stats.get_stale_bytes())
            .sum::<u64>();
        if stale_bytes == 0 {
            return 0.0;
        }

        stale_bytes as f32 / used_bytes as f32
    }

    /// Returns the approximate space amplification
    ///
    /// Returns 0.0 if there are no items.
    ///
    /// This value may not be fresh, as it is only set after running [`ValueLog::refresh_stats`].
    #[must_use]
    pub fn space_amp(&self) -> f32 {
        let used_bytes = self
            .segments
            .values()
            .map(|x| x.stats.total_bytes())
            .sum::<u64>();
        if used_bytes == 0 {
            return 0.0;
        }

        let stale_bytes = self
            .segments
            .values()
            .map(|x| x.stats.get_stale_bytes())
            .sum::<u64>();

        let alive_bytes = used_bytes - stale_bytes;
        if alive_bytes == 0 {
            return 0.0;
        }

        used_bytes as f32 / alive_bytes as f32
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;
    use std::io::Write;
    use test_log::test;

    #[test]
    fn test_atomic_rewrite() -> crate::Result<()> {
        let dir = tempfile::tempdir()?;

        let path = dir.path().join("test.txt");
        {
            let mut file = File::create(&path)?;
            write!(file, "asdasdasdasdasd")?;
        }

        rewrite_atomic(&path, b"newcontent")?;

        let content = std::fs::read_to_string(&path)?;
        assert_eq!("newcontent", content);

        Ok(())
    }
}
