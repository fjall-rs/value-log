pub mod gc_stats;
pub mod merge;
pub mod meta;
pub mod multi_writer;
pub mod reader;
pub mod trailer;
pub mod writer;

use crate::id::SegmentId;
use gc_stats::GcStats;
use meta::Metadata;
use std::path::PathBuf;

/// A disk segment is an immutable, sorted, contiguous file
/// that contains key-value pairs.
#[derive(Debug)]
pub struct Segment {
    /// Segment ID
    pub id: SegmentId,

    /// Segment file path
    pub path: PathBuf,

    /// Segment statistics
    pub meta: Metadata,

    /// Runtime stats for garbage collection
    pub gc_stats: GcStats,
}

impl Segment {
    /// Returns a scanner that can iterate through the segment.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn scan(&self) -> crate::Result<reader::Reader> {
        reader::Reader::new(&self.path, self.id)
    }

    /// Always returns `false` because a segment is never empty.
    pub fn is_empty(&self) -> bool {
        false
    }

    /// Returns the amount of items in the segment.
    pub fn len(&self) -> u64 {
        self.meta.item_count
    }

    /// Marks the segment as fully stale.
    pub(crate) fn mark_as_stale(&self) {
        self.gc_stats.set_stale_items(self.meta.item_count);

        self.gc_stats
            .set_stale_bytes(self.meta.total_uncompressed_bytes);
    }

    /// Returns `true` if the segment is fully stale.
    pub fn is_stale(&self) -> bool {
        self.gc_stats.stale_items() == self.meta.item_count
    }

    /// Returns the percent of dead items in the segment.
    pub fn stale_ratio(&self) -> f32 {
        let dead = self.gc_stats.stale_items() as f32;
        if dead == 0.0 {
            return 0.0;
        }

        dead / self.meta.item_count as f32
    }
}
