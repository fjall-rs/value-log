pub mod merge;
pub mod multi_writer;
pub mod reader;
pub mod stats;
pub mod writer;

use self::stats::Stats;
use crate::id::SegmentId;
use std::path::PathBuf;

/// A disk segment is an immutable, sorted, contiguous file
/// that contains key-value pairs.
///
/// ### File format
///
/// KV: \<key length: u16\> \<key: N\> \<crc hash: u32\> \<value length: u32\> \<value: N\>
///
/// Segment: { KV } +
#[derive(Debug)]
pub struct Segment {
    /// Segment ID
    pub id: SegmentId,

    /// Segment path (folder)
    pub path: PathBuf,

    /// Segment statistics
    pub stats: Stats,
}

impl Segment {
    /// Returns a scanner that can iterate through the segment
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn scan(&self) -> std::io::Result<reader::Reader> {
        let path = self.path.join("data");
        reader::Reader::new(path, self.id)
    }

    /// Always returns `false`
    pub fn is_empty(&self) -> bool {
        false
    }

    /// Returns the amount of items in the segment
    pub fn len(&self) -> u64 {
        self.stats
            .item_count
            .load(std::sync::atomic::Ordering::Acquire)
    }
}
