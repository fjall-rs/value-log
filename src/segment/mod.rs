use std::{
    path::PathBuf,
    sync::{atomic::AtomicU64, Arc},
};

pub mod merge;
pub mod multi_writer;
pub mod reader;
pub mod writer;

/* TODO: per blob CRC value */

/// A disk segment is an immutable, sorted, contiguous file
/// that contains key-value pairs.
///
/// ### File format
///
/// KV: \<key length: u16\> \<key: N\> \<value length: u32\> \<value: N\>
///
/// Segment: { KV } +
#[derive(Debug)]
pub struct Segment {
    /// Segment ID
    pub id: Arc<str>,

    /// asdasd
    pub path: PathBuf,

    /// asdasd
    pub item_count: u64,

    /// asdasd
    pub stale_values: AtomicU64,
}
