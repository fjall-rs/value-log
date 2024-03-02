//! Generic value log implementation for key-value separated storage.
//!
//! > This crate is intended for key-value separated LSM storage.
//! > You probably want to use <https://github.com/fjall-rs/fjall> instead.
//!
//! The value log's contents are split into segments, each segment holds a sorted
//! list of key-value pairs:
//!
//! [k0, v0][k1, v1][k2, v2][k3, v3][k4, v4]
//!
//! The value log does not have an index - to efficiently retrieve an item, a
//! [`ValueHandle`] needs to be retrieved from an external [`Index`]. Holding a
//! value handle then allows loading the value from the file by seeking to it.
//!
//! Recently retrieved ("hot") items may be cached by an in-memory value cache to avoid
//! repeated disk accesses.
//!
//! As data changes, old values will unnecessarily waste disk space. As space amplification
//! increases, stale data needs to be discarded by rewriting old segments (garbage collection).
//!
//! While segments are internally sorted, which may help with range scans, data may not be stored
//! contiguously, which hurts read performance of ranges. Point reads also require an extra level of
//! indirection, as the value handle needs to be retrieved from the index. However, this index is generally
//! small, so ideally it can be cached efficiently.
//!
//! In summary, a value log trades read & space amplification for superior write
//! amplification when storing large blobs.
//!
//! Use a value log, when:
//! - you are storing large documents (HTML pages, small images, ...)
//! - your data is rarely deleted, or you do not have strict disk space requirements
//! - your access pattern is point read heavy

#![forbid(unsafe_code)]
#![deny(clippy::all, missing_docs)]
#![deny(clippy::unwrap_used, clippy::indexing_slicing)]
#![warn(clippy::pedantic, clippy::nursery, clippy::cargo)]
#![warn(clippy::expect_used)]
#![allow(clippy::missing_const_for_fn)]

mod config;
mod error;
mod handle;
mod id;
mod index;
mod segment;
mod value_log;
mod version;

pub use {
    config::Config,
    error::{Error, Result},
    handle::ValueHandle,
    index::Index,
    segment::multi_writer::MultiWriter as SegmentWriter,
    segment::reader::Reader as SegmentReader,
    segment::Segment,
    value_log::ValueLog,
};
