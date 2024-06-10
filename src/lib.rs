//! Generic value log implementation for key-value separated storage.
//!
//! > This crate is intended as a building block for key-value separated storage.
//! > You probably want to use <https://github.com/fjall-rs/fjall> instead.
//!
//! The value log's contents are split into segments, each segment holds a sorted
//! list of key-value pairs:
//!
//! [k0, v0][k1, v1][k2, v2][k3, v3][k4, v4]
//!
//! The value log does not have an index - to efficiently retrieve an item, a
//! [`ValueHandle`] needs to be retrieved from an [`ExternalIndex`]. Using the
//! value handle then allows loading the value from the value log.
//!
//! Recently retrieved ("hot") items may be cached by an in-memory value cache to avoid
//! repeated disk accesses.
//!
//! As data changes, old values will unnecessarily occupy disk space. As space amplification
//! increases, stale data needs to be discarded by rewriting old segments (garbage collection).
//! This process can happen on-line.
//!
//! Even though segments are internally sorted, which may help with range scans, data may not be stored
//! contiguously, which hurts read performance of ranges. Point reads also require an extra level of
//! indirection, as the value handle needs to be retrieved from the index. However, this index is generally
//! small, so ideally it can be cached efficiently. And because compaction needs to rewrite less data, more
//! disk I/O is freed to fulfill write and read requests.
//!
//! In summary, a value log trades read & space amplification for superior write
//! amplification when storing large blobs.
//!
//! Use a value log, when:
//! - you are storing large values (HTML pages, big JSON, small images, archiving, ...)
//! - your data is rarely deleted or updated, or you do not have strict disk space requirements
//! - your access pattern is point read heavy
//!
//! # Example usage
//!
//! ```
//! # use value_log::{ExternalIndex, IndexWriter, MockIndex};
//! # use std::collections::BTreeMap;
//! use value_log::{Config, ValueHandle, ValueLog};
//!
//! # fn main() -> value_log::Result<()> {
//! # let folder = tempfile::tempdir()?;
//! # let index = MockIndex::default();
//! # let path = folder.path();
//! #
//! // Open or recover value log from disk
//! let value_log = ValueLog::open(path, Config::default())?;
//!
//! // Write some data
//! let mut writer = value_log.get_writer()?;
//! let segment_id = writer.segment_id();
//!
//! for key in ["a", "b", "c", "d", "e"] {
//!     let value = key.repeat(1_000);
//!     let value = value.as_bytes();
//!
//!     let offset = writer.offset(key.as_bytes());
//!
//!     // NOTE: The data written to the value log is only stable
//!     // after the writer is registered in the value log (see below)
//!     //
//!     // When implementing the ExternalIndex trait, it's important to
//!     // not hand out ValueHandles before that point in time
//!     index.insert_indirection(key.as_bytes(), ValueHandle { offset, segment_id }, value.len() as u32)?;
//!
//!     writer.write(key.as_bytes(), value)?;
//! }
//!
//! // Finish writing
//! value_log.register(writer)?;
//!
//! // Get some stats
//! assert_eq!(1.0, value_log.manifest.space_amp());
//! #
//! # Ok(())
//! # }
//! ```

#![doc(html_logo_url = "https://raw.githubusercontent.com/fjall-rs/value-log/main/logo.png")]
#![doc(html_favicon_url = "https://raw.githubusercontent.com/fjall-rs/value-log/main/logo.png")]
#![forbid(unsafe_code)]
#![deny(clippy::all, missing_docs, clippy::cargo)]
#![deny(clippy::unwrap_used)]
#![deny(clippy::indexing_slicing)]
#![warn(clippy::pedantic, clippy::nursery)]
#![warn(clippy::expect_used)]
#![allow(clippy::missing_const_for_fn)]

mod blob_cache;
mod compression;
mod config;
mod error;
mod handle;
mod id;
mod index;
mod key_range;
mod manifest;
mod mock;
mod path;
mod segment;
mod serde;
mod value;
mod value_log;
mod version;

pub use {
    blob_cache::BlobCache,
    compression::CompressionType,
    config::Config,
    error::{Error, Result},
    handle::ValueHandle,
    index::{ExternalIndex, Writer as IndexWriter},
    segment::multi_writer::MultiWriter as SegmentWriter,
    segment::reader::Reader as SegmentReader,
    segment::Segment,
    value::UserValue,
    value_log::ValueLog,
    version::Version,
};

#[doc(hidden)]
pub use mock::{MockIndex, MockIndexWriter};

#[doc(hidden)]
pub use key_range::KeyRange;
