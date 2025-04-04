// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::ValueLogId;
use std::{fs::File, io::BufReader};

/// The unique identifier for a value log blob file. Another name for SegmentId
pub type BlobFileId = u64;

/// File descriptor cache, to cache file descriptors after an fopen().
/// Reduces the number of fopen() needed when accessing the same blob file.
pub trait FDCache: Clone {
    /// Caches a file descriptor
    fn insert(&self, vlog_id: ValueLogId, blob_file_id: BlobFileId, fd: File);

    /// Retrieves a file descriptor from the cache, or `None` if it could not be found
    fn get(&self, vlog_id: ValueLogId, blob_file_id: BlobFileId) -> Option<File>;
}
