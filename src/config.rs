// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{blob_cache::BlobCache, compression::Compressor, fd_cache::BlobFileId, FDCache};

/// Value log configuration
pub struct Config<BC: BlobCache, FDC: FDCache, C: Compressor + Clone> {
    /// Target size of vLog segments
    pub(crate) segment_size_bytes: u64,

    /// Blob cache to use
    pub(crate) blob_cache: BC,

    /// File descriptor cache to use
    pub(crate) fd_cache: FDC,

    /// Compression to use
    pub(crate) compression: C,
}

impl<BC: BlobCache, FDC: FDCache, C: Compressor + Clone + Default> Config<BC, FDC, C> {
    /// Creates a new configuration builder.
    pub fn new(blob_cache: BC, fd_cache: FDC) -> Self {
        Self {
            blob_cache,
            fd_cache,
            compression: Default::default(),
            segment_size_bytes: 128 * 1_024 * 1_024,
        }
    }

    /// Sets the compression & decompression scheme.
    #[must_use]
    pub fn compression(mut self, compressor: C) -> Self {
        self.compression = compressor;
        self
    }

    /// Sets the blob cache.
    ///
    /// You can create a global [`BlobCache`] and share it between multiple
    /// value logs to cap global cache memory usage.
    #[must_use]
    pub fn blob_cache(mut self, blob_cache: BC) -> Self {
        self.blob_cache = blob_cache;
        self
    }

    /// Sets the maximum size of value log segments.
    ///
    /// This heavily influences space amplification, as
    /// space reclamation works on a per-segment basis.
    ///
    /// Like `blob_file_size` in `RocksDB`.
    ///
    /// Default = 256 MiB
    #[must_use]
    pub fn segment_size_bytes(mut self, bytes: u64) -> Self {
        self.segment_size_bytes = bytes;
        self
    }
}
