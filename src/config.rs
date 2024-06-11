use crate::{blob_cache::BlobCache, CompressionType};
use std::sync::Arc;

/// Value log configuration
#[derive(Debug)]
pub struct Config {
    /// Target size of vLog segments
    pub(crate) segment_size_bytes: u64,

    /// Blob cache to use
    pub(crate) blob_cache: Arc<BlobCache>,

    /// Compression to use
    pub(crate) compression: CompressionType,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            segment_size_bytes: 256 * 1_024 * 1_024,
            blob_cache: Arc::new(BlobCache::with_capacity_bytes(16 * 1_024 * 1_024)),

            // TODO: setter method
            compression: CompressionType::None,
        }
    }
}

impl Config {
    /// Sets the compression type to use.
    ///
    /// Using compression is recommended, see [`CompressionType`].
    ///
    /// Default = none
    #[must_use]
    pub fn compression(mut self, compression: CompressionType) -> Self {
        self.compression = compression;
        self
    }

    /// Sets the blob cache.
    ///
    /// You can create a global [`BlobCache`] and share it between multiple
    /// value logs to cap global cache memory usage.
    ///
    /// Defaults to a blob cache with 16 MiB of capacity *per value log*.
    #[must_use]
    pub fn blob_cache(mut self, blob_cache: Arc<BlobCache>) -> Self {
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
