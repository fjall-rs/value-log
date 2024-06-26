use crate::{blob_cache::BlobCache, compression::Compressor};
use std::sync::Arc;

/// No compression
pub struct NoCompressor;

impl Compressor for NoCompressor {
    fn compress(&self, bytes: &[u8]) -> Result<Vec<u8>, crate::compression::CompressError> {
        Ok(bytes.into())
    }

    fn decompress(&self, bytes: &[u8]) -> Result<Vec<u8>, crate::compression::DecompressError> {
        Ok(bytes.into())
    }
}

/// Value log configuration
pub struct Config {
    /// Target size of vLog segments
    pub(crate) segment_size_bytes: u64,

    /// Blob cache to use
    pub(crate) blob_cache: Arc<BlobCache>,

    /// Compression to use
    pub(crate) compression: Arc<dyn Compressor + Send + Sync>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            segment_size_bytes: 256 * 1_024 * 1_024,
            blob_cache: Arc::new(BlobCache::with_capacity_bytes(16 * 1_024 * 1_024)),
            compression: Arc::new(NoCompressor),
        }
    }
}

impl Config {
    /// Sets the compression type to use.
    ///
    /// Using compression is recommended.
    ///
    /// Default = none
    #[must_use]
    pub fn use_compression(mut self, compressor: Arc<dyn Compressor + Send + Sync>) -> Self {
        self.compression = compressor;
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
