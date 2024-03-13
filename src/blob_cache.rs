use crate::ValueHandle;
use quick_cache::{sync::Cache, Weighter};
use std::sync::Arc;

type CacheKey = ValueHandle;
type Item = Arc<[u8]>;

#[derive(Clone)]
struct BlobWeighter;

impl Weighter<CacheKey, Item> for BlobWeighter {
    fn weight(&self, _: &CacheKey, blob: &Item) -> u32 {
        // TODO: quick_cache only supports u32 as weight...?
        blob.len() as u32
    }
}

/// Blob cache, in which blobs are cached in-memory
/// after being retrieved from disk
///
/// This speeds up consecutive accesses to the same blobs, improving
/// read performance for hot data.
pub struct BlobCache {
    data: Cache<CacheKey, Item, BlobWeighter>,
    capacity: u64,
}

impl std::fmt::Debug for BlobCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "BlobCache<cap: {} bytes>", self.capacity)
    }
}

impl BlobCache {
    /// Creates a new block cache with roughly `n` bytes of capacity
    #[must_use]
    pub fn with_capacity_bytes(bytes: u64) -> Self {
        Self {
            data: Cache::with_weighter(10_000, bytes, BlobWeighter),
            capacity: bytes,
        }
    }

    pub(crate) fn insert(&self, handle: CacheKey, value: Arc<[u8]>) {
        if self.capacity > 0 {
            self.data.insert(handle, value);
        }
    }

    pub(crate) fn get(&self, handle: &CacheKey) -> Option<Item> {
        if self.capacity > 0 {
            self.data.get(handle)
        } else {
            None
        }
    }

    /// Returns the cache capacity in bytes
    #[must_use]
    pub fn capacity(&self) -> u64 {
        self.capacity
    }

    /// Returns the number of cached blocks
    #[must_use]
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Returns `true` if there are no cached blocks
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}
