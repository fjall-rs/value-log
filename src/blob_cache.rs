// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{value::UserValue, value_log::ValueLogId, ValueHandle};
use quick_cache::{sync::Cache, Equivalent, Weighter};

type Item = UserValue;

#[derive(Eq, std::hash::Hash, PartialEq)]
pub struct CacheKey(ValueLogId, ValueHandle);

impl Equivalent<CacheKey> for (ValueLogId, &ValueHandle) {
    fn equivalent(&self, key: &CacheKey) -> bool {
        self.0 == key.0 && self.1 == &key.1
    }
}

impl From<(ValueLogId, ValueHandle)> for CacheKey {
    fn from((vid, vhandle): (ValueLogId, ValueHandle)) -> Self {
        Self(vid, vhandle)
    }
}

#[derive(Clone)]
struct BlobWeighter;

impl Weighter<CacheKey, Item> for BlobWeighter {
    #[allow(clippy::cast_possible_truncation)]
    fn weight(&self, _: &CacheKey, blob: &Item) -> u64 {
        blob.len() as u64
    }
}

/// Blob cache, in which blobs are cached in-memory
/// after being retrieved from disk
///
/// This speeds up consecutive accesses to the same blobs, improving
/// read performance for hot data.
pub struct BlobCache {
    // NOTE: rustc_hash performed best: https://fjall-rs.github.io/post/fjall-2-1
    /// Concurrent cache implementation
    data: Cache<CacheKey, Item, BlobWeighter, rustc_hash::FxBuildHasher>,

    /// Capacity in bytes
    capacity: u64,
}

impl std::fmt::Debug for BlobCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "BlobCache<cap: {} bytes>", self.capacity)
    }
}

impl BlobCache {
    /// Creates a new block cache with roughly `n` bytes of capacity.
    #[must_use]
    pub fn with_capacity_bytes(bytes: u64) -> Self {
        use quick_cache::sync::DefaultLifecycle;

        #[allow(clippy::default_trait_access)]
        let quick_cache = Cache::with(
            10_000,
            bytes,
            BlobWeighter,
            Default::default(),
            DefaultLifecycle::default(),
        );

        Self {
            data: quick_cache,
            capacity: bytes,
        }
    }

    pub(crate) fn insert(&self, key: CacheKey, value: UserValue) {
        self.data.insert(key, value);
    }

    pub(crate) fn get(&self, vlog_id: ValueLogId, vhandle: &ValueHandle) -> Option<Item> {
        self.data.get(&(vlog_id, vhandle))
    }

    /// Returns the cache capacity in bytes.
    #[must_use]
    pub fn capacity(&self) -> u64 {
        self.capacity
    }

    /// Returns the size in bytes.
    #[must_use]
    pub fn size(&self) -> u64 {
        self.data.weight()
    }

    /// Returns the number of cached blocks.
    #[must_use]
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Returns `true` if there are no cached blocks.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}
