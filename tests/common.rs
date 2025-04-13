// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use std::{
    cell::RefCell,
    collections::{BTreeMap, HashMap},
    fs::File,
    io::BufReader,
    ops::Add,
    sync::{Arc, Mutex, RwLock},
};
use value_log::{
    BlobCache, BlobFileId, Compressor, FDCache, IndexReader, IndexWriter, UserKey, UserValue,
    ValueHandle, ValueLogId,
};

type MockIndexInner = RwLock<BTreeMap<UserKey, (ValueHandle, u32)>>;

/// Mock in-memory index
#[allow(clippy::module_name_repetitions)]
#[derive(Clone, Default)]
pub struct MockIndex(Arc<MockIndexInner>);

impl std::ops::Deref for MockIndex {
    type Target = MockIndexInner;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl MockIndex {
    /// Remove item
    #[allow(unused)]
    pub fn remove(&self, key: &[u8]) {
        self.0.write().expect("lock is poisoned").remove(key);
    }
}

impl IndexReader for MockIndex {
    fn get(&self, key: &[u8]) -> std::io::Result<Option<ValueHandle>> {
        Ok(self
            .read()
            .expect("lock is poisoned")
            .get(key)
            .map(|(vhandle, _)| vhandle)
            .cloned())
    }
}

/// Used for tests only
#[allow(clippy::module_name_repetitions)]
pub struct MockIndexWriter(pub MockIndex);

impl IndexWriter for MockIndexWriter {
    fn insert_indirect(
        &mut self,
        key: &[u8],
        value: ValueHandle,
        size: u32,
    ) -> std::io::Result<()> {
        self.0
            .write()
            .expect("lock is poisoned")
            .insert(key.into(), (value, size));
        Ok(())
    }

    fn finish(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[derive(Clone, Default)]
pub struct NoCompressor;

impl Compressor for NoCompressor {
    fn compress(&self, bytes: &[u8]) -> value_log::Result<Vec<u8>> {
        Ok(bytes.into())
    }

    fn decompress(&self, bytes: &[u8]) -> value_log::Result<Vec<u8>> {
        Ok(bytes.into())
    }
}

#[derive(Clone)]
pub struct NoCacher;

impl BlobCache for NoCacher {
    fn get(&self, _: ValueLogId, _: &ValueHandle) -> Option<UserValue> {
        None
    }

    fn insert(&self, _: ValueLogId, _: &ValueHandle, _: UserValue) {}
}

impl FDCache for NoCacher {
    fn get(&self, _: ValueLogId, _: BlobFileId) -> Option<BufReader<File>> {
        None
    }
    fn insert(&self, _: ValueLogId, _: BlobFileId, _: BufReader<File>) {}
}

#[derive(Clone, Default)]
pub struct InMemCacher {
    fd_cache: Arc<Mutex<HashMap<(ValueLogId, BlobFileId), File>>>,
    fd_hit_count: Arc<RefCell<u32>>,
    fd_miss_count: Arc<RefCell<u32>>,
}
impl InMemCacher {
    pub(crate) fn get_hit_count(&self) -> u32 {
        *self.fd_hit_count.borrow()
    }

    pub(crate) fn get_miss_count(&self) -> u32 {
        *self.fd_miss_count.borrow()
    }
}

impl FDCache for InMemCacher {
    fn get(&self, vlog_id: ValueLogId, blob_file_id: BlobFileId) -> Option<BufReader<File>> {
        let lock = self.fd_cache.lock().unwrap();
        let fd = match lock.get(&(vlog_id, blob_file_id)) {
            Some(fd) => fd,
            None => {
                *self.fd_miss_count.borrow_mut() += 1;
                return None;
            }
        };

        let fd_clone = fd.try_clone().unwrap();
        *self.fd_hit_count.borrow_mut() += 1;
        Some(BufReader::new(fd_clone))
    }

    fn insert(&self, vlog_id: ValueLogId, blob_file_id: BlobFileId, fd: BufReader<File>) {
        self.fd_cache
            .lock()
            .unwrap()
            .insert((vlog_id, blob_file_id), fd.into_inner());
    }
}
