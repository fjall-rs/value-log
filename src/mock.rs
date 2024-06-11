use crate::{IndexReader, IndexWriter, ValueHandle};
use std::{
    collections::BTreeMap,
    sync::{Arc, RwLock},
};

type MockIndexInner = RwLock<BTreeMap<Arc<[u8]>, (ValueHandle, u32)>>;

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
            .map(|(handle, _)| handle)
            .cloned())
    }
}

/// Used for tests only
#[allow(clippy::module_name_repetitions)]
pub struct MockIndexWriter(pub MockIndex);

impl IndexWriter for MockIndexWriter {
    fn insert_direct(&mut self, _key: &[u8], _value: &[u8]) -> std::io::Result<()> {
        Ok(())
    }

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
