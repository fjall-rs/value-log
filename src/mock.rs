use crate::{ExternalIndex, IndexWriter, ValueHandle};
use std::{
    collections::BTreeMap,
    sync::{Arc, RwLock},
};

type MockIndexInner = RwLock<BTreeMap<Arc<[u8]>, ValueHandle>>;

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
    /// Used for tests only
    pub fn insert_indirection(&self, key: &[u8], value: ValueHandle) -> std::io::Result<()> {
        self.write()
            .expect("lock is poisoned")
            .insert(key.into(), value);

        Ok(())
    }
}

impl ExternalIndex for MockIndex {
    fn get(&self, key: &[u8]) -> std::io::Result<Option<ValueHandle>> {
        Ok(self.read().expect("lock is poisoned").get(key).cloned())
    }
}

/// Used for tests only
#[allow(clippy::module_name_repetitions)]
pub struct MockIndexWriter(MockIndex);

impl From<MockIndex> for MockIndexWriter {
    fn from(value: MockIndex) -> Self {
        Self(value)
    }
}

impl IndexWriter for MockIndexWriter {
    fn insert_indirection(&self, key: &[u8], value: ValueHandle) -> std::io::Result<()> {
        self.0.insert_indirection(key, value)
    }

    fn finish(&self) -> std::io::Result<()> {
        Ok(())
    }
}
