use std::{
    collections::BTreeMap,
    sync::{Arc, RwLock},
};
use value_log::{Config, ExternalIndex, IndexWriter, ValueHandle, ValueLog};

type DebugIndexInner = RwLock<BTreeMap<Arc<[u8]>, ValueHandle>>;

#[derive(Clone, Default)]
pub struct DebugIndex(Arc<DebugIndexInner>);

impl std::ops::Deref for DebugIndex {
    type Target = DebugIndexInner;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DebugIndex {
    fn insert_indirection(&self, key: &[u8], value: ValueHandle) -> std::io::Result<()> {
        self.write()
            .expect("lock is poisoned")
            .insert(key.into(), value);

        Ok(())
    }
}

impl ExternalIndex for DebugIndex {
    fn get(&self, key: &[u8]) -> std::io::Result<Option<ValueHandle>> {
        Ok(self.read().expect("lock is poisoned").get(key).cloned())
    }
}

/// Used for tests only
#[allow(clippy::module_name_repetitions)]
pub struct DebugIndexWriter(DebugIndex);

impl From<DebugIndex> for DebugIndexWriter {
    fn from(value: DebugIndex) -> Self {
        Self(value)
    }
}

impl IndexWriter for DebugIndexWriter {
    fn insert_indirection(&mut self, key: &[u8], value: ValueHandle) -> std::io::Result<()> {
        self.0.insert_indirection(key, value)
    }

    fn finish(&mut self) -> std::io::Result<()> {
        Err(std::io::Error::new(std::io::ErrorKind::Other, "Oh no"))
    }
}

#[test]
fn rollover_index_fail_finish() -> value_log::Result<()> {
    let folder = tempfile::tempdir()?;

    let index = DebugIndex(RwLock::new(BTreeMap::<Arc<[u8]>, ValueHandle>::default()).into());

    let vl_path = folder.path();
    std::fs::create_dir_all(vl_path)?;
    let value_log = ValueLog::open(vl_path, Config::default())?;

    let items = ["a", "b", "c", "d", "e"];

    {
        let mut writer = value_log.get_writer()?;

        let segment_id = writer.segment_id();

        for key in &items {
            let offset = writer.offset(key.as_bytes());

            index.insert_indirection(key.as_bytes(), ValueHandle { offset, segment_id })?;

            writer.write(key.as_bytes(), key.repeat(500).as_bytes())?;
        }

        value_log.register(writer)?;
    }

    assert_eq!(value_log.manifest.list_segment_ids(), [0]);

    value_log.refresh_stats(0)?;
    assert_eq!(
        value_log
            .manifest
            .get_segment(0)
            .unwrap()
            .stats
            .stale_ratio(),
        0.0
    );

    let mut writer = DebugIndexWriter(index.clone());
    let result = value_log.rollover(&[0], &mut writer);
    assert!(result.is_err());

    assert_eq!(
        {
            let mut ids = value_log.manifest.list_segment_ids();
            ids.sort();
            ids
        },
        [0, 1]
    );

    value_log.refresh_stats(0)?;
    assert_eq!(
        value_log
            .manifest
            .get_segment(0)
            .unwrap()
            .stats
            .stale_ratio(),
        1.0
    );

    value_log.drop_stale_segments()?;

    assert_eq!(value_log.manifest.list_segment_ids(), [1]);

    Ok(())
}
