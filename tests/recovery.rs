use std::{
    collections::BTreeMap,
    sync::{Arc, RwLock},
};
use test_log::test;
use value_log::{Config, Index, ValueHandle, ValueLog};

type Inner = RwLock<BTreeMap<Arc<[u8]>, ValueHandle>>;

#[derive(Default)]
pub struct DebugIndex(Inner);

impl std::ops::Deref for DebugIndex {
    type Target = Inner;

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

impl Index for DebugIndex {
    fn get(&self, key: &[u8]) -> std::io::Result<Option<ValueHandle>> {
        Ok(self.read().expect("lock is poisoned").get(key).cloned())
    }
}

#[test]
fn basic_kv() -> value_log::Result<()> {
    let folder = tempfile::tempdir()?;

    let index = DebugIndex(RwLock::new(BTreeMap::<Arc<[u8]>, ValueHandle>::default()));
    let index = Arc::new(index);

    let vl_path = folder.path();
    std::fs::create_dir_all(vl_path)?;

    let items = ["a", "b", "c", "d", "e"];

    {
        let value_log = ValueLog::open(vl_path, Config::default(), index.clone())?;

        {
            let mut writer = value_log.get_writer()?;

            let segment_id = writer.segment_id();

            for key in &items {
                let offset = writer.offset(key.as_bytes());

                index.insert_indirection(key.as_bytes(), ValueHandle { offset, segment_id })?;

                writer.write(key.as_bytes(), key.repeat(1_000).as_bytes())?;
            }

            value_log.register(writer)?;
        }

        {
            assert_eq!(1, value_log.segment_count());

            let segments = value_log.manifest.read().expect("lock is poisoned");
            let segments = segments.list_segments();

            assert_eq!(items.len() as u64, segments.first().unwrap().len());
            assert_eq!(0, segments.first().unwrap().stats.get_stale_items());
        }

        for (key, handle) in index.0.read().unwrap().iter() {
            let item = value_log.get(handle)?.unwrap();
            assert_eq!(item, key.repeat(1_000).into());
        }
    }

    {
        let value_log = ValueLog::open(vl_path, Config::default(), index.clone())?;

        {
            assert_eq!(1, value_log.segment_count());

            let segments = value_log.manifest.read().expect("lock is poisoned");
            let segments = segments.list_segments();

            assert_eq!(items.len() as u64, segments.first().unwrap().len());
            assert_eq!(0, segments.first().unwrap().stats.get_stale_items());
        }

        for (key, handle) in index.0.read().unwrap().iter() {
            let item = value_log.get(handle)?.unwrap();
            assert_eq!(item, key.repeat(1_000).into());
        }
    }

    Ok(())
}
