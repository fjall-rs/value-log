use std::{
    collections::BTreeMap,
    sync::{Arc, RwLock},
};
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

impl Index for DebugIndex {
    fn get(&self, key: &[u8]) -> std::io::Result<Option<ValueHandle>> {
        Ok(self.read().expect("lock is poisoned").get(key).cloned())
    }

    fn insert_indirection(&self, key: &[u8], value: ValueHandle) -> std::io::Result<()> {
        self.write()
            .expect("lock is poisoned")
            .insert(key.into(), value);

        Ok(())
    }
}

#[test]
fn basic_kv() -> value_log::Result<()> {
    let folder = tempfile::tempdir()?;

    let index = DebugIndex(RwLock::new(BTreeMap::<Arc<[u8]>, ValueHandle>::default()));
    let index = Arc::new(index);

    let vl_path = folder.path();
    std::fs::create_dir_all(vl_path)?;
    let value_log = ValueLog::new(vl_path, Config::default(), index.clone())?;

    let items = ["a", "b", "c", "d", "e"];

    {
        let mut writer = value_log.get_writer()?;

        let segment_id = writer.segment_id();

        for key in &items {
            let offset = writer.offset(key.as_bytes());

            index.insert_indirection(
                key.as_bytes(),
                ValueHandle {
                    offset,
                    segment_id: segment_id.clone(),
                },
            )?;

            writer.write(key.as_bytes(), key.repeat(1_000).as_bytes())?;
        }

        value_log.register(writer)?;
    }

    {
        let lock = value_log.segments.read().unwrap();
        assert_eq!(1, lock.len());
        assert_eq!(items.len() as u64, lock.values().next().unwrap().item_count);
        assert_eq!(
            0,
            lock.values()
                .next()
                .unwrap()
                .stale_values
                .load(std::sync::atomic::Ordering::Relaxed),
        );
    }

    for (key, handle) in index.0.read().unwrap().iter() {
        let item = value_log.get(handle)?.unwrap();
        assert_eq!(item, key.repeat(1_000).into());
    }

    Ok(())
}
