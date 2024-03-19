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
fn worst_case_space_amp() -> value_log::Result<()> {
    let folder = tempfile::tempdir()?;

    let index = DebugIndex(RwLock::new(BTreeMap::<Arc<[u8]>, ValueHandle>::default()));
    let index = Arc::new(index);

    let vl_path = folder.path();
    std::fs::create_dir_all(vl_path)?;
    let value_log = ValueLog::new(vl_path, Config::default(), index.clone())?;

    assert_eq!(0.0, value_log.space_amp());
    assert_eq!(0.0, value_log.stale_ratio());

    let key = "key";
    let value = "value";

    // NOTE: Write a single item 10x
    // -> should result in space amp = 10.0x
    for x in 1..=10 {
        let mut writer = value_log.get_writer()?;
        let segment_id = writer.segment_id();

        let offset = writer.offset(key.as_bytes());

        index.insert_indirection(
            key.as_bytes(),
            ValueHandle {
                offset,
                segment_id: segment_id.clone(),
            },
        )?;

        writer.write(key.as_bytes(), value.as_bytes())?;
        value_log.register(writer)?;

        for id in value_log.list_segment_ids() {
            value_log.refresh_stats(&id)?;
        }

        assert_eq!(x as f32, value_log.space_amp());

        if x > 1 {
            assert!((1.0 - (1.0 / x as f32) - value_log.stale_ratio()) < 0.00001);
        }
    }

    Ok(())
}

#[test]
fn no_overlap_space_amp() -> value_log::Result<()> {
    let folder = tempfile::tempdir()?;

    let index = DebugIndex(RwLock::new(BTreeMap::<Arc<[u8]>, ValueHandle>::default()));
    let index = Arc::new(index);

    let vl_path = folder.path();
    std::fs::create_dir_all(vl_path)?;
    let value_log = ValueLog::new(vl_path, Config::default(), index.clone())?;

    assert_eq!(0.0, value_log.stale_ratio());
    assert_eq!(0.0, value_log.space_amp());

    // NOTE: No blobs overlap, so there are no dead blobs => space amp = 1.0 => perfect space amp
    for i in 0..100 {
        let key = i.to_string();
        let value = "afsasfdfasdfsda";

        let mut writer = value_log.get_writer()?;
        let segment_id = writer.segment_id();

        let offset = writer.offset(key.as_bytes());

        index.insert_indirection(
            key.as_bytes(),
            ValueHandle {
                offset,
                segment_id: segment_id.clone(),
            },
        )?;

        writer.write(key.as_bytes(), value.as_bytes())?;
        value_log.register(writer)?;

        for id in value_log.list_segment_ids() {
            value_log.refresh_stats(&id)?;
        }

        assert_eq!(1.0, value_log.space_amp());
        assert_eq!(0.0, value_log.stale_ratio());
    }

    Ok(())
}
