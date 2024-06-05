use test_log::test;
use value_log::{Config, MockIndex, ValueHandle, ValueLog};

#[test]
fn basic_kv() -> value_log::Result<()> {
    let folder = tempfile::tempdir()?;

    let index = MockIndex::default();

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

            writer.write(key.as_bytes(), key.repeat(1_000).as_bytes())?;
        }

        value_log.register(writer)?;
    }

    {
        assert_eq!(1, value_log.segment_count());

        let segments = value_log.manifest.list_segments();

        let segment = segments.first().unwrap();

        assert_eq!(items.len() as u64, segment.len());
        assert_eq!(0, segment.stats.stale_items());

        assert_eq!(
            segment.len(),
            segment.scan().into_iter().flatten().count() as u64
        );
    }

    for (key, handle) in index.read().unwrap().iter() {
        let item = value_log.get(handle)?.unwrap();
        assert_eq!(item, key.repeat(1_000).into());
    }

    Ok(())
}
