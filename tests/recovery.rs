use test_log::test;
use value_log::{Config, MockIndex, ValueHandle, ValueLog};

#[test]
fn basic_recovery() -> value_log::Result<()> {
    let folder = tempfile::tempdir()?;

    let index = MockIndex::default();

    let vl_path = folder.path();
    std::fs::create_dir_all(vl_path)?;

    let items = ["a", "b", "c", "d", "e"];

    {
        let value_log = ValueLog::open(vl_path, Config::default())?;

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

            assert_eq!(items.len() as u64, segments.first().unwrap().len());
            assert_eq!(0, segments.first().unwrap().stats.stale_items());
        }

        for (key, handle) in index.read().unwrap().iter() {
            let item = value_log.get(handle)?.unwrap();
            assert_eq!(item, key.repeat(1_000).into());
        }
    }

    {
        let value_log = ValueLog::open(vl_path, Config::default())?;

        // TODO: should be recovered
        for id in value_log.manifest.list_segment_ids() {
            value_log.refresh_stats(id)?;
        }

        {
            assert_eq!(1, value_log.segment_count());

            let segments = value_log.manifest.list_segments();

            assert_eq!(items.len() as u64, segments.first().unwrap().len());
            assert_eq!(0, segments.first().unwrap().stats.stale_items());
        }

        for (key, handle) in index.read().unwrap().iter() {
            let item = value_log.get(handle)?.unwrap();
            assert_eq!(item, key.repeat(1_000).into());
        }
    }

    Ok(())
}

#[test]
fn delete_unfinished_segment_folders() -> value_log::Result<()> {
    let folder = tempfile::tempdir()?;

    let vl_path = folder.path();
    std::fs::create_dir_all(vl_path)?;

    let mock_path = vl_path.join("segments").join("463298");
    std::fs::create_dir_all(&mock_path)?;
    assert!(mock_path.try_exists()?);

    {
        let _value_log = ValueLog::open(vl_path, Config::default())?;
        assert!(mock_path.try_exists()?);
    }

    {
        let _value_log = ValueLog::open(vl_path, Config::default())?;
        assert!(!mock_path.try_exists()?);
    }

    Ok(())
}
