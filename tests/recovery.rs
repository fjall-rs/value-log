use test_log::test;
use value_log::{Config, IndexWriter, MockIndex, MockIndexWriter, ValueLog};

#[test]
fn basic_recovery() -> value_log::Result<()> {
    let folder = tempfile::tempdir()?;
    let vl_path = folder.path();

    let index = MockIndex::default();

    let items = ["a", "b", "c", "d", "e"];

    {
        let value_log = ValueLog::open(vl_path, Config::default())?;

        {
            let mut index_writer = MockIndexWriter(index.clone());
            let mut writer = value_log.get_writer()?;

            for key in &items {
                let value = key.repeat(10_000);
                let value = value.as_bytes();

                let key = key.as_bytes();

                let handle = writer.get_next_value_handle(key);
                index_writer.insert_indirect(key, handle, value.len() as u32)?;

                writer.write(key, value)?;
            }

            value_log.register_writer(writer)?;
        }

        {
            assert_eq!(1, value_log.segment_count());

            let segments = value_log.manifest.list_segments();

            assert_eq!(items.len() as u64, segments.first().unwrap().len());
            assert_eq!(0, segments.first().unwrap().gc_stats.stale_items());
        }

        for (key, (handle, _)) in index.read().unwrap().iter() {
            let item = value_log.get(handle)?.unwrap();
            assert_eq!(item, key.repeat(10_000).into());
        }
    }

    {
        let value_log = ValueLog::open(vl_path, Config::default())?;

        value_log.scan_for_stats(index.read().unwrap().values().cloned().map(Ok))?;

        {
            assert_eq!(1, value_log.segment_count());

            let segments = value_log.manifest.list_segments();

            assert_eq!(items.len() as u64, segments.first().unwrap().len());
            assert_eq!(0, segments.first().unwrap().gc_stats.stale_items());
        }

        for (key, (handle, _)) in index.read().unwrap().iter() {
            let item = value_log.get(handle)?.unwrap();
            assert_eq!(item, key.repeat(10_000).into());
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
