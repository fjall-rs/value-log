use test_log::test;
use value_log::{Config, MockIndex, MockIndexWriter, ValueLog};

#[test]
fn gc_space_amp_target_1() -> value_log::Result<()> {
    let folder = tempfile::tempdir()?;
    let vl_path = folder.path();

    let index = MockIndex::default();

    let value_log = ValueLog::open(vl_path, Config::default())?;

    assert_eq!(0.0, value_log.manifest.space_amp());
    assert_eq!(0.0, value_log.manifest.stale_ratio());

    let key = "key";
    let value = "value".repeat(4);

    // NOTE: Write a single item 10x
    // -> should result in space amp = 10.0x
    for x in 1..=10 {
        let index_writer = MockIndexWriter(index.clone());
        let mut writer = value_log.get_writer(index_writer)?;

        writer.write(key.as_bytes(), value.as_bytes())?;

        {
            let key = format!("key{x}");
            let value = "value";

            writer.write(key.as_bytes(), value.as_bytes())?;
        }

        value_log.register(writer)?;
    }

    value_log.scan_for_stats(index.read().unwrap().values().cloned().map(Ok))?;

    assert!(value_log.manifest.space_amp() > 2.0);

    {
        let target_space_amp = 8.0;

        let ids = value_log.select_segments_for_space_amp_reduction(target_space_amp);
        value_log.rollover(&ids, &index, MockIndexWriter(index.clone()))?;
        value_log.drop_stale_segments()?;

        value_log.scan_for_stats(index.read().unwrap().values().cloned().map(Ok))?;
        assert!(value_log.manifest.space_amp() <= target_space_amp);
    }

    {
        let target_space_amp = 2.0;

        let ids = value_log.select_segments_for_space_amp_reduction(target_space_amp);
        value_log.rollover(&ids, &index, MockIndexWriter(index.clone()))?;
        value_log.drop_stale_segments()?;

        value_log.scan_for_stats(index.read().unwrap().values().cloned().map(Ok))?;
        assert!(value_log.manifest.space_amp() <= target_space_amp);
    }

    Ok(())
}
