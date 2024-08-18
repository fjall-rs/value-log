use test_log::test;
use value_log::{
    Compressor, Config, IndexWriter, MockIndex, MockIndexWriter, SpaceAmpStrategy, ValueLog,
};

#[derive(Clone, Default)]
struct NoCompressor;

impl Compressor for NoCompressor {
    fn compress(&self, bytes: &[u8]) -> value_log::Result<Vec<u8>> {
        Ok(bytes.into())
    }

    fn decompress(&self, bytes: &[u8]) -> value_log::Result<Vec<u8>> {
        Ok(bytes.into())
    }
}

#[test]
fn gc_space_amp_target_1() -> value_log::Result<()> {
    let folder = tempfile::tempdir()?;
    let vl_path = folder.path();

    let index = MockIndex::default();

    let value_log = ValueLog::open(vl_path, Config::<NoCompressor>::default())?;

    assert_eq!(0.0, value_log.space_amp());
    assert_eq!(0.0, value_log.manifest.stale_ratio());

    let key = b"key";
    let value = "value".repeat(20_000);

    // NOTE: Write a single item 10x
    // -> should result in space amp = 10.0x
    for x in 1..=10 {
        let mut index_writer = MockIndexWriter(index.clone());
        let mut writer = value_log.get_writer()?;

        let handle = writer.get_next_value_handle();
        index_writer.insert_indirect(key, handle, value.len() as u32)?;

        writer.write(key, value.as_bytes())?;

        {
            let key = format!("key{x}");
            let value = "value".repeat(5_000);

            let key = key.as_bytes();

            let handle = writer.get_next_value_handle();
            index_writer.insert_indirect(key, handle, value.len() as u32)?;

            writer.write(key, value.as_bytes())?;
        }

        value_log.register_writer(writer)?;
    }

    value_log.scan_for_stats(index.read().unwrap().values().cloned().map(Ok))?;

    assert!(value_log.space_amp() > 2.0);

    {
        let target_space_amp = 8.0;

        let strategy = SpaceAmpStrategy::new(target_space_amp);
        value_log.apply_gc_strategy(&strategy, &index, MockIndexWriter(index.clone()))?;
        value_log.drop_stale_segments()?;

        value_log.scan_for_stats(index.read().unwrap().values().cloned().map(Ok))?;
        assert!(value_log.space_amp() <= target_space_amp);
    }

    {
        let target_space_amp = 2.0;

        let strategy = SpaceAmpStrategy::new(target_space_amp);
        value_log.apply_gc_strategy(&strategy, &index, MockIndexWriter(index.clone()))?;
        value_log.drop_stale_segments()?;

        value_log.scan_for_stats(index.read().unwrap().values().cloned().map(Ok))?;
        assert!(value_log.space_amp() <= target_space_amp);
    }

    Ok(())
}
