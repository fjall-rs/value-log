mod common;

use common::{MockIndex, MockIndexWriter};
use test_log::test;
use value_log::{
    BlobCache, Compressor, Config, IndexWriter, UserValue, ValueHandle, ValueLog, ValueLogId,
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

#[derive(Clone)]
struct NoCacher;

impl BlobCache for NoCacher {
    fn get(&self, _: ValueLogId, _: &ValueHandle) -> Option<UserValue> {
        None
    }

    fn insert(&self, _: ValueLogId, _: &ValueHandle, _: UserValue) {}
}

#[test]
fn worst_case_space_amp() -> value_log::Result<()> {
    let folder = tempfile::tempdir()?;
    let vl_path = folder.path();

    let index = MockIndex::default();

    let value_log = ValueLog::open(vl_path, Config::<_, NoCompressor>::new(NoCacher))?;

    assert_eq!(0.0, value_log.space_amp());
    assert_eq!(0.0, value_log.manifest.stale_ratio());

    let key = b"key";
    let value = "value".repeat(5_000);

    // NOTE: Write a single item 10x
    // -> should result in space amp = 10.0x
    for x in 1..=10 {
        let mut index_writer = MockIndexWriter(index.clone());
        let mut writer = value_log.get_writer()?;

        let vhandle = writer.get_next_value_handle();
        index_writer.insert_indirect(key, vhandle, value.len() as u32)?;

        writer.write(key, value.as_bytes())?;

        value_log.register_writer(writer)?;

        value_log.scan_for_stats(index.read().unwrap().values().cloned().map(Ok))?;

        assert_eq!(x as f32, value_log.space_amp());

        if x > 1 {
            assert!((1.0 - (1.0 / x as f32) - value_log.manifest.stale_ratio()) < 0.00001);
        }
    }

    Ok(())
}

#[test]
fn no_overlap_space_amp() -> value_log::Result<()> {
    let folder = tempfile::tempdir()?;
    let vl_path = folder.path();

    let index = MockIndex::default();

    let value_log = ValueLog::open(vl_path, Config::<_, NoCompressor>::new(NoCacher))?;

    assert_eq!(0.0, value_log.manifest.stale_ratio());
    assert_eq!(0.0, value_log.space_amp());

    // NOTE: No blobs overlap, so there are no dead blobs => space amp = 1.0 => perfect space amp
    for i in 0..100 {
        let key = i.to_string();
        let value = "afsasfdfasdfsda".repeat(5_000);

        let mut index_writer = MockIndexWriter(index.clone());
        let mut writer = value_log.get_writer()?;

        let vhandle = writer.get_next_value_handle();
        index_writer.insert_indirect(key.as_bytes(), vhandle, value.len() as u32)?;

        writer.write(key.as_bytes(), value.as_bytes())?;
        value_log.register_writer(writer)?;

        value_log.scan_for_stats(index.read().unwrap().values().cloned().map(Ok))?;

        assert_eq!(1.0, value_log.space_amp());
        assert_eq!(0.0, value_log.manifest.stale_ratio());
    }

    Ok(())
}
