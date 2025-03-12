use test_log::test;
use value_log::{BlobCache, Compressor, Config, UserValue, ValueHandle, ValueLog, ValueLogId};

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
fn recovery_mac_ds_store() -> value_log::Result<()> {
    let folder = tempfile::tempdir()?;
    let vl_path = folder.path();

    {
        let value_log = ValueLog::open(vl_path, Config::<_, NoCompressor>::new(NoCacher))?;

        let mut writer = value_log.get_writer()?;
        writer.write("a", "a")?;
        value_log.register_writer(writer)?;
    }

    let ds_store = vl_path.join("segments").join(".DS_Store");
    std::fs::File::create(&ds_store)?;
    assert!(ds_store.try_exists()?);

    {
        let value_log = ValueLog::open(vl_path, Config::<_, NoCompressor>::new(NoCacher))?;
        assert_eq!(1, value_log.segment_count());
    }
    assert!(ds_store.try_exists()?);

    Ok(())
}
