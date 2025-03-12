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
fn recovery_fail() -> value_log::Result<()> {
    let folder = tempfile::tempdir()?;
    let vl_path = folder.path();

    let index = MockIndex::default();

    let items = ["a", "b", "c", "d", "e"];

    {
        let value_log = ValueLog::open(vl_path, Config::<_, NoCompressor>::new(NoCacher))?;

        for _ in 0..2 {
            let mut index_writer = MockIndexWriter(index.clone());
            let mut writer = value_log.get_writer()?;

            for key in &items {
                let value = key.repeat(10_000);
                let value = value.as_bytes();

                let key = key.as_bytes();

                let vhandle = writer.get_next_value_handle();
                index_writer.insert_indirect(key, vhandle, value.len() as u32)?;

                writer.write(key, value)?;
            }

            value_log.register_writer(writer)?;
        }

        {
            assert_eq!(2, value_log.segment_count());
        }
    }

    // NOTE: Delete blob file to trigger Unrecoverable (it is referenced in manifest, but not on disk anymore)
    std::fs::remove_file(vl_path.join("segments").join("1"))?;

    {
        matches!(
            ValueLog::open(vl_path, Config::<_, NoCompressor>::new(NoCacher)),
            Err(value_log::Error::Unrecoverable),
        );
    }

    Ok(())
}
