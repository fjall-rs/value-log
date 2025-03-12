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
fn basic_gc() -> value_log::Result<()> {
    let folder = tempfile::tempdir()?;
    let vl_path = folder.path();

    let index = MockIndex::default();

    let value_log = ValueLog::open(vl_path, Config::<_, NoCompressor>::new(NoCacher))?;

    {
        let items = ["a", "b", "c", "d", "e"];

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
        assert_eq!(1, value_log.segment_count());

        let segments = value_log.manifest.list_segments();

        assert_eq!(5, segments.first().unwrap().len());
        assert_eq!(0, segments.first().unwrap().gc_stats.stale_items());
    }

    for (key, (vhandle, _)) in index.read().unwrap().iter() {
        let item = value_log.get(vhandle)?.unwrap();
        assert_eq!(&*item, &*key.repeat(10_000));
    }

    {
        let items = ["a", "b", "c", "d", "e"];

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

        let segments = value_log.manifest.list_segments();

        assert_eq!(5, segments.first().unwrap().len());
        assert_eq!(0, segments.first().unwrap().gc_stats.stale_items());
    }

    for (key, (vhandle, _)) in index.read().unwrap().iter() {
        let item = value_log.get(vhandle)?.unwrap();
        assert_eq!(&*item, &*key.repeat(10_000));
    }

    value_log.major_compact(&index, MockIndexWriter(index.clone()))?;
    value_log.drop_stale_segments()?;

    {
        assert_eq!(1, value_log.segment_count());

        let segments = value_log.manifest.list_segments();

        assert_eq!(5, segments.first().unwrap().len());
        assert_eq!(0, segments.first().unwrap().gc_stats.stale_items());
    }

    Ok(())
}
