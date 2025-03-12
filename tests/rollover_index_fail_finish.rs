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

#[allow(clippy::module_name_repetitions)]
pub struct DebugIndexWriter;

impl IndexWriter for DebugIndexWriter {
    fn insert_indirect(&mut self, _: &[u8], _: ValueHandle, _: u32) -> std::io::Result<()> {
        Ok(())
    }

    fn finish(&mut self) -> std::io::Result<()> {
        Err(std::io::Error::new(std::io::ErrorKind::Other, "Oh no"))
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
fn rollover_index_fail_finish() -> value_log::Result<()> {
    let folder = tempfile::tempdir()?;
    let vl_path = folder.path();

    let index = MockIndex::default();

    let value_log = ValueLog::open(vl_path, Config::<_, NoCompressor>::new(NoCacher))?;

    let items = ["a", "b", "c", "d", "e"];

    {
        let mut index_writer = MockIndexWriter(index.clone());
        let mut writer = value_log.get_writer()?;

        for key in &items {
            let value = key.repeat(10_000);
            let value = value.as_bytes();

            let vhandle = writer.get_next_value_handle();
            index_writer.insert_indirect(key.as_bytes(), vhandle, value.len() as u32)?;

            writer.write(key.as_bytes(), value)?;
        }

        value_log.register_writer(writer)?;
    }

    assert_eq!(value_log.manifest.list_segment_ids(), [0]);
    value_log.scan_for_stats(index.read().unwrap().values().cloned().map(Ok))?;
    assert_eq!(1.0, value_log.space_amp());

    index.remove(b"a");
    index.remove(b"b");
    index.remove(b"c");
    index.remove(b"d");

    value_log.scan_for_stats(index.read().unwrap().values().cloned().map(Ok))?;
    assert_eq!(
        value_log.manifest.get_segment(0).unwrap().stale_ratio(),
        0.8
    );
    assert!(value_log.space_amp() > 1.0);

    let result = value_log.rollover(&[0], &index, DebugIndexWriter);
    assert!(result.is_err());

    // NOTE: Segment 1's value handles were not committed to index, so it's not referenced at all
    // We get no data loss, the segment is just left dangling and can be removed
    assert_eq!(
        {
            let mut ids = value_log.manifest.list_segment_ids();
            ids.sort();
            ids
        },
        [0, 1]
    );

    value_log.scan_for_stats(index.read().unwrap().values().cloned().map(Ok))?;
    assert_eq!(
        value_log.manifest.get_segment(0).unwrap().stale_ratio(),
        0.8
    );
    assert!(value_log.space_amp() > 1.0);

    value_log.drop_stale_segments()?;
    assert_eq!(value_log.manifest.list_segment_ids(), [0]);

    index.remove(b"e");

    // NOTE: Now all values are stale, and everything can be dropped
    value_log.scan_for_stats(index.read().unwrap().values().cloned().map(Ok))?;
    value_log.drop_stale_segments()?;
    assert_eq!(value_log.manifest.list_segment_ids(), []);
    assert_eq!(0.0, value_log.space_amp());

    Ok(())
}
