// This test is to prevent a race condition that may cause new blob files
// to be accidentally dropped during GC.
//
// When a blob file is registered is after a `scan_for_stats`, it has an reference
// count of 0. Then it would be dropped even though it was just created.

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
fn accidental_drop_rc() -> value_log::Result<()> {
    let folder = tempfile::tempdir()?;
    let vl_path = folder.path();

    let index = MockIndex::default();

    let value_log = ValueLog::open(vl_path, Config::<_, NoCompressor>::new(NoCacher))?;

    for key in ["a", "b"] {
        let value = &key;

        let mut index_writer = MockIndexWriter(index.clone());
        let mut writer = value_log.get_writer()?;

        let vhandle = writer.get_next_value_handle();
        index_writer.insert_indirect(key.as_bytes(), vhandle, value.len() as u32)?;

        writer.write(key.as_bytes(), value.as_bytes())?;
        value_log.register_writer(writer)?;
    }

    assert_eq!(2, value_log.segment_count());

    value_log.scan_for_stats(index.read().unwrap().values().cloned().map(Ok))?;
    value_log.drop_stale_segments()?;
    assert_eq!(2, value_log.segment_count());

    let segment_ids = value_log.manifest.list_segment_ids();

    // NOTE: Now start a new GC scan
    let index_lock = index.read().unwrap();
    let mut scanner = value_log::scanner::Scanner::new(
        index_lock.values().cloned().map(Ok),
        value_log.rollover_guard.lock().unwrap(),
        &segment_ids,
    );
    scanner.scan()?;
    let scan_result = scanner.finish();
    drop(index_lock);

    // NOTE: Now, we create a new blob file, that won't be referenced in the GC report
    {
        let key = "c";
        let value = &key;

        let mut index_writer = MockIndexWriter(index.clone());
        let mut writer = value_log.get_writer()?;

        let vhandle = writer.get_next_value_handle();
        index_writer.insert_indirect(key.as_bytes(), vhandle, value.len() as u32)?;

        writer.write(key.as_bytes(), value.as_bytes())?;
        value_log.register_writer(writer)?;
    }
    assert_eq!(3, value_log.segment_count());

    // NOTE: Now we can consume the scan result, which, in a bad implementation
    // would cause the new blob file to be marked as stale
    //
    // But we are forced to pass the list of segment IDs we saw before starting the
    // scan, which prevents marking ones as stale that were created later
    let _ = value_log.consume_scan_result(&scan_result);

    // IMPORTANT: The new blob file should not be dropped
    value_log.drop_stale_segments()?;
    assert_eq!(3, value_log.segment_count());

    Ok(())
}
