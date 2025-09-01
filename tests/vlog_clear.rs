mod common;

use common::{MockIndex, MockIndexWriter, NoCacher};
use test_log::test;
use value_log::{Compressor, Config, IndexWriter, ValueLog};

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
fn vlog_clear() -> value_log::Result<()> {
    let folder = tempfile::tempdir()?;
    let vl_path = folder.path();

    let index = MockIndex::default();

    let items = ["a", "b", "c", "d", "e"];

    {
        let value_log = ValueLog::open(
            vl_path,
            Config::<NoCacher, _, NoCompressor>::new(NoCacher, NoCacher),
        )?;

        for _ in 0..5 {
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

        assert_eq!(5, value_log.segment_count());

        value_log.clear(false)?;
        assert_eq!(0, value_log.segment_count());

        // NOTE: Need to clear the index to get rid of all vHandles
        index.write().unwrap().clear();
    }

    {
        let value_log = ValueLog::open(
            vl_path,
            Config::<NoCacher, _, NoCompressor>::new(NoCacher, NoCacher),
        )?;

        value_log.scan_for_stats(index.read().unwrap().values().cloned().map(Ok))?;

        assert_eq!(0, value_log.segment_count());

        value_log.clear(false)?;
        assert_eq!(0, value_log.segment_count());
    }

    Ok(())
}
