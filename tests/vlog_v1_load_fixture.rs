use test_log::test;
use value_log::{Compressor, Config, ValueLog};

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
fn vlog_load_v1() -> value_log::Result<()> {
    let path = std::path::Path::new("test_fixture/v1_vlog");

    let value_log = ValueLog::open(path, Config::<NoCompressor>::default())?;

    let count = {
        let mut count = 0;

        for kv in value_log.get_reader()? {
            let _ = kv?;
            count += 1;
        }

        count
    };

    assert_eq!(4, count);
    assert_eq!(2, value_log.segment_count());
    assert_eq!(0, value_log.verify()?);

    Ok(())
}

#[test]
fn vlog_load_v1_corrupt() -> value_log::Result<()> {
    let path = std::path::Path::new("test_fixture/v1_vlog_corrupt");

    let value_log = ValueLog::open(path, Config::<NoCompressor>::default())?;

    assert_eq!(2, value_log.verify()?);

    Ok(())
}
