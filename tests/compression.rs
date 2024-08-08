use std::sync::Arc;
use test_log::test;
use value_log::{Compressor, Config, IndexWriter, MockIndex, MockIndexWriter, ValueLog};

struct Lz4Compressor;
impl Compressor for Lz4Compressor {
    fn compress(&self, bytes: &[u8]) -> Result<Vec<u8>, value_log::CompressError> {
        Ok(lz4_flex::compress_prepend_size(bytes))
    }

    fn decompress(&self, bytes: &[u8]) -> Result<Vec<u8>, value_log::DecompressError> {
        lz4_flex::decompress_size_prepended(bytes)
            .map_err(|e| value_log::DecompressError(e.to_string()))
    }
}

#[test]
fn compression() -> value_log::Result<()> {
    let folder = tempfile::tempdir()?;
    let vl_path = folder.path();

    let index = MockIndex::default();

    let value_log = ValueLog::open(
        vl_path,
        Config::default().use_compression(Arc::new(Lz4Compressor)),
    )?;

    let mut index_writer = MockIndexWriter(index.clone());
    let mut writer = value_log.get_writer()?;

    let key = "abc";
    let value = "verycompressable".repeat(1_000);

    let handle = writer.get_next_value_handle();
    index_writer.insert_indirect(key.as_bytes(), handle.clone(), value.len() as u32)?;

    let written_bytes = writer.write(key, &value)?;
    assert!(written_bytes < value.len() as u32);

    value_log.register_writer(writer)?;

    assert_eq!(
        &*value_log.get(&handle)?.expect("value should exist"),
        value.as_bytes(),
    );

    Ok(())
}
