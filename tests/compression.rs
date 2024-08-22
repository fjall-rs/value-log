use test_log::test;
use value_log::{
    Compressor, Config, IndexReader, IndexWriter, MockIndex, MockIndexWriter, ValueLog,
};

#[derive(Clone, Default)]
struct Lz4Compressor;
impl Compressor for Lz4Compressor {
    fn compress(&self, bytes: &[u8]) -> value_log::Result<Vec<u8>> {
        Ok(lz4_flex::compress_prepend_size(bytes))
    }

    fn decompress(&self, bytes: &[u8]) -> value_log::Result<Vec<u8>> {
        lz4_flex::decompress_size_prepended(bytes).map_err(|_| value_log::Error::Decompress)
    }
}

#[test]
fn compression() -> value_log::Result<()> {
    let folder = tempfile::tempdir()?;
    let vl_path = folder.path();

    let index = MockIndex::default();

    let value_log = ValueLog::open(vl_path, Config::<Lz4Compressor>::default())?;

    let mut index_writer = MockIndexWriter(index.clone());
    let mut writer = value_log.get_writer()?;

    let key = "abc";
    let value = "verycompressable".repeat(10);

    {
        let vhandle = writer.get_next_value_handle();
        index_writer.insert_indirect(key.as_bytes(), vhandle.clone(), value.len() as u32)?;

        let written_bytes = writer.write(key, &value)?;
        assert!(written_bytes < value.len() as u32);

        value_log.register_writer(writer)?;

        assert_eq!(
            &*value_log.get(&vhandle)?.expect("value should exist"),
            value.as_bytes(),
        );
    }

    {
        let index_writer = MockIndexWriter(index.clone());
        value_log.major_compact(&index, index_writer)?;

        let vhandle = index.get(key.as_bytes())?.unwrap();

        assert_eq!(
            &*value_log.get(&vhandle)?.expect("value should exist"),
            value.as_bytes(),
        );
    }

    Ok(())
}
