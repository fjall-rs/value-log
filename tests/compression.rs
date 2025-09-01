mod common;

use common::{MockIndex, MockIndexWriter, NoCacher};
use test_log::test;
use value_log::{Compressor, Config, IndexReader, IndexWriter, ValueLog};

#[derive(Clone, Debug, Default)]
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

    let value_log = ValueLog::open(
        vl_path,
        Config::<_, Lz4Compressor>::new(NoCacher, NoCacher).compression(Some(Lz4Compressor)),
    )?;

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
        let segments = value_log.manifest.list_segments();
        let segment = segments.first().unwrap();
        assert_eq!(segment.meta.compressed_bytes, 32);
        assert_eq!(segment.meta.total_uncompressed_bytes, value.len() as u64);
    }

    {
        let index_writer = MockIndexWriter(index.clone());
        let bytes_saved = value_log.major_compact(&index, index_writer)?;
        value_log.drop_stale_segments()?;

        assert_eq!(0, bytes_saved);

        let vhandle = index.get(key.as_bytes())?.unwrap();

        assert_eq!(
            &*value_log.get(&vhandle)?.expect("value should exist"),
            value.as_bytes(),
        );
    }

    {
        let segments = value_log.manifest.list_segments();
        let segment = segments.first().unwrap();
        assert_eq!(segment.meta.compressed_bytes, 32);
        assert_eq!(segment.meta.total_uncompressed_bytes, value.len() as u64);
    }

    Ok(())
}
