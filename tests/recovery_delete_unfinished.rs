use test_log::test;
use value_log::{Compressor, Config, IndexWriter, MockIndex, MockIndexWriter, ValueLog};

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
fn recovery_delete_unfinished() -> value_log::Result<()> {
    let folder = tempfile::tempdir()?;
    let vl_path = folder.path();

    let index = MockIndex::default();

    let items = ["a", "b", "c", "d", "e"];

    {
        let value_log = ValueLog::open(vl_path, Config::<NoCompressor>::default())?;

        {
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
    }

    let faux_segment = vl_path.join("segments").join("5");
    {
        std::fs::File::create(&faux_segment)?;
    }

    {
        let value_log = ValueLog::open(vl_path, Config::<NoCompressor>::default())?;
        assert_eq!(1, value_log.segment_count());
    }

    assert!(!faux_segment.try_exists()?);

    Ok(())
}
