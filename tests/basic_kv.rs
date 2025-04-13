mod common;

use common::{InMemCacher, MockIndex, MockIndexWriter, NoCacher, NoCompressor};
use test_log::test;
use value_log::{Config, IndexWriter, KeyRange, Slice, ValueLog};

#[test]
fn basic_kv() -> value_log::Result<()> {
    let folder = tempfile::tempdir()?;
    let vl_path = folder.path();

    let index = MockIndex::default();

    let items = ["a", "b", "c", "d", "e"];

    {
        let value_log = ValueLog::open(
            vl_path,
            Config::<_, _, NoCompressor>::new(NoCacher, NoCacher),
        )?;

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

    {
        let value_log = ValueLog::open(
            vl_path,
            Config::<_, _, NoCompressor>::new(NoCacher, NoCacher),
        )?;

        assert_eq!(1, value_log.segment_count());

        let segments = value_log.manifest.list_segments();

        let segment = segments.first().unwrap();

        assert_eq!(items.len() as u64, segment.len());
        assert_eq!(0, segment.gc_stats.stale_items());

        assert_eq!(
            segment.meta.key_range,
            KeyRange::new((Slice::from(*b"a"), Slice::from(*b"e")))
        );

        assert_eq!(
            segment.len(),
            segment.scan().into_iter().flatten().count() as u64
        );

        for (key, (vhandle, _)) in index.read().unwrap().iter() {
            let item = value_log.get(vhandle)?.unwrap();
            assert_eq!(&*item, &*key.repeat(10_000));
        }
    }

    Ok(())
}

#[test]
fn get_with_cached_fd() -> value_log::Result<()> {
    let folder = tempfile::tempdir()?;
    let vl_path = folder.path();
    let items = ["a", "b", "c", "d", "e"];
    let index = MockIndex::default();

    let fd_cache = InMemCacher::default();
    let value_log = ValueLog::open(
        vl_path,
        Config::<_, _, NoCompressor>::new(NoCacher, fd_cache.clone()),
    )?;

    let mut index_writer = MockIndexWriter(index.clone());
    let mut writer = value_log.get_writer()?;

    for key in items {
        let value = key.repeat(10_000);
        let value = value.as_bytes();

        let key = key.as_bytes();

        let vhandle = writer.get_next_value_handle();
        index_writer.insert_indirect(key, vhandle, value.len() as u32)?;

        writer.write(key, value)?;
    }

    value_log.register_writer(writer)?;

    for (key, (vhandle, _)) in index.read().unwrap().iter() {
        let item = value_log.get(vhandle)?.unwrap();
        assert_eq!(item, key.repeat(10_000));
    }

    let index_len = index.read().unwrap().len() as u32;
    // The first item will cache the fd, subsequent accesses will all be cache hits
    assert_eq!(fd_cache.get_miss_count(), 1);
    assert_eq!(fd_cache.get_hit_count(), index_len - 1);

    Ok(())
}
