use std::{
    collections::BTreeMap,
    path::Path,
    sync::{Arc, RwLock},
};
use value_log::{Config, Index, IndexWriter, ValueHandle, ValueLog};

#[derive(Default)]
pub struct DebugIndex(RwLock<BTreeMap<Arc<[u8]>, ValueHandle>>);

impl Index for DebugIndex {
    fn get(&self, key: &[u8]) -> std::io::Result<Option<ValueHandle>> {
        Ok(self.0.read().expect("lock is poisoned").get(key).cloned())
    }

    fn insert_indirection(&self, key: &[u8], value: ValueHandle) -> std::io::Result<()> {
        self.0
            .write()
            .expect("lock is poisoned")
            .insert(key.into(), value);

        Ok(())
    }
}

struct DebugIndexWriter(Arc<DebugIndex>);

impl IndexWriter for DebugIndexWriter {
    fn insert_indirection(&self, key: &[u8], value: ValueHandle) -> std::io::Result<()> {
        self.0.insert_indirection(key, value)
    }

    fn finish(&self) -> std::io::Result<()> {
        Ok(())
    }
}

fn main() -> value_log::Result<()> {
    let index = DebugIndex(RwLock::new(BTreeMap::<Arc<[u8]>, ValueHandle>::default()));
    let index = Arc::new(index);

    let vl_path = Path::new("test_data");

    if vl_path.try_exists()? {
        std::fs::remove_dir_all(vl_path)?;
    }
    std::fs::create_dir_all(vl_path)?;

    let value_log = ValueLog::new(vl_path, Config::default(), index.clone())?;

    {
        let mut writer = value_log.get_writer()?;
        let segment_id = writer.segment_id();

        for key in ["a", "b", "c", "d", "e"] {
            let offset = writer.offset(key.as_bytes());

            index.insert_indirection(
                key.as_bytes(),
                ValueHandle {
                    offset,
                    segment_id: segment_id.clone(),
                },
            )?;

            writer.write(key.as_bytes(), key.repeat(10).as_bytes())?;
        }

        value_log.register(writer)?;
    }

    {
        let mut writer = value_log.get_writer()?;
        let segment_id = writer.segment_id();

        let key = "html";
        let offset = writer.offset(key.as_bytes());

        index.insert_indirection(
            key.as_bytes(),
            ValueHandle {
                offset,
                segment_id: segment_id.clone(),
            },
        )?;

        writer.write(
            key.as_bytes(),
            b"
        <html>
            <div data-hk=\"0-0-0-0-0-0-0-0-0-0-0-0-0-0-0\">Hello <i>World</i></div>
            <div data-hk=\"0-0-0-0-0-0-0-0-0-0-0-0-0-0-1\">Hello <i>World</i></div>
            <div data-hk=\"0-0-0-0-0-0-0-0-0-0-0-0-0-0-2\">Hello <i>World</i></div>
            <div data-hk=\"0-0-0-0-0-0-0-0-0-0-0-0-0-0-3\">Hello <i>World</i></div>
            <div data-hk=\"0-0-0-0-0-0-0-0-0-0-0-0-0-0-4\">Hello <i>World</i></div>
            <div data-hk=\"0-0-0-0-0-0-0-0-0-0-0-0-0-0-5\">Hello <i>World</i></div>
            <div data-hk=\"0-0-0-0-0-0-0-0-0-0-0-0-0-0-6\">Hello <i>World</i></div>
            <div data-hk=\"0-0-0-0-0-0-0-0-0-0-0-0-0-0-7\">Hello <i>World</i></div>
            <div data-hk=\"0-0-0-0-0-0-0-0-0-0-0-0-0-0-8\">Hello <i>World</i></div>
            <div data-hk=\"0-0-0-0-0-0-0-0-0-0-0-0-0-0-9\">Hello <i>World</i></div>
            <div data-hk=\"0-0-0-0-0-0-0-0-0-0-0-0-0-0-10\">Hello <i>World</i></div>
            <div data-hk=\"0-0-0-0-0-0-0-0-0-0-0-0-0-0-11\">Hello <i>World</i></div>
            <div data-hk=\"0-0-0-0-0-0-0-0-0-0-0-0-0-0-12\">Hello <i>World</i></div>
            <div data-hk=\"0-0-0-0-0-0-0-0-0-0-0-0-0-0-13\">Hello <i>World</i></div>
            <div data-hk=\"0-0-0-0-0-0-0-0-0-0-0-0-0-0-14\">Hello <i>World</i></div>
            <div data-hk=\"0-0-0-0-0-0-0-0-0-0-0-0-0-0-15\">Hello <i>World</i></div>
            <div data-hk=\"0-0-0-0-0-0-0-0-0-0-0-0-0-0-16\">Hello <i>World</i></div>
            <div data-hk=\"0-0-0-0-0-0-0-0-0-0-0-0-0-0-17\">Hello <i>World</i></div>
            <div data-hk=\"0-0-0-0-0-0-0-0-0-0-0-0-0-0-18\">Hello <i>World</i></div>
        </html>",
        )?;

        value_log.register(writer)?;
    }

    /* {
        let mut writer = value_log.get_writer()?;
        let segment_id = writer.segment_id();

        for key in ["e", "f", "g"] {
            let offset = writer.offset(key.as_bytes());

            index.insert_indirection(
                key.as_bytes(),
                ValueHandle {
                    offset,
                    segment_id: segment_id.clone(),
                },
            )?;

            writer.write(key.as_bytes(), key.repeat(20).as_bytes())?;
        }

        value_log.register(writer)?;
    }

    {
        let mut writer = value_log.get_writer()?;
        let segment_id = writer.segment_id();

        for key in ["a", "h"] {
            let offset = writer.offset(key.as_bytes());

            index.insert_indirection(
                key.as_bytes(),
                ValueHandle {
                    offset,
                    segment_id: segment_id.clone(),
                },
            )?;

            writer.write(key.as_bytes(), key.repeat(30).as_bytes())?;
        }

        value_log.register(writer)?;
    }

    {
        let mut writer = value_log.get_writer()?;
        let segment_id = writer.segment_id();

        for key in ["e", "i", "j"] {
            let offset = writer.offset(key.as_bytes());

            index.insert_indirection(
                key.as_bytes(),
                ValueHandle {
                    offset,
                    segment_id: segment_id.clone(),
                },
            )?;

            writer.write(key.as_bytes(), key.repeat(40).as_bytes())?;
        }

        value_log.register(writer)?;
    }

    eprintln!("{:#?}", value_log.segments.read().unwrap());

    for (key, handle) in index.0.read().expect("lock is poisoned").iter() {
        eprintln!(
            "loading KV: {:?} -> {handle:?}",
            std::str::from_utf8(key).unwrap()
        );

        let val = value_log.get(handle)?.unwrap();

        eprintln!(
            "loaded KV: {:?}: <{} bytes>",
            std::str::from_utf8(key).unwrap(),
            val.len()
        );
    }

    index.remove("d".as_bytes())?;

    for segment_id in value_log.list_segments() {
        // Scan segment
        let reader = SegmentReader::new(
            vl_path.join("segments").join(&*segment_id).join("data"),
            segment_id.clone(),
        )?;

        let mut stale_values = 0;

        for item in reader {
            let (key, val) = item?;

            eprintln!(
                "scanned KV: {:?}: <{} bytes>",
                std::str::from_utf8(&key).unwrap(),
                val.len()
            );

            match value_log.index.get(&key)? {
                Some(item) => {
                    // NOTE: Segment IDs are monotonically increasing
                    if item.segment_id > segment_id {
                        eprintln!(
                            "{} is ELIGIBLE FOR GC (fresher => segment:{})",
                            std::str::from_utf8(&key).unwrap(),
                            item.segment_id,
                        );
                        stale_values += 1;
                    }
                }
                None => {
                    eprintln!(
                        "{} is ELIGIBLE FOR GC (deleted)",
                        std::str::from_utf8(&key).unwrap()
                    );
                    stale_values += 1;
                }
            }
        }

        value_log.set_stale_items(&segment_id, stale_values);
    }

    eprintln!("=== rollover ===");
    value_log.rollover(&value_log.list_segments(), DebugIndexWriter(index.clone()))?; */

    eprintln!("{:#?}", value_log.segments.read().unwrap());

    let handle = index.get(b"html")?.unwrap();
    eprintln!(
        "{}",
        String::from_utf8_lossy(&value_log.get(&handle)?.unwrap())
    );

    for _ in 0..10 {
        let value_handle = ValueHandle {
            segment_id: value_log.list_segments().first().unwrap().clone(),
            offset: 3,
        };

        let before = std::time::Instant::now();
        value_log.get(&value_handle)?;
        eprintln!("blob loaded in {:?}ns", before.elapsed().as_nanos());
    }

    Ok(())
}
