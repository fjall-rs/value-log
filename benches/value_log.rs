use criterion::{criterion_group, criterion_main, Criterion};
use rand::{Rng, RngCore};
use std::sync::Arc;
use value_log::{
    BlobCache, Compressor, Config, IndexReader, IndexWriter, MockIndex, MockIndexWriter, UserValue,
    ValueHandle, ValueLog, ValueLogId,
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

fn prefetch(c: &mut Criterion) {
    let mut group = c.benchmark_group("prefetch range");

    let range_size = 10;
    let item_size = 1_024;

    let index = MockIndex::default();
    let mut index_writer = MockIndexWriter(index.clone());

    let folder = tempfile::tempdir().unwrap();
    let vl_path = folder.path();

    let value_log = ValueLog::open(vl_path, Config::<_, NoCompressor>::new(NoCacher)).unwrap();

    let mut writer = value_log.get_writer().unwrap();

    let mut rng = rand::thread_rng();

    for key in (0u64..2_000_000).map(u64::to_be_bytes) {
        let mut data = vec![0u8; item_size];
        rng.fill_bytes(&mut data);

        index_writer
            .insert_indirect(&key, writer.get_next_value_handle(), data.len() as u32)
            .unwrap();

        writer.write(key, &data).unwrap();

        data.clear();
    }

    value_log.register_writer(writer).unwrap();

    let mut rng = rand::thread_rng();

    group.bench_function(format!("{range_size}x{item_size}B - no prefetch"), |b| {
        b.iter(|| {
            let start = rng.gen_range(0u64..1_999_000);

            for x in start..(start + range_size) {
                let vhandle = index.get(&x.to_be_bytes()).unwrap().unwrap();
                let value = value_log.get(&vhandle).unwrap().unwrap();
                assert_eq!(item_size, value.len());
            }
        })
    });

    group.bench_function(format!("{range_size}x{item_size}B - with prefetch"), |b| {
        b.iter(|| {
            let start = rng.gen_range(0u64..1_999_000);

            {
                let vhandle = index.get(&start.to_be_bytes()).unwrap().unwrap();

                let value = value_log
                    .get_with_prefetch(&vhandle, (range_size - 1) as usize)
                    .unwrap()
                    .unwrap();

                assert_eq!(item_size, value.len());
            }

            for x in (start..(start + range_size)).skip(1) {
                let vhandle = index.get(&x.to_be_bytes()).unwrap().unwrap();
                let value = value_log.get(&vhandle).unwrap().unwrap();
                assert_eq!(item_size, value.len());
            }
        })
    });
}

fn load_value(c: &mut Criterion) {
    let mut group = c.benchmark_group("load blob");

    let sizes = [
        128,               // 128 B
        512,               // 512 B
        1_024,             // 1 KiB
        4_096,             // 4 KiB
        16_000,            // 16 KiB
        64_000,            // 64 KiB
        128_000,           // 128 KiB
        256_000,           // 256 KiB
        512_000,           // 512 KiB
        1_024 * 1_024,     // 1 MiB
        4 * 1_024 * 1_024, // 4 MiB
    ];

    {
        let index = MockIndex::default();
        let mut index_writer = MockIndexWriter(index.clone());

        let folder = tempfile::tempdir().unwrap();
        let vl_path = folder.path();

        let value_log = ValueLog::open(vl_path, Config::<_, NoCompressor>::new(NoCacher)).unwrap();

        let mut writer = value_log.get_writer().unwrap();

        let mut rng = rand::thread_rng();

        for size in sizes {
            let key = size.to_string();

            let mut data = vec![0u8; size];
            rng.fill_bytes(&mut data);

            index_writer
                .insert_indirect(
                    key.as_bytes(),
                    writer.get_next_value_handle(),
                    data.len() as u32,
                )
                .unwrap();

            writer.write(key.as_bytes(), &data).unwrap();
        }

        value_log.register_writer(writer).unwrap();

        for size in sizes {
            let key = size.to_string();
            let vhandle = index.get(key.as_bytes()).unwrap().unwrap();

            group.bench_function(format!("{size} bytes"), |b| {
                b.iter(|| {
                    value_log.get(&vhandle).unwrap().unwrap();
                })
            });
        }
    }
}

criterion_group!(benches, load_value, prefetch);
criterion_main!(benches);
