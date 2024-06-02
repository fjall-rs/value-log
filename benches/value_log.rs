use criterion::{criterion_group, criterion_main, Criterion};
use rand::RngCore;
use std::sync::Arc;
use value_log::{BlobCache, Config, ExternalIndex, MockIndex, ValueHandle, ValueLog};

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

        let folder = tempfile::tempdir().unwrap();
        let vl_path = folder.path();

        let value_log = ValueLog::open(
            vl_path,
            Config::default().blob_cache(Arc::new(BlobCache::with_capacity_bytes(0))),
        )
        .unwrap();

        let mut writer = value_log.get_writer().unwrap();
        let segment_id = writer.segment_id();

        let mut rng = rand::thread_rng();

        for size in sizes {
            let key = size.to_string();
            let offset = writer.offset(key.as_bytes());

            index
                .insert_indirection(key.as_bytes(), ValueHandle { offset, segment_id })
                .unwrap();

            let mut data = vec![0u8; size];
            rng.fill_bytes(&mut data);

            writer.write(key.as_bytes(), &data).unwrap();
        }

        value_log.register(writer).unwrap();

        for size in sizes {
            let key = size.to_string();
            let handle = index.get(key.as_bytes()).unwrap().unwrap();

            group.bench_function(format!("{size} bytes (uncached)"), |b| {
                b.iter(|| {
                    value_log.get(&handle).unwrap().unwrap();
                })
            });
        }
    }

    {
        let index = MockIndex::default();

        let folder = tempfile::tempdir().unwrap();
        let vl_path = folder.path();

        let value_log = ValueLog::open(
            vl_path,
            Config::default()
                .blob_cache(Arc::new(BlobCache::with_capacity_bytes(64 * 1_024 * 1_024))),
        )
        .unwrap();

        let mut writer = value_log.get_writer().unwrap();
        let segment_id = writer.segment_id();

        let mut rng = rand::thread_rng();

        for size in sizes {
            let key = size.to_string();
            let offset = writer.offset(key.as_bytes());

            index
                .insert_indirection(key.as_bytes(), ValueHandle { offset, segment_id })
                .unwrap();

            let mut data = vec![0u8; size];
            rng.fill_bytes(&mut data);

            writer.write(key.as_bytes(), &data).unwrap();
        }

        value_log.register(writer).unwrap();

        for size in sizes {
            let key = size.to_string();
            let handle = index.get(key.as_bytes()).unwrap().unwrap();

            // NOTE: Warm up cache
            value_log.get(&handle).unwrap().unwrap();

            group.bench_function(format!("{size} bytes (cached)"), |b| {
                b.iter(|| {
                    value_log.get(&handle).unwrap().unwrap();
                })
            });
        }
    }
}

fn compression(c: &mut Criterion) {
    let mut group = c.benchmark_group("compression");

    let index = MockIndex::default();

    let folder = tempfile::tempdir().unwrap();
    let vl_path = folder.path();

    let value_log = ValueLog::open(
        vl_path,
        Config::default().blob_cache(Arc::new(BlobCache::with_capacity_bytes(0))),
    )
    .unwrap();

    let mut writer = value_log.get_writer().unwrap();
    let segment_id = writer.segment_id();

    let mut rng = rand::thread_rng();

    let size_mb = 16;

    {
        let key = "random";
        let offset = writer.offset(key.as_bytes());

        index
            .insert_indirection(key.as_bytes(), ValueHandle { offset, segment_id })
            .unwrap();

        let mut data = vec![0u8; size_mb * 1_024 * 1_024];
        rng.fill_bytes(&mut data);

        writer.write(key.as_bytes(), &data).unwrap();
    }

    {
        let key = "good_compression";
        let offset = writer.offset(key.as_bytes());

        index
            .insert_indirection(key.as_bytes(), ValueHandle { offset, segment_id })
            .unwrap();

        let dummy = b"abcdefgh";
        let data = dummy.repeat(size_mb * 1_024 * 1_024 / dummy.len());

        writer.write(key.as_bytes(), &data).unwrap();
    }

    value_log.register(writer).unwrap();

    let handle_random = index.get(b"random").unwrap().unwrap();
    let handle_good_compression = index.get(b"good_compression").unwrap().unwrap();

    group.bench_function("no compression", |b| {
        b.iter(|| {
            value_log.get(&handle_random).unwrap().unwrap();
        })
    });

    group.bench_function("good compression", |b| {
        b.iter(|| {
            value_log.get(&handle_good_compression).unwrap().unwrap();
        })
    });
}

criterion_group!(benches, load_value, compression);
criterion_main!(benches);
