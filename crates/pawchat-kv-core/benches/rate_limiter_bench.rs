//! Baseline throughput/latency benchmark for `RateLimiter::incr_and_check`.
//!
//! This does not compare against a real Redis instance (the design
//! document is explicit that this is out of scope for a first pass) — it
//! only establishes a measurable baseline for this implementation so
//! future changes can be checked for regressions.
//!
//! Run with `cargo bench`.

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use pawchat_kv::RateLimiter;
use std::time::Duration;

fn bench_single_key_contended(c: &mut Criterion) {
    // Worst case: every call hits the same key, so every call serializes
    // on that key's shard lock.
    let rt = tokio::runtime::Runtime::new().unwrap();
    let limiter = RateLimiter::without_purge_task();

    c.bench_function("incr_and_check/single_key", |b| {
        b.to_async(&rt).iter(|| async {
            limiter
                .incr_and_check("bench:single", 1_000_000, Duration::from_secs(60))
                .await
        });
    });
}

fn bench_many_keys(c: &mut Criterion) {
    // Best case: distinct keys spread across shards, no contention between
    // iterations.
    let rt = tokio::runtime::Runtime::new().unwrap();
    let limiter = RateLimiter::without_purge_task();

    let mut group = c.benchmark_group("incr_and_check/many_keys");
    for key_space in [100usize, 10_000] {
        group.bench_with_input(
            BenchmarkId::from_parameter(key_space),
            &key_space,
            |b, &key_space| {
                let mut i: u64 = 0;
                b.to_async(&rt).iter(|| {
                    i = i.wrapping_add(1);
                    let key = format!("bench:{}", i as usize % key_space);
                    let limiter = &limiter;
                    async move {
                        limiter
                            .incr_and_check(&key, 1_000_000, Duration::from_secs(60))
                            .await
                    }
                });
            },
        );
    }
    group.finish();
}

fn bench_concurrent_same_key(c: &mut Criterion) {
    // Contended case with real parallelism: several tasks hammering the
    // same key concurrently on a multi-thread runtime.
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();
    let limiter = std::sync::Arc::new(RateLimiter::without_purge_task());

    c.bench_function("incr_and_check/concurrent_same_key_4tasks", |b| {
        b.to_async(&rt).iter(|| {
            let limiter = limiter.clone();
            async move {
                let mut handles = Vec::with_capacity(4);
                for _ in 0..4 {
                    let limiter = limiter.clone();
                    handles.push(tokio::spawn(async move {
                        limiter
                            .incr_and_check("bench:concurrent", 1_000_000, Duration::from_secs(60))
                            .await
                    }));
                }
                for h in handles {
                    let _ = h.await;
                }
            }
        });
    });
}

criterion_group!(
    benches,
    bench_single_key_contended,
    bench_many_keys,
    bench_concurrent_same_key
);
criterion_main!(benches);
