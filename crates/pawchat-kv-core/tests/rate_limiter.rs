//! Integration tests for `RateLimiter`.

use pawchat_kv::RateLimiter;
use std::sync::Arc;
use std::time::Duration;

#[tokio::test]
async fn allows_up_to_the_limit_then_denies() {
    let limiter = RateLimiter::without_purge_task();
    let window = Duration::from_secs(60);

    for i in 0..5 {
        assert!(
            limiter.incr_and_check("k", 5, window).await,
            "attempt {i} should be allowed"
        );
    }
    // 6th attempt within the same window must be denied.
    assert!(!limiter.incr_and_check("k", 5, window).await);
    assert!(!limiter.incr_and_check("k", 5, window).await);
}

#[tokio::test]
async fn different_keys_have_independent_limits() {
    let limiter = RateLimiter::without_purge_task();
    let window = Duration::from_secs(60);

    for _ in 0..3 {
        assert!(limiter.incr_and_check("a", 3, window).await);
        assert!(limiter.incr_and_check("b", 3, window).await);
    }
    assert!(!limiter.incr_and_check("a", 3, window).await);
    assert!(!limiter.incr_and_check("b", 3, window).await);
}

#[tokio::test(start_paused = true)]
async fn window_rollover_admits_new_attempts_after_expiry() {
    let limiter = RateLimiter::without_purge_task();
    let window = Duration::from_millis(100);

    for _ in 0..3 {
        assert!(limiter.incr_and_check("k", 3, window).await);
    }
    assert!(!limiter.incr_and_check("k", 3, window).await);

    // Advance time past the window: the old hits should have aged out and
    // fresh attempts should be admitted again.
    tokio::time::advance(Duration::from_millis(150)).await;

    assert!(limiter.incr_and_check("k", 3, window).await);
    assert!(limiter.incr_and_check("k", 3, window).await);
    assert!(limiter.incr_and_check("k", 3, window).await);
    assert!(!limiter.incr_and_check("k", 3, window).await);
}

#[tokio::test(start_paused = true)]
async fn sliding_window_is_exact_at_the_boundary_not_a_fixed_bucket() {
    // Regression test for the classic fixed-window bug: with a fixed
    // (non-sliding) window, all N hits placed anywhere inside one bucket
    // expire together at the bucket boundary, which can let 2N requests
    // through in a short span straddling two buckets. An exact sliding
    // window must only free up exactly as many slots as have individually
    // aged past `window`, one at a time.
    let limiter = RateLimiter::without_purge_task();
    let window = Duration::from_millis(100);
    let limit = 4;

    // Four hits spaced 20ms apart: t=0, 20, 40, 60.
    for _ in 0..limit {
        assert!(limiter.incr_and_check("k", limit, window).await);
        tokio::time::advance(Duration::from_millis(20)).await;
    }
    // Clock is now at t=80. All 4 hits are still within the last 100ms.
    assert!(!limiter.incr_and_check("k", limit, window).await);

    // Advance to t=110: the t=0 hit is now 110ms old (> window) and should
    // have aged out, while t=20/40/60 (90/70/50ms old) have not. Exactly
    // one slot should have freed up.
    tokio::time::advance(Duration::from_millis(30)).await;
    assert!(
        limiter.incr_and_check("k", limit, window).await,
        "one slot should have freed up"
    );
    assert!(
        !limiter.incr_and_check("k", limit, window).await,
        "no further slot should be free"
    );
}

#[tokio::test]
async fn concurrent_increments_never_exceed_the_limit() {
    let limiter = Arc::new(RateLimiter::without_purge_task());
    let window = Duration::from_secs(60);
    let limit: u32 = 50;
    let attempts = 500;

    let mut handles = Vec::with_capacity(attempts);
    for _ in 0..attempts {
        let limiter = Arc::clone(&limiter);
        handles.push(tokio::spawn(async move {
            limiter.incr_and_check("hot-key", limit, window).await
        }));
    }

    let mut allowed = 0u32;
    for h in handles {
        if h.await.unwrap() {
            allowed += 1;
        }
    }

    assert_eq!(
        allowed, limit,
        "exactly `limit` attempts should have been admitted under concurrency"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_increments_across_real_os_threads_never_exceed_the_limit() {
    let limiter = Arc::new(RateLimiter::without_purge_task());
    let window = Duration::from_secs(60);
    let limit: u32 = 20;
    let attempts = 400;

    let mut handles = Vec::with_capacity(attempts);
    for _ in 0..attempts {
        let limiter = Arc::clone(&limiter);
        handles.push(tokio::spawn(async move {
            limiter.incr_and_check("hot-key", limit, window).await
        }));
    }

    let mut allowed = 0u32;
    for h in handles {
        if h.await.unwrap() {
            allowed += 1;
        }
    }

    assert_eq!(allowed, limit);
}

#[tokio::test(start_paused = true)]
async fn background_purge_evicts_inactive_keys() {
    let limiter = RateLimiter::with_purge_interval(Duration::from_millis(20));
    let window = Duration::from_millis(50);

    assert!(limiter.incr_and_check("idle-key", 5, window).await);
    assert_eq!(limiter.len(), 1);

    // Let the key go idle past its window, and let the purge task tick a
    // few times.
    tokio::time::advance(Duration::from_millis(200)).await;
    // Yield so the spawned purge task actually gets to run under the
    // paused clock.
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_millis(20)).await;
    tokio::task::yield_now().await;

    assert_eq!(limiter.len(), 0, "idle key should have been purged");
}

#[tokio::test(start_paused = true)]
async fn manual_purge_evicts_inactive_keys_deterministically() {
    let limiter = RateLimiter::without_purge_task();
    let window = Duration::from_millis(50);

    assert!(limiter.incr_and_check("a", 5, window).await);
    assert!(limiter.incr_and_check("b", 5, window).await);
    assert_eq!(limiter.len(), 2);

    tokio::time::advance(Duration::from_millis(60)).await;
    let removed = limiter.purge_expired();

    assert_eq!(removed, 2);
    assert_eq!(limiter.len(), 0);
}

#[tokio::test]
async fn metrics_track_hits_and_misses() {
    let limiter = RateLimiter::without_purge_task();
    let window = Duration::from_secs(60);

    assert!(limiter.incr_and_check("k", 1, window).await);
    assert!(!limiter.incr_and_check("k", 1, window).await);

    let snap = limiter.metrics();
    assert_eq!(snap.hits, 1);
    assert_eq!(snap.misses, 1);
    assert_eq!(snap.len, 1);
}
