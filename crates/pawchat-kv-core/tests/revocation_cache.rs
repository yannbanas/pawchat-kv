//! Integration tests for `RevocationCache`, including real `redb`
//! persistence round-trips through a simulated process restart (drop the
//! cache, reopen the same file).

use pawchat_kv::RevocationCache;
use std::sync::Arc;
use tempfile::tempdir;

#[tokio::test]
async fn in_memory_cache_basic_get_set() {
    let cache = RevocationCache::new_in_memory();
    assert_eq!(cache.get_cv(1).await, None);

    cache.set_cv(1, 7).await.unwrap();
    assert_eq!(cache.get_cv(1).await, Some(7));

    cache.set_cv(1, 8).await.unwrap();
    assert_eq!(cache.get_cv(1).await, Some(8));

    assert!(!cache.is_persistent());
}

#[tokio::test]
async fn persisted_cache_survives_simulated_restart() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("revocation.redb");

    {
        let cache = RevocationCache::open(&path).unwrap();
        cache.set_cv(100, 1).await.unwrap();
        cache.set_cv(200, 5).await.unwrap();
        cache.set_cv(100, 2).await.unwrap(); // overwrite
        assert!(cache.is_persistent());
        // `cache` dropped here: simulates the process exiting.
    }

    // Simulated restart: reopen the same file in a brand new instance.
    let reopened = RevocationCache::open(&path).unwrap();
    assert_eq!(reopened.get_cv(100).await, Some(2));
    assert_eq!(reopened.get_cv(200).await, Some(5));
    assert_eq!(reopened.get_cv(999).await, None);
    assert_eq!(reopened.len(), 2);
}

#[tokio::test]
async fn opening_the_same_path_twice_sequentially_is_idempotent() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("revocation.redb");

    {
        let cache = RevocationCache::open(&path).unwrap();
        cache.set_cv(1, 1).await.unwrap();
    }
    {
        let cache = RevocationCache::open(&path).unwrap();
        assert_eq!(cache.get_cv(1).await, Some(1));
    }
    // A third open should not fail or duplicate anything.
    let cache = RevocationCache::open(&path).unwrap();
    assert_eq!(cache.len(), 1);
}

#[tokio::test]
async fn invalidate_removes_from_memory_and_disk() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("revocation.redb");

    {
        let cache = RevocationCache::open(&path).unwrap();
        cache.set_cv(1, 1).await.unwrap();
        cache.invalidate(1).await.unwrap();
        assert_eq!(cache.get_cv(1).await, None);
    }

    let reopened = RevocationCache::open(&path).unwrap();
    assert_eq!(reopened.get_cv(1).await, None);
    assert_eq!(reopened.len(), 0);
}

#[tokio::test]
async fn eventful_invalidation_via_set_cv_overwrites_immediately() {
    // "Invalidation événementielle": a credential_version bump must be
    // visible to readers right away, not eventually.
    let cache = Arc::new(RevocationCache::new_in_memory());
    cache.set_cv(1, 1).await.unwrap();
    assert_eq!(cache.get_cv(1).await, Some(1));

    cache.set_cv(1, 2).await.unwrap();
    assert_eq!(
        cache.get_cv(1).await,
        Some(2),
        "credential_version bump must be visible immediately, not just eventually"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_writes_then_reads_are_consistent() {
    // Many tasks race to set different users' versions concurrently, then
    // every value must be exactly what was last written for that user.
    let cache = Arc::new(RevocationCache::new_in_memory());
    let users = 200u64;

    let mut handles = Vec::with_capacity(users as usize);
    for user_id in 0..users {
        let cache = Arc::clone(&cache);
        handles.push(tokio::spawn(async move {
            for v in 1..=5u32 {
                cache.set_cv(user_id, v).await.unwrap();
            }
        }));
    }
    for h in handles {
        h.await.unwrap();
    }

    for user_id in 0..users {
        assert_eq!(cache.get_cv(user_id).await, Some(5));
    }
    assert_eq!(cache.len(), users as usize);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_writes_to_persisted_cache_are_all_durable() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("revocation.redb");
    let cache = Arc::new(RevocationCache::open(&path).unwrap());

    let users = 50u64;
    let mut handles = Vec::with_capacity(users as usize);
    for user_id in 0..users {
        let cache = Arc::clone(&cache);
        handles.push(tokio::spawn(async move {
            cache
                .set_cv(user_id, (user_id % 10) as u32 + 1)
                .await
                .unwrap();
        }));
    }
    for h in handles {
        h.await.unwrap();
    }
    drop(cache);

    let reopened = RevocationCache::open(&path).unwrap();
    assert_eq!(reopened.len(), users as usize);
    for user_id in 0..users {
        assert_eq!(
            reopened.get_cv(user_id).await,
            Some((user_id % 10) as u32 + 1)
        );
    }
}

#[tokio::test]
async fn metrics_track_hits_and_misses() {
    let cache = RevocationCache::new_in_memory();
    cache.set_cv(1, 1).await.unwrap();

    assert_eq!(cache.get_cv(1).await, Some(1)); // hit
    assert_eq!(cache.get_cv(2).await, None); // miss

    let snap = cache.metrics();
    assert_eq!(snap.hits, 1);
    assert_eq!(snap.misses, 1);
    assert_eq!(snap.writes, 1);
}
