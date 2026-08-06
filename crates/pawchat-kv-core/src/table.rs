//! The shared concurrent-table engine backing both [`crate::RateLimiter`]
//! and [`crate::RevocationCache`].
//!
//! Both structures need the same primitive: a concurrent map keyed by a
//! small hashable key, holding a small value, with an optional
//! expiration timestamp and an active (not just lazy-on-read) purge of
//! stale entries. Rather than share one heterogeneous `DashMap` between
//! two unrelated value types (which would need an `enum` or `Box<dyn Any>`
//! and lose type safety for no real benefit at this scale), each structure
//! owns its own instance of this same generic engine. That is what the
//! design document (`docs/kv-store-research-pawchat-design.md`, §6.2)
//! means by "même moteur, deux structures logiques": one implementation,
//! two independently-sized tables with independent eviction policies.

use dashmap::DashMap;
use std::hash::Hash;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;
use tokio::time::Instant;

use crate::metrics::{Metrics, MetricsSnapshot};

/// A stored value plus its optional expiry instant.
///
/// `expires_at = None` means "never expires on its own", which is the
/// steady-state case for `RevocationCache` entries: they live until an
/// explicit `set_cv` overwrites them, not until a clock runs out.
pub(crate) struct StoredEntry<V> {
    pub value: V,
    pub expires_at: Option<Instant>,
}

/// Generic sharded, TTL-aware concurrent map.
///
/// Backed by [`dashmap::DashMap`], which partitions its key space into
/// internal shards each guarded by its own `RwLock`, instead of one global
/// lock over a `HashMap`. See the crate README for why `DashMap` was
/// chosen over `moka` for this project.
pub(crate) struct ShardedTtlMap<K, V>
where
    K: Eq + Hash + Send + Sync + Clone + 'static,
    V: Send + Sync + 'static,
{
    name: &'static str,
    inner: Arc<DashMap<K, StoredEntry<V>>>,
    metrics: Arc<Metrics>,
    purge_task: Option<JoinHandle<()>>,
    stopped: Arc<AtomicBool>,
}

impl<K, V> ShardedTtlMap<K, V>
where
    K: Eq + Hash + Send + Sync + Clone + 'static,
    V: Send + Sync + 'static,
{
    /// Creates a new empty table.
    ///
    /// If `purge_interval` is `Some`, a background `tokio::spawn` task is
    /// started that periodically scans the table and evicts expired
    /// entries. Starting that task requires an active Tokio runtime; if
    /// none is running at construction time (e.g. inside a `criterion`
    /// benchmark or a plain `#[test]`), no task is spawned and a warning is
    /// logged via `tracing` — callers can still evict manually with
    /// [`ShardedTtlMap::purge_expired`].
    pub(crate) fn new(name: &'static str, purge_interval: Option<Duration>) -> Self {
        let inner: Arc<DashMap<K, StoredEntry<V>>> = Arc::new(DashMap::new());
        let metrics = Arc::new(Metrics::default());
        let stopped = Arc::new(AtomicBool::new(false));

        let purge_task =
            purge_interval.and_then(|interval| match tokio::runtime::Handle::try_current() {
                Ok(handle) => {
                    let map = Arc::clone(&inner);
                    let m = Arc::clone(&metrics);
                    let stop = Arc::clone(&stopped);
                    Some(handle.spawn(purge_loop(name, map, m, stop, interval)))
                }
                Err(_) => {
                    tracing::warn!(
                        table = name,
                        "no tokio runtime available at construction time; \
                         background purge task not started, call purge_expired() manually"
                    );
                    None
                }
            });

        Self {
            name,
            inner,
            metrics,
            purge_task,
            stopped,
        }
    }

    /// Atomically gets-or-inserts the entry for `key`, then applies `f` to
    /// it while holding that key's shard lock, returning `f`'s result.
    ///
    /// This is the primitive that makes `RateLimiter::incr_and_check`
    /// race-free: the read (current count), the TTL check, and the write
    /// (incrementing / resetting) all happen under a single shard lock, so
    /// two concurrent callers incrementing the same key can never both
    /// observe "one slot left" and both succeed.
    pub(crate) fn with_entry<F, R>(&self, key: K, default: impl FnOnce() -> V, f: F) -> R
    where
        F: FnOnce(&mut StoredEntry<V>) -> R,
    {
        let mut entry = self.inner.entry(key).or_insert_with(|| StoredEntry {
            value: default(),
            expires_at: None,
        });
        f(&mut entry)
    }

    /// Reads a clone of the live value for `key`, if present and not
    /// expired. Expired-but-not-yet-purged entries are treated as absent.
    pub(crate) fn get_cloned(&self, key: &K, now: Instant) -> Option<V>
    where
        V: Clone,
    {
        let hit = self.inner.get(key).and_then(|entry| {
            if entry.expires_at.is_none_or(|exp| exp > now) {
                Some(entry.value.clone())
            } else {
                None
            }
        });
        if hit.is_some() {
            self.metrics.record_hit();
        } else {
            self.metrics.record_miss();
        }
        hit
    }

    /// Inserts or overwrites `key` unconditionally.
    pub(crate) fn insert(&self, key: K, value: V, expires_at: Option<Instant>) {
        self.inner.insert(key, StoredEntry { value, expires_at });
        self.metrics.record_write();
    }

    /// Removes `key`, returning its value if it was present.
    pub(crate) fn remove(&self, key: &K) -> Option<V> {
        self.inner.remove(key).map(|(_, entry)| entry.value)
    }

    /// Number of entries currently stored (including any not yet purged
    /// expired entries — call [`ShardedTtlMap::purge_expired`] first for an
    /// exact "live" count).
    pub(crate) fn len(&self) -> usize {
        self.inner.len()
    }

    /// Synchronously scans the whole table and removes every entry whose
    /// `expires_at` is in the past. Returns the number of entries removed.
    ///
    /// This is what the background task calls on a timer; it is also
    /// exposed so tests (and callers without a running Tokio runtime) can
    /// trigger a deterministic purge instead of racing a timer.
    pub(crate) fn purge_expired(&self) -> usize {
        purge_once(self.name, &self.inner, &self.metrics)
    }

    /// Returns current hit/miss/write/purge counters plus table size.
    pub(crate) fn metrics(&self) -> MetricsSnapshot {
        self.metrics.snapshot(self.inner.len())
    }

    /// Records a logical hit (e.g. a rate-limit check that was allowed, or
    /// a cache lookup that found a value). Exposed so callers that mutate
    /// entries via [`ShardedTtlMap::with_entry`] — which does not itself
    /// know whether the outcome counts as a hit or a miss — can report it.
    pub(crate) fn record_hit(&self) {
        self.metrics.record_hit();
    }

    /// Records a logical miss (e.g. a rate-limit check that was denied, or
    /// a cache lookup that found nothing).
    pub(crate) fn record_miss(&self) {
        self.metrics.record_miss();
    }
}

impl<K, V> Drop for ShardedTtlMap<K, V>
where
    K: Eq + Hash + Send + Sync + Clone + 'static,
    V: Send + Sync + 'static,
{
    fn drop(&mut self) {
        self.stopped.store(true, Ordering::Relaxed);
        if let Some(task) = self.purge_task.take() {
            task.abort();
        }
    }
}

fn purge_once<K, V>(
    name: &'static str,
    map: &DashMap<K, StoredEntry<V>>,
    metrics: &Metrics,
) -> usize
where
    K: Eq + Hash + Clone,
{
    let now = Instant::now();
    let before = map.len();
    map.retain(|_, entry| entry.expires_at.is_none_or(|exp| exp > now));
    let removed = before.saturating_sub(map.len());
    if removed > 0 {
        metrics.record_purged(removed as u64);
        tracing::debug!(
            table = name,
            removed,
            remaining = map.len(),
            "purged expired entries"
        );
    }
    removed
}

async fn purge_loop<K, V>(
    name: &'static str,
    map: Arc<DashMap<K, StoredEntry<V>>>,
    metrics: Arc<Metrics>,
    stopped: Arc<AtomicBool>,
    interval: Duration,
) where
    K: Eq + Hash + Clone,
{
    let mut ticker = tokio::time::interval(interval);
    // The first tick fires immediately; skip it so we don't purge a
    // brand-new empty table right away.
    ticker.tick().await;
    loop {
        ticker.tick().await;
        if stopped.load(Ordering::Relaxed) {
            return;
        }
        purge_once(name, &map, &metrics);
    }
}
