use std::sync::atomic::{AtomicU64, Ordering};

/// Point-in-time counters for a table.
///
/// Cheap to compute (a handful of atomic loads) so it is safe to call
/// `snapshot()` on a hot path (e.g. an admin/metrics endpoint scraped by
/// Prometheus) without contending with readers/writers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MetricsSnapshot {
    /// Number of lookups that found a live (non-expired) entry.
    pub hits: u64,
    /// Number of lookups that found no entry, or a stale one.
    pub misses: u64,
    /// Number of write operations (insert/update) performed.
    pub writes: u64,
    /// Number of entries removed by the periodic purge task.
    pub purged: u64,
    /// Current number of entries held in the table.
    pub len: usize,
}

/// Atomic hit/miss/write/purge counters, exposed to `tracing` on every
/// operation and readable at any time via [`Metrics::snapshot`].
///
/// This is intentionally a plain counter set rather than a full metrics
/// registry (e.g. `metrics`/`prometheus` crates) — wiring those up is left
/// to the embedding service (`pawchat-auth`), which already owns its
/// observability stack. `pawchat-kv` only guarantees the numbers are
/// tracked from day one so nothing needs to be retrofitted later.
#[derive(Debug, Default)]
pub struct Metrics {
    hits: AtomicU64,
    misses: AtomicU64,
    writes: AtomicU64,
    purged: AtomicU64,
}

impl Metrics {
    pub(crate) fn record_hit(&self) {
        self.hits.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_miss(&self) {
        self.misses.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_write(&self) {
        self.writes.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_purged(&self, count: u64) {
        self.purged.fetch_add(count, Ordering::Relaxed);
    }

    pub(crate) fn snapshot(&self, len: usize) -> MetricsSnapshot {
        MetricsSnapshot {
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            writes: self.writes.load(Ordering::Relaxed),
            purged: self.purged.load(Ordering::Relaxed),
            len,
        }
    }
}
