//! Sliding-window rate limiter.
//!
//! Counters are held only in memory and are never persisted: losing them on
//! restart is not a security concern, at worst a rate-limit window resets a
//! few seconds early (see `docs/kv-store-research-pawchat-design.md`, §6.2).

use std::collections::VecDeque;
use std::time::Duration;
use tokio::time::Instant;

use crate::metrics::MetricsSnapshot;
use crate::table::ShardedTtlMap;

/// Default interval at which the background task scans for and evicts
/// inactive keys (keys with no hit within their own window).
const DEFAULT_PURGE_INTERVAL: Duration = Duration::from_secs(30);

/// Per-key sliding-window state: exact timestamps of hits still inside the
/// current window.
///
/// An exact sliding-window *log* (rather than a fixed-window counter or an
/// approximated weighted sliding window) was chosen deliberately: it never
/// over-admits at a window boundary, which is the classic correctness bug
/// of the naive `INCR` + `EXPIRE` pattern this is replacing. The memory
/// cost is `O(limit)` timestamps per active key, which is negligible for
/// the limits PawChat actually uses (tens to low hundreds per window).
struct SlidingWindow {
    hits: VecDeque<Instant>,
}

/// A concurrent, in-memory, sliding-window rate limiter.
///
/// Keys are arbitrary strings — callers decide the keying scheme (e.g.
/// `"login:{ip}"`, `"api:{user_id}"`). Each key tracks its own window
/// independently; `limit` and `window` are passed per call so the same
/// limiter instance can serve routes with different policies.
///
/// # Example
///
/// ```
/// use pawchat_kv::RateLimiter;
/// use std::time::Duration;
///
/// # #[tokio::main]
/// # async fn main() {
/// let limiter = RateLimiter::new();
///
/// for _ in 0..5 {
///     assert!(limiter.incr_and_check("login:127.0.0.1", 5, Duration::from_secs(60)).await);
/// }
/// // The 6th attempt within the same 60s window is denied.
/// assert!(!limiter.incr_and_check("login:127.0.0.1", 5, Duration::from_secs(60)).await);
/// # }
/// ```
pub struct RateLimiter {
    table: ShardedTtlMap<String, SlidingWindow>,
}

impl RateLimiter {
    /// Creates a rate limiter with the default purge interval (30s).
    ///
    /// Must be called from within a running Tokio runtime for the
    /// background purge task to start; otherwise a warning is logged and
    /// purging falls back to manual (see [`RateLimiter::purge_expired`]).
    pub fn new() -> Self {
        Self::with_purge_interval(DEFAULT_PURGE_INTERVAL)
    }

    /// Creates a rate limiter whose background purge task runs every
    /// `interval`. A shorter interval reclaims memory from abandoned keys
    /// (e.g. one-off IPs that never come back) faster, at the cost of more
    /// frequent full-table scans.
    pub fn with_purge_interval(interval: Duration) -> Self {
        Self { table: ShardedTtlMap::new("rate_limiter", Some(interval)) }
    }

    /// Creates a rate limiter with no background purge task. Inactive keys
    /// only get reclaimed when [`RateLimiter::purge_expired`] is called
    /// explicitly. Useful outside a Tokio runtime (e.g. `criterion`
    /// benchmarks) or in tests that want deterministic purge timing.
    pub fn without_purge_task() -> Self {
        Self { table: ShardedTtlMap::new("rate_limiter", None) }
    }

    /// Records one attempt against `key` and reports whether it is allowed
    /// under a `limit`-per-`window` sliding window.
    ///
    /// Returns `true` and counts the attempt if fewer than `limit` attempts
    /// occurred in the trailing `window` duration; returns `false` (without
    /// counting it) otherwise. Safe to call concurrently for the same key
    /// from multiple tasks/threads — the read-check-write sequence happens
    /// atomically under that key's shard lock, so concurrent callers can
    /// never jointly exceed `limit`.
    #[tracing::instrument(level = "trace", skip(self))]
    pub async fn incr_and_check(&self, key: &str, limit: u32, window: Duration) -> bool {
        let now = Instant::now();
        let cutoff = now.checked_sub(window);

        let allowed = self.table.with_entry(
            key.to_string(),
            || SlidingWindow { hits: VecDeque::new() },
            |entry| {
                let hits = &mut entry.value.hits;
                while let Some(&front) = hits.front() {
                    let stale = match cutoff {
                        Some(c) => front <= c,
                        None => false,
                    };
                    if stale {
                        hits.pop_front();
                    } else {
                        break;
                    }
                }

                let allowed = (hits.len() as u64) < u64::from(limit);
                if allowed {
                    hits.push_back(now);
                }
                // A key with no further hits for a full `window` is
                // considered inactive and eligible for the background
                // purge, regardless of whether this particular attempt
                // was allowed or denied.
                entry.expires_at = Some(now + window);
                allowed
            },
        );

        if allowed {
            self.table.record_hit();
        } else {
            self.table.record_miss();
        }
        tracing::trace!(key, limit, allowed, "rate limit check");
        allowed
    }

    /// Forces an immediate synchronous purge of inactive keys, returning
    /// the number removed. Normally unnecessary (the background task does
    /// this on a timer) — provided for tests and for callers running
    /// without a Tokio runtime.
    pub fn purge_expired(&self) -> usize {
        self.table.purge_expired()
    }

    /// Number of keys currently tracked (including any not yet purged that
    /// have gone fully idle).
    pub fn len(&self) -> usize {
        self.table.len()
    }

    /// Whether no keys are currently tracked.
    pub fn is_empty(&self) -> bool {
        self.table.len() == 0
    }

    /// Current hit/miss/write/purge counters for this limiter.
    pub fn metrics(&self) -> MetricsSnapshot {
        self.table.metrics()
    }
}

impl Default for RateLimiter {
    fn default() -> Self {
        Self::new()
    }
}
