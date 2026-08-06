//! `pawchat-kv`: an embedded, in-process rate limiter and
//! `credential_version` revocation cache for PawChat.
//!
//! This is **not** a general-purpose key-value store and is not meant to
//! grow into one — it implements exactly the two needs identified in
//! `docs/kv-store-research-pawchat-design.md` (§6) as a from-scratch
//! alternative to running Redis/Dragonfly/KeyDB/Garnet/Kvrocks for a
//! workload those engines are all oversized for:
//!
//! - [`RateLimiter`]: sliding-window counters, in-memory only, never
//!   persisted.
//! - [`RevocationCache`]: one `credential_version` integer per user,
//!   warm-loadable from a local `redb` file after a restart. The
//!   application database remains the source of truth.
//!
//! See `pawchat-kv/README.md` for the full design rationale and an
//! explicit list of what is intentionally out of scope (no RESP/Memcached
//! protocol, no clustering, no VR/metaverse ephemeral state, no
//! RocksDB-style LSM engine).

mod error;
mod metrics;
mod rate_limiter;
mod revocation;
mod table;

pub use error::KvError;
pub use metrics::MetricsSnapshot;
pub use rate_limiter::RateLimiter;
pub use revocation::RevocationCache;
