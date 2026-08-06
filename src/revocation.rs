//! `credential_version` revocation cache.
//!
//! Holds one small integer per user: the credential version currently
//! considered valid. `pawchat-auth` bumps it on password change, 2FA
//! disable, or ban, and rejects any token minted against an older version.
//!
//! The database (Postgres/SQLite, per `docs/auth-microservice-rust-plan.md`)
//! remains the source of truth. This cache is purely an accelerator for the
//! hot read path; `redb` persistence exists only so a process restart can
//! warm-load recent values instead of running cold and hitting the
//! database for every first request per user.

use std::path::Path;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::time::Instant;

use redb::{Database, ReadableTable, TableDefinition};
use serde::{Deserialize, Serialize};

use crate::error::KvError;
use crate::metrics::MetricsSnapshot;
use crate::table::ShardedTtlMap;

const TABLE: TableDefinition<u64, Vec<u8>> = TableDefinition::new("credential_versions");

/// On-disk record for one user's credential version.
///
/// Serialized with `bincode` and stored as the `redb` value. Kept as a
/// small struct rather than a bare `u32` so the persisted record carries a
/// timestamp of when it was last bumped — useful context to have on disk
/// even though the in-memory hot path only ever needs the version number.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct CvRecord {
    version: u32,
    updated_at_unix_ms: u64,
}

fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// A concurrent `credential_version` cache, warm-loadable from and
/// persisted to a local `redb` database file.
///
/// # Example
///
/// ```
/// use pawchat_kv::RevocationCache;
///
/// # #[tokio::main]
/// # async fn main() {
/// let cache = RevocationCache::new_in_memory();
/// assert_eq!(cache.get_cv(42).await, None);
///
/// cache.set_cv(42, 3).await.unwrap();
/// assert_eq!(cache.get_cv(42).await, Some(3));
/// # }
/// ```
pub struct RevocationCache {
    table: ShardedTtlMap<u64, u32>,
    db: Option<Arc<Database>>,
}

impl RevocationCache {
    /// Creates a cache with no backing file: everything is lost on drop.
    /// Useful for tests, or for an embedding process that is fine relying
    /// entirely on the database as source of truth after every restart.
    pub fn new_in_memory() -> Self {
        Self { table: ShardedTtlMap::new("revocation_cache", None), db: None }
    }

    /// Opens (creating if absent) a `redb` database file at `path` and
    /// warm-loads every stored `credential_version` into memory.
    ///
    /// # Errors
    ///
    /// Returns [`KvError`] if the file cannot be created/opened, or if the
    /// warm-load transaction fails (e.g. the file exists but is not a
    /// valid `redb` database).
    #[tracing::instrument(skip(path))]
    pub fn open(path: impl AsRef<Path>) -> Result<Self, KvError> {
        let db = Database::create(path.as_ref())?;

        // Ensure the table exists even on a brand new file: `open_table`
        // creates it implicitly inside a write transaction.
        {
            let write_txn = db.begin_write()?;
            {
                let _ = write_txn.open_table(TABLE)?;
            }
            write_txn.commit()?;
        }

        let table = ShardedTtlMap::new("revocation_cache", None);

        let mut loaded = 0u64;
        {
            let read_txn = db.begin_read()?;
            let redb_table = read_txn.open_table(TABLE)?;
            for row in redb_table.iter()? {
                let (key, value) = row?;
                let user_id = key.value();
                let record: CvRecord = bincode::deserialize(&value.value())
                    .map_err(|e| KvError::Serialization(e.to_string()))?;
                table.insert(user_id, record.version, None);
                loaded += 1;
            }
        }
        tracing::info!(loaded, path = %path.as_ref().display(), "warm-loaded revocation cache from disk");

        Ok(Self { table, db: Some(Arc::new(db)) })
    }

    /// Returns the cached credential version for `user_id`, if known.
    ///
    /// A `None` result means "not in cache" — callers should treat that as
    /// "fetch from the database and call `set_cv`", not as "version 0" or
    /// "no restriction". This never touches disk: it reads only the
    /// in-memory table (which is warm-loaded once at [`RevocationCache::open`]
    /// time and kept in sync by every [`RevocationCache::set_cv`] call).
    pub async fn get_cv(&self, user_id: u64) -> Option<u32> {
        self.table.get_cloned(&user_id, Instant::now())
    }

    /// Sets (or overwrites) the credential version for `user_id`.
    ///
    /// Updates the in-memory table immediately (so a `get_cv` issued right
    /// after this returns always observes the new value, even from another
    /// task/thread) and, if this cache was opened with [`RevocationCache::open`],
    /// durably persists it to `redb` before returning — the write is
    /// synchronous from the caller's point of view, run on a blocking task
    /// so it doesn't stall the async executor. This is a direct
    /// write-through per call, not a batched WAL; see the crate README for
    /// why that simplification is acceptable given `credential_version` is
    /// written rarely.
    ///
    /// # Errors
    ///
    /// Returns [`KvError`] if the `redb` write transaction fails. The
    /// in-memory table has already been updated at that point regardless —
    /// a persistence failure degrades durability (a crash right after would
    /// lose this update) but not current-process correctness.
    #[tracing::instrument(skip(self))]
    pub async fn set_cv(&self, user_id: u64, version: u32) -> Result<(), KvError> {
        self.table.insert(user_id, version, None);

        if let Some(db) = self.db.clone() {
            let record = CvRecord { version, updated_at_unix_ms: now_unix_ms() };
            tokio::task::spawn_blocking(move || persist(&db, user_id, &record))
                .await
                .map_err(|e| KvError::Serialization(format!("persist task panicked: {e}")))??;
        }

        tracing::debug!(user_id, version, "credential_version updated");
        Ok(())
    }

    /// Removes `user_id` from the cache entirely (both memory and, if
    /// persisted, disk). Intended for account deletion, not for routine
    /// credential rotation — a normal password/2FA/ban event should call
    /// [`RevocationCache::set_cv`] with the new version instead.
    pub async fn invalidate(&self, user_id: u64) -> Result<(), KvError> {
        self.table.remove(&user_id);

        if let Some(db) = self.db.clone() {
            tokio::task::spawn_blocking(move || remove(&db, user_id))
                .await
                .map_err(|e| KvError::Serialization(format!("invalidate task panicked: {e}")))??;
        }
        Ok(())
    }

    /// Number of users currently cached in memory.
    pub fn len(&self) -> usize {
        self.table.len()
    }

    /// Whether the cache is currently empty.
    pub fn is_empty(&self) -> bool {
        self.table.len() == 0
    }

    /// Whether this cache is backed by a `redb` file (`true` when opened
    /// via [`RevocationCache::open`], `false` for [`RevocationCache::new_in_memory`]).
    pub fn is_persistent(&self) -> bool {
        self.db.is_some()
    }

    /// Current hit/miss/write counters for this cache.
    pub fn metrics(&self) -> MetricsSnapshot {
        self.table.metrics()
    }
}

fn persist(db: &Database, user_id: u64, record: &CvRecord) -> Result<(), KvError> {
    let bytes = bincode::serialize(record).map_err(|e| KvError::Serialization(e.to_string()))?;
    let write_txn = db.begin_write()?;
    {
        let mut table = write_txn.open_table(TABLE)?;
        table.insert(user_id, bytes)?;
    }
    write_txn.commit()?;
    Ok(())
}

fn remove(db: &Database, user_id: u64) -> Result<(), KvError> {
    let write_txn = db.begin_write()?;
    {
        let mut table = write_txn.open_table(TABLE)?;
        table.remove(user_id)?;
    }
    write_txn.commit()?;
    Ok(())
}
