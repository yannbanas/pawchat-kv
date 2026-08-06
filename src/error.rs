use std::fmt;

/// Errors returned by `pawchat-kv` operations.
///
/// The library only surfaces fallible errors for operations that touch the
/// `redb` persistence layer used by [`crate::RevocationCache`]. Purely
/// in-memory operations (the [`crate::RateLimiter`] and in-memory reads of
/// the revocation cache) never fail.
#[derive(Debug)]
pub enum KvError {
    /// Failed to open or create the underlying `redb` database file.
    Database(Box<redb::DatabaseError>),
    /// Failed to start a `redb` transaction.
    Transaction(Box<redb::TransactionError>),
    /// Failed to open a table within a `redb` transaction.
    Table(Box<redb::TableError>),
    /// Failed to commit a `redb` write transaction.
    Commit(Box<redb::CommitError>),
    /// Failed to read or write a value from/to a `redb` table.
    Storage(Box<redb::StorageError>),
    /// Failed to encode or decode a stored value.
    Serialization(String),
}

impl fmt::Display for KvError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            KvError::Database(e) => write!(f, "redb database error: {e}"),
            KvError::Transaction(e) => write!(f, "redb transaction error: {e}"),
            KvError::Table(e) => write!(f, "redb table error: {e}"),
            KvError::Commit(e) => write!(f, "redb commit error: {e}"),
            KvError::Storage(e) => write!(f, "redb storage error: {e}"),
            KvError::Serialization(msg) => write!(f, "serialization error: {msg}"),
        }
    }
}

impl std::error::Error for KvError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            KvError::Database(e) => Some(e),
            KvError::Transaction(e) => Some(e),
            KvError::Table(e) => Some(e),
            KvError::Commit(e) => Some(e),
            KvError::Storage(e) => Some(e),
            KvError::Serialization(_) => None,
        }
    }
}

impl From<redb::DatabaseError> for KvError {
    fn from(e: redb::DatabaseError) -> Self {
        KvError::Database(Box::new(e))
    }
}

impl From<redb::TransactionError> for KvError {
    fn from(e: redb::TransactionError) -> Self {
        KvError::Transaction(Box::new(e))
    }
}

impl From<redb::TableError> for KvError {
    fn from(e: redb::TableError) -> Self {
        KvError::Table(Box::new(e))
    }
}

impl From<redb::CommitError> for KvError {
    fn from(e: redb::CommitError) -> Self {
        KvError::Commit(Box::new(e))
    }
}

impl From<redb::StorageError> for KvError {
    fn from(e: redb::StorageError) -> Self {
        KvError::Storage(Box::new(e))
    }
}
