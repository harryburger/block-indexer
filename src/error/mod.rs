use std::fmt;

#[derive(Debug)]
pub enum TokenSyncError {
    DatabaseError(DatabaseError),
    NetworkError(String),
    ConfigError(String),
    SerializationError(String),
    ValidationError(String),
}

#[derive(Debug)]
pub enum DatabaseError {
    ConnectionFailed(String),
    QueryFailed(String),
    InsertFailed(String),
    UpdateFailed(String),
    SerializationFailed(String),
    NotFound(String),
    DuplicateKey(String),
    Timeout(String),
}

#[derive(Debug)]
pub enum InjectorError {
    ConfigError(String),
    ConnectionError(String),
    NetworkError(String),
    UnexpectedError(String),
}

impl fmt::Display for DatabaseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DatabaseError::ConnectionFailed(msg) => {
                write!(f, "Database connection failed: {msg}")
            }
            DatabaseError::QueryFailed(msg) => write!(f, "Database query failed: {msg}"),
            DatabaseError::InsertFailed(msg) => write!(f, "Database insert failed: {msg}"),
            DatabaseError::UpdateFailed(msg) => write!(f, "Database update failed: {msg}"),
            DatabaseError::SerializationFailed(msg) => {
                write!(f, "Database serialization failed: {msg}")
            }
            DatabaseError::NotFound(msg) => write!(f, "Database record not found: {msg}"),
            DatabaseError::DuplicateKey(msg) => write!(f, "Database duplicate key: {msg}"),
            DatabaseError::Timeout(msg) => write!(f, "Database timeout: {msg}"),
        }
    }
}

impl fmt::Display for TokenSyncError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TokenSyncError::DatabaseError(db_err) => write!(f, "{db_err}"),
            TokenSyncError::NetworkError(msg) => write!(f, "Network error: {msg}"),
            TokenSyncError::ConfigError(msg) => write!(f, "Configuration error: {msg}"),
            TokenSyncError::SerializationError(msg) => write!(f, "Serialization error: {msg}"),
            TokenSyncError::ValidationError(msg) => write!(f, "Validation error: {msg}"),
        }
    }
}

impl fmt::Display for InjectorError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            InjectorError::ConfigError(msg) => write!(f, "Configuration error: {msg}"),
            InjectorError::ConnectionError(msg) => write!(f, "Connection error: {msg}"),
            InjectorError::NetworkError(msg) => write!(f, "Network error: {msg}"),
            InjectorError::UnexpectedError(msg) => write!(f, "Unexpected error: {msg}"),
        }
    }
}

impl std::error::Error for DatabaseError {}
impl std::error::Error for TokenSyncError {}
impl std::error::Error for InjectorError {}

// Convert from MongoDB errors
impl From<mongodb::error::Error> for DatabaseError {
    fn from(err: mongodb::error::Error) -> Self {
        match err.kind.as_ref() {
            mongodb::error::ErrorKind::Write(_write_err) => {
                // Check error message for duplicate key instead of code field
                if err.to_string().contains("duplicate key") || err.to_string().contains("E11000") {
                    DatabaseError::DuplicateKey(err.to_string())
                } else {
                    DatabaseError::InsertFailed(err.to_string())
                }
            }
            mongodb::error::ErrorKind::Command(_) => DatabaseError::QueryFailed(err.to_string()),
            mongodb::error::ErrorKind::Authentication { .. } => {
                DatabaseError::ConnectionFailed(err.to_string())
            }
            mongodb::error::ErrorKind::Io(_) => DatabaseError::ConnectionFailed(err.to_string()),
            _ => DatabaseError::QueryFailed(err.to_string()),
        }
    }
}

// Convert from BSON errors
impl From<bson::ser::Error> for DatabaseError {
    fn from(err: bson::ser::Error) -> Self {
        DatabaseError::SerializationFailed(err.to_string())
    }
}

impl From<bson::de::Error> for DatabaseError {
    fn from(err: bson::de::Error) -> Self {
        DatabaseError::SerializationFailed(err.to_string())
    }
}

// Convert from serde_json errors
impl From<serde_json::Error> for TokenSyncError {
    fn from(err: serde_json::Error) -> Self {
        TokenSyncError::SerializationError(err.to_string())
    }
}

// Convert DatabaseError to TokenSyncError
impl From<DatabaseError> for TokenSyncError {
    fn from(err: DatabaseError) -> Self {
        TokenSyncError::DatabaseError(err)
    }
}

// Convert from anyhow errors
impl From<anyhow::Error> for DatabaseError {
    fn from(err: anyhow::Error) -> Self {
        DatabaseError::QueryFailed(err.to_string())
    }
}

// Add direct conversion from bson::ser::Error to TokenSyncError
impl From<bson::ser::Error> for TokenSyncError {
    fn from(err: bson::ser::Error) -> Self {
        TokenSyncError::SerializationError(err.to_string())
    }
}

impl From<bson::de::Error> for TokenSyncError {
    fn from(err: bson::de::Error) -> Self {
        TokenSyncError::SerializationError(err.to_string())
    }
}

// Convert MongoDB errors to TokenSyncError
impl From<mongodb::error::Error> for TokenSyncError {
    fn from(err: mongodb::error::Error) -> Self {
        TokenSyncError::DatabaseError(DatabaseError::from(err))
    }
}
