use std::fmt;
use yrs::error::UpdateError;

#[derive(Debug)]
pub enum StoreError {
    UpdateError(UpdateError),
    YrsError(yrs::error::Error),
    WriteError(String),
    SqlxError(sqlx::Error),
}

impl fmt::Display for StoreError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            StoreError::UpdateError(e) => e.fmt(f),
            StoreError::SqlxError(e) => e.fmt(f),
            StoreError::YrsError(e) => e.fmt(f),
            StoreError::WriteError(msg) => write!(f, "Write error: {}", msg),
        }
    }
}

impl std::error::Error for StoreError {}
