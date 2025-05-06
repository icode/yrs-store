use std::error::Error;
use std::fmt;
use yrs::error::UpdateError;

#[derive(Debug)]
pub enum StoreError {
    UpdateError(UpdateError),
    YrsError(yrs::error::Error),
    WriteError(String),
    Error(Box<dyn Error + Send + Sync + 'static>),
    StorageError(Box<dyn Error + Send + Sync + 'static>),
}

impl fmt::Display for StoreError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            StoreError::UpdateError(e) => e.fmt(f),
            StoreError::Error(e) => e.fmt(f),
            StoreError::StorageError(e) => e.fmt(f),
            StoreError::YrsError(e) => e.fmt(f),
            StoreError::WriteError(msg) => write!(f, "Write error: {}", msg),
        }
    }
}

impl Error for StoreError {}
