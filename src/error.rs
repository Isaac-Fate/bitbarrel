/// Result type of this crate.
pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, thiserror::Error)]
/// Error type of this crate.
pub enum Error {
    #[error("failed to open data file")]
    FileOpen,

    #[error("failed to read from data file")]
    FileRead,

    #[error("failed to write to data file")]
    FileWrite,

    #[error("failed to sync data to disk")]
    FileSync,

    #[error("key cannot be empty")]
    EmptyKey,

    #[error("failed to update index")]
    IndexUpdate,
}
