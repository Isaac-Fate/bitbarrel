use bytes::Bytes;

/// Data record type.
/// It is named log record because
/// each data will be appended to the end of a data file,
/// just like a log record.
pub struct LogRecord {
    pub(crate) key: Bytes,
    pub(crate) value: Bytes,
    pub(crate) status: LogRecordStatus,
}

impl LogRecord {
    pub fn encode(&self) -> Bytes {
        todo!()
    }
}

pub enum LogRecordStatus {
    /// Normal status.
    /// The record exists in the index.
    Normal,

    /// Indicates that the record has been deleted.
    /// The record should be cleaned up later.
    Deleted,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct LogRecordPos {
    pub(crate) file_id: u32,
    pub(crate) offest: u64,
}
