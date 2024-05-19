use std::{ path::Path, sync::Arc };
use parking_lot::RwLock;
use crate::{ Result, io::IOManager };

/// Data file structure.
pub struct DataFile {
    /// ID of the data file.
    file_id: Arc<RwLock<u32>>,

    /// Current write offset.
    write_offset: Arc<RwLock<u64>>,

    /// IO manager that handles data file operations.
    io_manager: Box<dyn IOManager>,
}

impl DataFile {
    /// Open or create a new data file under the given directory with the specified  file ID.
    pub fn new<P: AsRef<Path>>(dir: P, file_id: u32) -> Self {
        todo!()
    }
    /// Gets the file ID.
    pub fn file_id(&self) -> u32 {
        // Aquire read lock
        let read_guard = self.file_id.read();

        *read_guard
    }
    /// Gets the current write offset.
    pub fn write_offset(&self) -> u64 {
        // Aquire read lock
        let read_guard = self.write_offset.read();

        *read_guard
    }

    /// Writes data from the given buffer to the data file.
    pub fn write(&self, buf: &[u8]) -> Result<()> {
        todo!()
    }

    /// Syncs data file to disk.
    pub fn sync(&self) -> Result<()> {
        todo!()
    }
}
