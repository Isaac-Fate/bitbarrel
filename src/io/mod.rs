mod file_io;
pub use file_io::FileIO;

use crate::Result;

/// Abstract IO Manager.
pub trait IOManager: Sync + Send {
    /// Reads data from the file at the given position and puts it in the buffer.
    fn read(&self, buf: &mut [u8], offest: u64) -> Result<usize>;

    /// Writes data in the buffer to file.
    fn write(&self, buf: &[u8]) -> Result<usize>;

    /// Syncs data to disk.
    fn sync(&self) -> Result<()>;
}
