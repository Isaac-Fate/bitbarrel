use std::{
    fs::{ File, OpenOptions },
    io::Write,
    os::unix::prelude::FileExt,
    path::Path,
    sync::Arc,
};
use parking_lot::RwLock;
use crate::{ Result, Error };
use super::IOManager;

pub struct FileIO {
    /// File descriptor.
    fd: Arc<RwLock<File>>,
}

impl FileIO {
    pub fn new<P: AsRef<Path>>(file_path: P) -> Result<Self> {
        // Open file
        match OpenOptions::new().create(true).read(true).write(true).append(true).open(file_path) {
            Ok(file) => {
                // Return new instance of the IO manager
                Ok(Self {
                    fd: Arc::new(RwLock::new(file)),
                })
            }
            Err(error) => {
                // Log
                log::error!("Failed to open data file: {}", error);

                // Return error
                Err(Error::FileOpen)
            }
        }
    }
}

impl IOManager for FileIO {
    fn read(&self, buf: &mut [u8], offest: u64) -> Result<usize> {
        // Aquire read lock
        let read_guard = self.fd.read();

        // Read data from file
        match read_guard.read_at(buf, offest) {
            Ok(size) => Ok(size),
            Err(error) => {
                // Log
                log::error!("Failed to read from data file: {}", error);

                // Return error
                Err(Error::FileRead)
            }
        }
    }

    fn write(&self, buf: &[u8]) -> crate::Result<usize> {
        // Aquire write lock
        let mut write_guard = self.fd.write();

        // Write data to file
        match write_guard.write(buf) {
            Ok(size) => Ok(size),
            Err(error) => {
                // Log
                log::error!("Failed to write to data file: {}", error);

                // Return error
                Err(Error::FileWrite)
            }
        }
    }

    fn sync(&self) -> Result<()> {
        // Aquire read lock
        let read_guard = self.fd.read();

        // Sync data to disk
        match read_guard.sync_all() {
            Ok(_) => Ok(()),
            Err(error) => {
                // Log
                log::error!("Failed to sync data to disk: {}", error);

                // Return error
                Err(Error::FileSync)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_write() {
        // Create a temporary directory
        let tmp_dir = tempfile::tempdir().unwrap();
        let tmp_file_path = tmp_dir.path().join("test.txt");

        // Create new instance of FileIO
        let file_io = FileIO::new(&tmp_file_path);
        assert!(file_io.is_ok());
        println!("Temporary file created at: {:?}", tmp_file_path);
        let file_io = file_io.unwrap();

        // Write to file and get number of bytes written
        let num_bytes = file_io.write("Hello, World!".as_bytes());
        assert!(num_bytes.is_ok());
        let num_bytes = num_bytes.unwrap();
        assert_eq!(num_bytes, 13);
    }

    #[test]
    fn test_read() {
        // Create a temporary directory
        let tmp_dir = tempfile::tempdir().unwrap();
        let tmp_file_path = tmp_dir.path().join("test.txt");

        // Create new instance of FileIO
        let file_io = FileIO::new(&tmp_file_path);
        assert!(file_io.is_ok());
        println!("Temporary file created at: {:?}", tmp_file_path);
        let file_io = file_io.unwrap();

        // Write to file and get number of bytes written
        let num_bytes = file_io.write("Hello, World!".as_bytes());
        assert!(num_bytes.is_ok());
        let num_bytes = num_bytes.unwrap();
        assert_eq!(num_bytes, 13);

        // Create a large enough buffer holding the bytes to read
        let mut buf = [0u8; 100];

        // Read from file at given offset and get number of bytes read
        let num_bytes = file_io.read(&mut buf, 3);
        assert!(num_bytes.is_ok());
        let num_bytes = num_bytes.unwrap();
        assert_eq!(num_bytes, 10);
        assert_eq!(&buf[..num_bytes], "lo, World!".as_bytes());
        println!("Read bytes as a string: {}", String::from_utf8_lossy(&buf[..num_bytes]));

        // Read from file at given offset and get number of bytes read
        let num_bytes = file_io.read(&mut buf, 7);
        assert!(num_bytes.is_ok());
        let num_bytes = num_bytes.unwrap();
        assert_eq!(num_bytes, 6);
        assert_eq!(&buf[..num_bytes], "World!".as_bytes());
        println!("Read bytes as a string: {}", String::from_utf8_lossy(&buf[..num_bytes]));

        // Write more data at the end of the file
        let num_bytes = file_io.write(" Greetings!".as_bytes());
        assert!(num_bytes.is_ok());
        let num_bytes = num_bytes.unwrap();
        assert_eq!(num_bytes, 11);

        // Read from file at given offset and get number of bytes read
        let num_bytes = file_io.read(&mut buf, 14);
        assert!(num_bytes.is_ok());
        let num_bytes = num_bytes.unwrap();
        assert_eq!(num_bytes, 10);
        assert_eq!(&buf[..num_bytes], "Greetings!".as_bytes());
        println!("Read bytes as a string: {}", String::from_utf8_lossy(&buf[..num_bytes]));

        // Read from file at given offset and get number of bytes read
        let num_bytes = file_io.read(&mut buf, 12);
        assert!(num_bytes.is_ok());
        let num_bytes = num_bytes.unwrap();
        assert_eq!(num_bytes, 12);
        assert_eq!(&buf[..num_bytes], "! Greetings!".as_bytes());
        println!("Read bytes as a string: {}", String::from_utf8_lossy(&buf[..num_bytes]));
    }
}
