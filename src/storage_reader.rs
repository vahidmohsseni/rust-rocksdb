use std::{io::{BufReader, self, Read}, fs::{File, OpenOptions}, path::PathBuf};


pub struct StorageEntry {
    key: Vec<u8>,
    value: Option<Vec<u8>>,
    timestamp: u128,
    deleted: bool
}

pub struct StorageReader {
    reader: BufReader<File>,
}

impl StorageReader {
    pub fn new(path: PathBuf) -> io::Result<StorageReader> {
        let file = OpenOptions::new()
            .read(true)
            .open(path)?;
        let reader = BufReader::new(file);
        Ok(StorageReader { reader })
    }
}

// The data layout:
// +---------------+-------------------+-----------------+----------+------------+-----------------+ 
// | Key size (8B) | Deleted flag (1B) | Value size (8B) | key (?B) | value (?B) | timestamp (16B) |
// +---------------+-------------------+-----------------+----------+------------+-----------------+ 
// 
impl Iterator for StorageReader {
    type Item = StorageEntry;

    fn next(&mut self) -> Option<StorageEntry> {
        let mut buffer = [0; 17];
        if self.reader.read_exact(&mut buffer).is_err() {
            return None;
        }

        let key_size = usize::from_le_bytes(buffer[0..8].try_into().expect("required length of 8"));
        let deleted = buffer[8] != 0;
        let value_size = usize::from_le_bytes(buffer[9..17].try_into().expect("required length of 8"));

        let mut key = vec![0; key_size];
        let mut value_buffer = vec![0; value_size];
        let mut value = None;

        if self.reader.read_exact(&mut key).is_err() {
            return None;
        }

        if !deleted {
            if self.reader.read_exact(&mut value_buffer).is_err() {
                return None;
            }
            value = Some(value_buffer);
        }

        let mut timestamp_buffer = [0; 16];
        if self.reader.read_exact(&mut timestamp_buffer).is_err() {
            return None;
        }

        let timestamp = u128::from_le_bytes(timestamp_buffer);

        Some(StorageEntry { 
            key,
            value,
            timestamp,
            deleted
        })

    }
}