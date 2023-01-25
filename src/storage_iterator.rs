use std::{
    fs::{File, OpenOptions},
    io::{self, BufReader, Read},
    path::PathBuf,
};

use crate::entry::Entry;

pub struct StorageIterator {
    reader: BufReader<File>,
}

impl StorageIterator {
    pub fn new(path: &PathBuf) -> io::Result<StorageIterator> {
        let file = OpenOptions::new().read(true).open(path)?;
        let reader = BufReader::new(file);
        Ok(StorageIterator { reader })
    }
}

// The data layout:
// +---------------+-------------------+-----------------+----------+------------+-----------------+
// | Key size (8B) | Deleted flag (1B) | Value size (8B) | key (?B) | value (?B) | timestamp (16B) |
// +---------------+-------------------+-----------------+----------+------------+-----------------+
//
impl Iterator for StorageIterator {
    type Item = Entry;

    fn next(&mut self) -> Option<Entry> {
        let mut buffer = [0; 17];
        if self.reader.read_exact(&mut buffer).is_err() {
            return None;
        }

        let key_size = usize::from_le_bytes(buffer[0..8].try_into().expect("required length of 8"));
        let deleted = buffer[8] != 0;
        let value_size =
            usize::from_le_bytes(buffer[9..17].try_into().expect("required length of 8"));

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

        Some(Entry {
            key,
            value,
            timestamp,
            deleted,
        })
    }
}

#[cfg(test)]
mod test {
    use std::time::SystemTime;

    use rand::Rng;

    use super::*;
    use crate::{
        storage::Storage,
        utils::{create_dir, remove_dir, scan_dir},
    };

    #[test]
    fn init_memory_from_file() {
        let mut range = rand::thread_rng();
        let path = PathBuf::from(format!("./test-{}-temp", range.gen::<u32>()));

        create_dir(&path).unwrap();

        let mut storage = Storage::new(&path).unwrap();

        let key1 = b"Hello".to_owned();
        let value1 = *b"World!";
        let timestamp1 = SystemTime::now().elapsed().unwrap().as_micros();
        storage
            .set(&key1, &value1, false, timestamp1)
            .expect("Error: could not write in the file");

        let key2 = b"Name".to_owned();
        let value2 = *b"Vahid";
        let timestamp2 = SystemTime::now().elapsed().unwrap().as_micros();
        storage
            .set(&key2, &value2, false, timestamp2)
            .expect("Error: could not write in the file");

        storage.commit().expect("Error: could not flush the file");

        drop(storage);

        let files = scan_dir(&path).expect("Error: could not scan the directory");

        let storage_iterator = StorageIterator::new(&files[0]).unwrap();

        let data: Vec<Entry> = storage_iterator.collect();

        assert_eq!(data[1].key, key2);

        // Clean up
        remove_dir(&path).unwrap();
    }

    #[test]
    #[should_panic]
    fn not_found() {
        let mut range = rand::thread_rng();
        let path = PathBuf::from(format!("./test-{}-temp", range.gen::<u32>()));
        let _storage_iter = StorageIterator::new(&path).unwrap();
    }
}
