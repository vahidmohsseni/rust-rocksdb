use crate::entry::Entry;

pub struct MemTable {
    entities: Vec<Entry>,
    size: usize,
}

impl MemTable {
    pub fn new() -> MemTable {
        MemTable {
            entities: Vec::new(),
            size: 0,
        }
    }

    pub fn init_from_file(entities: Vec<Entry>) -> MemTable {
        let size = 0;
        let mut mem_table = MemTable { entities, size };
        mem_table.restore_size();
        mem_table
    }

    fn restore_size(&mut self) {
        for entry in &self.entities {
            match entry.value.as_ref() {
                Some(val) => {
                    self.size += entry.key.len() + val.len() + 16 + 1;
                }
                None => {
                    self.size += entry.key.len() + 16 + 1;
                }
            }
        }
    }

    pub fn get_index(&self, key: &[u8]) -> Result<usize, usize> {
        self.entities
            .binary_search_by_key(&key, |entry| entry.key.as_slice())
    }

    pub fn set_or_insert(&mut self, key: &[u8], value: &[u8], timestamp: u128) {
        let entry = Entry {
            key: key.to_owned(),
            value: Some(value.to_owned()),
            timestamp,
            deleted: false,
        };

        match self.get_index(key) {
            // Update the value if the key exists already
            Ok(idx) => {
                if let Some(old_value) = self.entities[idx].value.as_ref() {
                    // Update the size of the MemTable
                    self.size += value.len();
                    self.size -= old_value.len();
                } else {
                    self.size += value.len();
                }
                self.entities[idx] = entry;
            }
            Err(idx) => {
                // key size + value size + 16 + 1 -> 16 is the size of u128
                self.size += key.len() + value.len() + 16 + 1;
                self.entities.insert(idx, entry);
            }
        }
    }

    pub fn delete(&mut self, key: &[u8], timestamp: u128) {
        let entry = Entry {
            key: key.to_owned(),
            value: None,
            timestamp,
            deleted: true,
        };

        match self.get_index(&key) {
            Ok(idx) => {
                if let Some(old_value) = self.entities[idx].value.as_ref() {
                    self.size -= old_value.len();
                }
                self.entities[idx] = entry;
            }
            Err(idx) => {
                self.size += key.len() + 16 + 1;
                self.entities.insert(idx, entry);
            }
        }
    }

    pub fn get(&self, key: &[u8]) -> Option<&Entry> {
        if let Ok(idx) = self.get_index(key) {
            return Some(&self.entities[idx]);
        }
        None
    }

    pub fn get_all(&self) -> &Vec<Entry> {
        &self.entities
    }
}

#[cfg(test)]
mod test {
    use rand::Rng;

    use crate::{
        storage::Storage,
        storage_iterator::StorageIterator,
        utils::{create_dir, remove_dir, scan_dir},
    };

    use super::*;
    use std::{path::PathBuf, time::SystemTime};

    #[test]
    fn check_single_add() {
        let mut mem_table = MemTable::new();

        let key = b"Hello".to_owned();
        let value = *b"World!";
        let timestamp = SystemTime::now().elapsed().unwrap().as_micros();
        mem_table.set_or_insert(&key, &value, timestamp);

        assert_eq!(mem_table.get_index(&key).unwrap(), 0);
        assert_eq!(
            mem_table.get(&key).unwrap().to_owned().value,
            Some(value.to_vec())
        );
    }

    #[test]
    fn check_size() {
        let mut mem_table = MemTable::new();

        let key = b"Hello".to_owned();
        let value = *b"World!";
        let timestamp = SystemTime::now().elapsed().unwrap().as_micros();
        mem_table.set_or_insert(&key, &value, timestamp);

        assert_eq!(mem_table.size, (5 + 6 + 16 + 1));
    }

    #[test]
    fn check_size_after_delete() {
        let mut mem_table = MemTable::new();

        let key = b"Hello".to_owned();
        let value = *b"World!";
        let mut timestamp = SystemTime::now().elapsed().unwrap().as_micros();
        mem_table.set_or_insert(&key, &value, timestamp);

        timestamp = SystemTime::now().elapsed().unwrap().as_micros();
        mem_table.delete(&key, timestamp);

        assert_eq!(mem_table.size, (5 + 0 + 16 + 1));
    }

    #[test]
    fn check_deleted_item() {
        let mut mem_table = MemTable::new();

        let key = b"Hello".to_owned();
        let value = *b"World!";
        let mut timestamp = SystemTime::now().elapsed().unwrap().as_micros();
        mem_table.set_or_insert(&key, &value, timestamp);

        timestamp = SystemTime::now().elapsed().unwrap().as_micros();
        mem_table.delete(&key, timestamp);

        assert_eq!(mem_table.get(&key).unwrap().deleted, true);
    }

    #[test]
    fn check_multiple_insert() {
        let mut mem_table = MemTable::new();

        let key1 = b"Hello".to_owned();
        let value1 = *b"World!";
        let mut timestamp = SystemTime::now().elapsed().unwrap().as_micros();
        mem_table.set_or_insert(&key1, &value1, timestamp);

        let key2 = b"MyName".to_owned();
        let value2 = *b"Vahid";
        timestamp = SystemTime::now().elapsed().unwrap().as_micros();
        mem_table.set_or_insert(&key2, &value2, timestamp);

        assert_eq!(mem_table.get_index(&key2).unwrap(), 1 as usize);

        timestamp = SystemTime::now().elapsed().unwrap().as_micros();
        mem_table.delete(&key2, timestamp);

        assert_eq!(mem_table.size, (5 + 6 + 16 + 1 + 6 + 1 + 16));
    }

    #[test]
    fn test_init_from_file() {
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

        let key3 = b"gg".to_owned();
        let value3 = *b"wp";
        let timestamp3 = SystemTime::now().elapsed().unwrap().as_micros();
        storage
            .set(&key3, &value3, false, timestamp3)
            .expect("Error: could not write in the file");

        let key4 = b"Name".to_owned();
        let timestamp4 = SystemTime::now().elapsed().unwrap().as_micros();
        storage
            .delete(&key4, timestamp4)
            .expect("Error: could not complete delete operation");

        storage.commit().expect("Error: could not flush the file");

        drop(storage);

        let files = scan_dir(&path).expect("Error: could not scan the directory");

        let storage_iterator = StorageIterator::new(&files[0]).unwrap();

        let data: Vec<Entry> = storage_iterator.collect();

        let mem_table = MemTable::init_from_file(data);

        assert_eq!(96, mem_table.size);

        // Clean up
        remove_dir(&path).unwrap();
    }
}
