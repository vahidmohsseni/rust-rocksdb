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
                    self.size += value.len() - old_value.len()
                } else {
                    self.size += value.len()
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
}

#[cfg(test)]
mod test {
    use super::*;
    use std::time::SystemTime;

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
}
