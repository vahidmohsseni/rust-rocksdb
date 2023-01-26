use std::{
    io,
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};

use crate::{
    entry::Entry, memtable::MemTable, storage::Storage, storage_iterator::StorageIterator,
    utils::scan_dir,
};

pub struct Db {
    pub dir: PathBuf,
    mem_table: MemTable,
    storage: Storage,
}

impl Db {
    pub fn new(dir: PathBuf) -> Db {
        let storage = match Storage::new(&dir) {
            Ok(s) => s,
            Err(e) => {
                panic!("Error in creating the file {}", e);
            }
        };

        let mem_table = MemTable::new();

        Db {
            dir,
            storage,
            mem_table,
        }
    }

    pub fn init_from_existing(dir: PathBuf) -> io::Result<Db> {
        let mut mem_table = MemTable::new();

        let files = scan_dir(&dir)?;

        for file in files {
            let data: Vec<Entry> = StorageIterator::new(&file)?.collect();
            for entry in data {
                if !entry.deleted {
                    mem_table.set_or_insert(&entry.key, &entry.value.unwrap(), entry.timestamp);
                } else {
                    mem_table.delete(&entry.key, entry.timestamp);
                }
            }
        }

        // create the new storage
        // suggestion: can continue from the last available file
        let mut storage = Storage::new(&dir)?;

        for entry in mem_table.get_all() {
            if !entry.deleted {
                storage.set(
                    &entry.key,
                    &entry.value.as_ref().unwrap(),
                    false,
                    entry.timestamp,
                )?;
            } else {
                storage.delete(&entry.key, entry.timestamp)?;
            }
        }
        storage.commit()?;

        Ok(Db {
            dir,
            storage,
            mem_table,
        })
    }

    pub fn set(&mut self, key: &[u8], value: &[u8]) -> Result<(), usize> {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros();

        if self.storage.set(key, value, false, timestamp).is_err() {
            return Err(0);
        }
        if self.storage.commit().is_err() {
            return Err(1);
        }

        self.mem_table.set_or_insert(key, value, timestamp);

        Ok(())
    }

    pub fn get(&mut self, key: &[u8]) -> Option<Entry> {
        if let Some(res) = self.mem_table.get(key) {
            return Some(Entry {
                key: res.key.clone(),
                value: res.value.clone(),
                timestamp: res.timestamp.clone(),
                deleted: res.deleted.clone(),
            });
        }
        None
    }

    pub fn delete(&mut self, key: &[u8]) -> Result<(), usize> {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros();
        if self.storage.delete(key, timestamp).is_err() {
            return Err(0);
        }

        if self.storage.commit().is_err() {
            return Err(1);
        }

        self.mem_table.delete(key, timestamp);

        return Ok(());
    }
}
