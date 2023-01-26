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

#[cfg(test)]
mod test {
    use std::{path::PathBuf, time::SystemTime};

    use rand::Rng;

    use crate::{
        entry::Entry,
        storage::Storage,
        storage_iterator::StorageIterator,
        utils::{create_dir, remove_dir, scan_dir},
    };

    use super::Db;

    #[test]
    fn init_engine() {
        let mut range = rand::thread_rng();
        let path = PathBuf::from(format!("./test-{}-temp", range.gen::<u32>()));

        create_dir(&path).unwrap();

        let mut db = Db::new(path);

        let key1 = b"Hello".to_owned();
        let value1 = *b"World!";

        db.set(&key1, &value1).unwrap();

        assert_eq!(b"Hello".to_owned().to_vec(), db.get(&key1).unwrap().key);

        let key2 = b"Name".to_owned();
        let value2 = *b"Vahid";

        db.set(&key2, &value2).unwrap();

        assert_eq!(
            b"Vahid".to_owned().to_vec(),
            db.get(&key2).unwrap().value.unwrap()
        );

        db.delete(&key1).unwrap();

        assert_eq!(true, db.get(&key1).unwrap().deleted);

        // Clean up
        remove_dir(&db.dir).expect("Error: could not remove the directory");
    }

    #[test]
    fn read_from_multiple_files() {
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

        let mut storage = Storage::new(&path).unwrap();

        let key1 = b"Hello".to_owned();
        let value1 = *b"RUST";
        let timestamp1 = SystemTime::now().elapsed().unwrap().as_micros();
        storage
            .set(&key1, &value1, false, timestamp1)
            .expect("Error: could not write in the file");

        let key2 = b"gg".to_owned();
        let timestamp2 = SystemTime::now().elapsed().unwrap().as_micros();
        storage
            .delete(&key2, timestamp2)
            .expect("Error: could not write in the file");

        storage.commit().unwrap();

        drop(storage);

        let db = Db::init_from_existing(path);
        
        // Clean up
        // remove_dir(&path).unwrap();
    }
}
