use std::{
    io,
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};

use crate::{
    entry::Entry,
    memtable::MemTable,
    storage::Storage,
    storage_iterator::StorageIterator,
    utils::{remove_file, scan_dir, create_dir},
};

#[derive(Debug)]
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

        let files = scan_dir(&dir).or_else(|e| {if let io::ErrorKind::NotFound = e.kind(){ create_dir(&dir)?; Ok(Vec::new())} else {Err(e)}})?;
        for file in &files {
            let data: Vec<Entry> = StorageIterator::new(file)?.collect();
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

        // now it is safe to remove old DB files
        // delete the files
        // suggestion: this can be an option from config
        for file in &files {
            remove_file(file)?;
        }

        Ok(Db {
            dir,
            storage,
            mem_table,
        })
    }

    pub fn set(&mut self, key: &[u8], value: &[u8]) -> io::Result<()> {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?
            .as_micros();

        self.storage.set(key, value, false, timestamp)?;
        self.storage.commit()?;

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

    pub fn delete(&mut self, key: &[u8]) -> io::Result<()> {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?
            .as_micros();

        self.storage.delete(key, timestamp)?;

        self.storage.commit()?;

        self.mem_table.delete(key, timestamp);

        Ok(())
    }

    pub fn get_snapshot(&mut self) -> Vec<u8> {
        let entries = self.mem_table.get_all();
        let mut snapshot: Vec<u8> = Vec::new();
        for data in entries {
            if !data.deleted {
                snapshot.extend_from_slice(&(data.key.len() as u64).to_le_bytes());
                snapshot.extend_from_slice(&(data.deleted as u8).to_le_bytes());
                snapshot
                    .extend_from_slice(&(data.value.as_ref().unwrap().len() as u64).to_le_bytes());

                snapshot.extend_from_slice(&(data.key));
                snapshot.extend_from_slice(&(data.value.as_ref().unwrap()));
                snapshot.extend_from_slice(&(data.timestamp.to_le_bytes()));
            }
        }
        snapshot
    }

    pub fn set_snapshot(&mut self, raw_data: Vec<u8>) -> io::Result<()> {
        self.storage.write_all(raw_data)?;
        let files = scan_dir(&self.dir)?;
        let data: Vec<Entry> = StorageIterator::new(&files.last().unwrap())?.collect();
        for entry in data {
            self.mem_table
                .set_or_insert(&entry.key, &entry.value.unwrap(), entry.timestamp);
        }

        Ok(())
    }

    pub fn purge_database(&mut self) -> io::Result<()> {
        self.storage.purge_storage()?;
        self.mem_table.purge_mem_table();
        Ok(())
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

        let key5 = b"Hello".to_owned();
        let value5 = *b"RUST";
        let timestamp5 = SystemTime::now().elapsed().unwrap().as_micros();
        storage
            .set(&key5, &value5, false, timestamp5)
            .expect("Error: could not write in the file");

        let key6 = b"gg".to_owned();
        let timestamp6 = SystemTime::now().elapsed().unwrap().as_micros();
        storage
            .delete(&key6, timestamp6)
            .expect("Error: could not write in the file");

        storage.commit().unwrap();

        drop(storage);

        let mut db = Db::init_from_existing(path).unwrap();

        // see a key exists
        assert_eq!(b"gg".to_owned().to_vec(), db.get(&key3).unwrap().key);
        assert_eq!(None, db.get(&key3).unwrap().value);

        assert_eq!(b"Hello".to_owned().to_vec(), db.get(&key5).unwrap().key);
        assert_eq!(
            b"RUST".to_owned().to_vec(),
            db.get(&key5).unwrap().value.unwrap()
        );

        // check the new storage file
        let files = scan_dir(&db.dir).unwrap();
        let str_iter = StorageIterator::new(&files[files.len() - 1]).unwrap();

        let data: Vec<Entry> = str_iter.collect();

        assert_eq!(3, data.len());

        // Clean up
        remove_dir(&db.dir).unwrap();
    }

    #[test]
    fn test_init_from(){
        let mut range = rand::thread_rng();
        let path = PathBuf::from(format!("./test-{}-temp", range.gen::<u32>()));
        let mut db = Db::init_from_existing(path).unwrap();
        let key1 = b"Hello".to_owned();
        let value1 = *b"World!";

        db.set(&key1, &value1).unwrap();

        assert_eq!(b"Hello".to_owned().to_vec(), db.get(&key1).unwrap().key);

        // clean up
        remove_dir(&db.dir).unwrap();
    }

    #[test]
    fn snapshot_test() {
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

        let snapshot = db.get_snapshot();

        // remove dir
        remove_dir(&db.dir).unwrap();
        drop(db);

        let path = PathBuf::from(format!("./test-{}-temp", range.gen::<u32>()));
        create_dir(&path).unwrap();
        let mut db = Db::new(path);

        db.set_snapshot(snapshot).unwrap();

        assert_eq!(
            b"Vahid".to_owned().to_vec(),
            db.get(&key2).unwrap().value.unwrap()
        );

        // clean up
        remove_dir(&db.dir).unwrap();
    }
}
