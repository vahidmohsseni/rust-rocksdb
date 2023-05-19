use std::{sync::{Arc, Mutex}, path::PathBuf, io};

use crate::{db::Db, entry::Entry};

#[derive(Clone)]
pub struct DBEngine {
    pub database: Arc<Mutex<Db>>,
}

impl DBEngine {
    pub fn new(dir: PathBuf) -> io::Result<Self>{
        Ok(Self { database: Arc::new(Mutex::new(Db::init_from_existing(dir)?)) })
    }

    pub fn set(&mut self, key: &[u8], value: &[u8]) -> io::Result<()> {
        let mut db = self.database.lock().unwrap();
        db.set(key, value)?;
        Ok(())
    }

    pub fn instant_set(&mut self, entry: &mut Entry) -> io::Result<()> {
        let mut db = self.database.lock().unwrap();
        db.instant_set(entry)?;
        Ok(())
    }

    pub fn get(&mut self, key: &[u8]) -> Option<Entry> {
        let mut db = self.database.lock().unwrap();
        db.get(key)
    }

    pub fn get_keys_with_pattern(&mut self, pattern: &[u8]) -> Vec<Entry> {
        let mut db = self.database.lock().unwrap();
        db.get_keys_with_pattern(pattern)
    }

    pub fn delete(&mut self, key: &[u8]) -> io::Result<()> {
        let mut db = self.database.lock().unwrap();
        db.delete(key)
    }

    pub fn get_snapshot(&mut self) -> Vec<u8> {
        let mut db = self.database.lock().unwrap();
        db.get_snapshot()
    }

    pub fn set_snapshot(&mut self, raw_data: Vec<u8>) -> io::Result<()> { 
        let mut db = self.database.lock().unwrap();
        db.set_snapshot(raw_data)
    }

    pub fn purge_database(&mut self) -> io::Result<()> {
        let mut db = self.database.lock().unwrap();
        db.purge_database()
    }
}
