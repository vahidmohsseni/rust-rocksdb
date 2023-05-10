use std::{sync::Arc, path::PathBuf, io};

use tokio::sync::Mutex;

use crate::{db::Db, entry::Entry};

#[derive(Clone)]
pub struct DBEngine {
    pub database: Arc<Mutex<Db>>,
}

impl DBEngine {
    pub fn new(dir: PathBuf) -> io::Result<Self>{
        Ok(Self { database: Arc::new(Mutex::new(Db::init_from_existing(dir)?)) })
    }

    pub async fn set(&mut self, key: &[u8], value: &[u8]) -> io::Result<()> {
        let mut db = self.database.lock().await;
        db.set(key, value)?;
        Ok(())
    }

    pub async fn get(&mut self, key: &[u8]) -> Option<Entry> {
        let mut db = self.database.lock().await;
        db.get(key)
    }

    pub async fn delete(&mut self, key: &[u8]) -> io::Result<()> {
        let mut db = self.database.lock().await;
        db.delete(key)
    }

    pub async fn get_snapshot(&mut self) -> Vec<u8> {
        let mut db = self.database.lock().await;
        db.get_snapshot()
    }

    pub async fn set_snapshot(&mut self, raw_data: Vec<u8>) -> io::Result<()> { 
        let mut db = self.database.lock().await;
        db.set_snapshot(raw_data)
    }

    pub async fn purge_database(&mut self) -> io::Result<()> {
        let mut db = self.database.lock().await;
        db.purge_database()
    }
}
