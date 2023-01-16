use std::{path::{PathBuf, Path}, io::{BufWriter, self, Write}, fs::{File, OpenOptions}, time::{SystemTime, UNIX_EPOCH}};



pub struct Storage {
    path: PathBuf,
    writer: BufWriter<File>
}


impl Storage {
    pub fn new(dir: &Path) -> io::Result<Storage> {

        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros();
        
        let path = Path::new(dir).join(timestamp.to_string() + ".db");
        let file = OpenOptions::new().append(true).create(true).open(&path)?;
        let writer = BufWriter::new(file);

        Ok(Storage { 
            path,
            writer 
        })
    }

    pub fn from_path(path: &Path) -> io::Result<Storage> {
        let file = OpenOptions::new().append(true).create(true).open(&path)?;
        let writer = BufWriter::new(file);

        Ok(Storage { 
            path: path.to_path_buf(),
            writer
        })
    }

    // The data layout:
    // +---------------+-------------------+-----------------+----------+------------+-----------------+ 
    // | Key size (8B) | Deleted flag (1B) | Value size (8B) | key (?B) | value (?B) | timestamp (16B) |
    // +---------------+-------------------+-----------------+----------+------------+-----------------+ 
    // 
    pub fn set(&mut self, key: &[u8], value: &[u8], deleted: bool, timestamp: u128) -> io::Result<()>{
        self.writer.write_all(&key.len().to_le_bytes())?;
        self.writer.write_all(&(deleted as u8).to_le_bytes())?;
        self.writer.write_all(&value.len().to_le_bytes())?;

        self.writer.write_all(key)?;
        self.writer.write_all(value)?;

        self.writer.write_all(&timestamp.to_le_bytes())?;

        Ok(())
    }

    pub fn delete(&mut self, key: &[u8], timestamp: u128) -> io::Result<()>{
        self.writer.write_all(&key.len().to_le_bytes())?;
        self.writer.write_all(&(true as u8).to_le_bytes())?;
        let value_size = 0x0000 as u64;
        self.writer.write_all(&value_size.to_le_bytes())?;

        self.writer.write_all(&timestamp.to_le_bytes())?;

        Ok(())
    }

    pub fn commit(&mut self) -> io::Result<()> {
        self.writer.flush()?;
        Ok(())
    }



}


#[cfg(test)]
mod test {

    #[test]
    fn test_create(){
        
    }
}