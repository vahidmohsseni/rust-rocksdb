use std::{
    fs::{self, File, OpenOptions},
    io::{self, BufReader},
    path::{Path, PathBuf},
};

#[allow(dead_code)]
pub(crate) fn file_reader(path: &Path) -> BufReader<File> {
    let file = OpenOptions::new().read(true).open(path).unwrap();
    return BufReader::new(file);
}

#[allow(dead_code)]
pub(crate) fn scan_dir(dir: &Path) -> io::Result<Vec<PathBuf>> {
    let mut files = fs::read_dir(&dir)?
        .map(|res| res.map(|e| e.path()))
        .collect::<Result<Vec<_>, io::Error>>()?;
    files.sort();
    Ok(files)
}

#[allow(dead_code)]
pub(crate) fn remove_dir(dir: &Path) -> io::Result<()> {
    fs::remove_dir_all(dir)?;
    Ok(())
}

#[allow(dead_code)]
pub(crate) fn remove_file(path: &Path) -> io::Result<()> {
    fs::remove_file(path)?;
    Ok(())
}

#[allow(dead_code)]
pub(crate) fn create_dir(dir: &Path) -> io::Result<()> {
    fs::create_dir(dir)?;
    Ok(())
}

#[cfg(test)]
mod test {
    use std::path::PathBuf;

    use crate::utils::remove_dir;

    use super::{scan_dir, create_dir};


    #[test]
    fn test_scan_dir(){
        let dir = PathBuf::from("test-directory".to_string());
        create_dir(&dir).unwrap();
        let files = scan_dir(&dir).unwrap();
        println!("files: {:?}", files);
        assert_eq!(files.len(), 0);
        remove_dir(&dir).unwrap();
    }
}
