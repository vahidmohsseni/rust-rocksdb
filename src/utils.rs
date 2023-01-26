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
pub(crate) fn create_dir(dir: &Path) -> io::Result<()> {
    fs::create_dir(dir)?;
    Ok(())
}
