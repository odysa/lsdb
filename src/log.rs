use crate::error::{Error, Result};
use std::fs::{File, OpenOptions};
use std::io::{prelude::*, BufReader, BufWriter};
use std::path::PathBuf;
use std::sync::Arc;
pub struct Logger {
    path: Arc<PathBuf>,
    writer: BufWriter<File>,
    reader: BufReader<File>,
}

impl Logger {
    pub fn new(path: PathBuf) -> Result<Logger> {
        let (reader, writer) = new_db(&path)?;
        Ok(Logger {
            path: Arc::new(path),
            writer,
            reader,
        })
    }

    pub fn append(&mut self, content: String) -> Result<()> {
        let content = content + "\n";
        if let Err(e) = self.writer.write(content.as_bytes()) {
            eprintln!("error to write, {}", e);
            Err(Error::from(e))
        } else {
            Ok(())
        }
    }
    pub fn read(&mut self) -> Result<String> {
        let mut result = String::new();
        self.reader.read_to_string(&mut result)?;
        Ok(result)
    }
}

fn new_db(file_path: &PathBuf) -> Result<(BufReader<File>, BufWriter<File>)> {
    let write_file = OpenOptions::new()
        .write(true)
        .append(true)
        .create(true)
        .open(&file_path)?;
    let read_file = OpenOptions::new()
        .write(false)
        .read(true)
        .open(&file_path)?;
    let writer = BufWriter::new(write_file);
    let reader = BufReader::new(read_file);
    Ok((reader, writer))
}
