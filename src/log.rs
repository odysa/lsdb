use crate::error::{Error, ErrorKind, Result};
use crate::Command;
use std::fs::{File, OpenOptions};
use std::io::{prelude::*, BufReader, BufWriter, SeekFrom};
use std::path::{Path, PathBuf};
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
    pub fn log_set(&mut self, key: &str, value: &str) -> Result<()> {
        self.append(format!("set,{},{}", key, value))?;
        Ok(())
    }
    pub fn log_rem(&mut self, key: &str) -> Result<()> {
        self.append(format!("rem,{}", key))?;
        Ok(())
    }
    /// get value by given key
    pub fn get_value(&mut self, key: &str) -> Result<Option<String>> {
        let content = self.read_to_string()?;
        for line in content.lines().rev() {
            if line.is_empty() || line == "\n" {
                continue;
            }
            if let Some(res) = self.deserialize_log(line) {
                let (cmd, log_key, log_value) = res;
                if log_key != key {
                    continue;
                }
                match cmd {
                    Command::Set => return Ok(Some(log_value)),
                    Command::Rem => return Ok(None),
                }
            } else {
                return Err(Error::from(ErrorKind::InvalidLog(format!(
                    "invalid log {}",
                    line
                ))));
            }
        }
        Ok(None)
    }
    /// deserialize log
    fn deserialize_log(&self, line: &str) -> Option<(Command, String, String)> {
        let mut split = line.split(',');

        let cmd = match split.next() {
            Some("set") => Command::Set,
            Some("rem") => Command::Rem,
            _ => return None,
        };
        let key: String;

        if let Some(v) = split.next() {
            key = v.to_owned();
        } else {
            return None;
        }
        // because rem does not need to specify value, we use unwrap_or_default
        let value = split.next().unwrap_or_default().to_owned();
        Some((cmd, key, value))
    }

    fn append(&mut self, content: String) -> Result<()> {
        let content = content + "\n";
        if let Err(e) = self.writer.write(content.as_bytes()) {
            eprintln!("error to write, {}", e);
            Err(Error::from(e))
        } else {
            Ok(())
        }
    }

    fn read_to_string(&mut self) -> Result<String> {
        let mut result = String::new();
        self.reader.seek(SeekFrom::Start(0))?;
        self.reader.read_to_string(&mut result)?;
        Ok(result)
    }
}

fn new_db(file_path: &Path) -> Result<(BufReader<File>, BufWriter<File>)> {
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
