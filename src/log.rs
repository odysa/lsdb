use crate::error::{Error, ErrorKind, Result};
use crate::log_writer::OffSet;
use crate::{Command, DataMaintainer};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{prelude::*, BufReader, BufWriter, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::Arc;
pub struct Logger {
    path: Arc<PathBuf>,
    writer: BufWriter<File>,
    reader: BufReader<File>,
    index: HashMap<String, OffSet>,
}

impl Logger {
    pub fn new(path: PathBuf) -> Result<Logger> {
        let (reader, writer) = new_db(&path)?;
        let index = HashMap::new();
        Ok(Logger {
            path: Arc::new(path),
            writer,
            reader,
            index,
        })
    }
    /// deserialize log
    fn deserialize_log(&self, line: &str) -> Result<Command> {
        let mut split = line.split(',');

        let cmd = split.next().unwrap();

        let key: String;

        if let Some(v) = split.next() {
            key = v.to_owned();
        } else {
            return Err(Error::from(ErrorKind::InvalidLog("invalid".to_string())));
        }
        // because rem does not need to specify value, we use unwrap_or_default
        let value = split.next().unwrap_or_default().to_owned();

        match cmd {
            "set" => Ok(Command::Set { key, value }),
            "rem" => Ok(Command::Rem { key }),
            _ => Err(Error::from(ErrorKind::InvalidLog("invalid".to_string()))),
        }
    }

    fn append(&mut self, content: String) -> Result<()> {
        let content = content + "\n";
        if let Err(e) = self.writer.write(content.as_bytes()) {
            eprintln!("error to write, {}", e);
            Err(Error::from(e))
        } else {
            self.writer.flush()?;
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

impl DataMaintainer for Logger {
    fn set(&mut self, key: &str, value: &str) -> Result<()> {
        self.append(format!("set,{},{}", key, value))?;
        Ok(())
    }

    fn rem(&mut self, key: &str) -> Result<()> {
        self.append(format!("rem,{}", key))?;
        Ok(())
    }

    fn get(&mut self, key: &str) -> Result<Option<String>> {
        let content = self.read_to_string()?;
        for line in content.lines().rev() {
            if line.is_empty() || line == "\n" {
                continue;
            }
            if let Ok(cmd) = self.deserialize_log(line) {
                let (log_key, log_value) = match cmd {
                    Command::Set { key, value } => (key, value),
                    Command::Rem { key: _ } => return Ok(None),
                };
                if log_key == key {
                    return Ok(Some(log_value));
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
