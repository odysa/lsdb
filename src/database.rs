use crate::common::{Command, DataBase, OffSet};
use crate::error::{Error, ErrorKind, Result};
use crate::reader::DataBaseReader;
use crate::writer::DataBaseWriter;
use serde_json::Deserializer;
use std::collections::HashMap;
use std::fs::OpenOptions;
use std::io::{BufReader, BufWriter, SeekFrom};
use std::path::{Path, PathBuf};
pub struct LogDataBase {
    writer: DataBaseWriter,
    reader: DataBaseReader,
    index: HashMap<String, OffSet>,
}

impl LogDataBase {
    pub fn new(path: PathBuf) -> Result<Self> {
        let (reader, writer) = Self::new_db(&path)?;
        let index = HashMap::new();

        let mut logger = LogDataBase {
            writer,
            reader,
            index,
        };
        logger.load_from_db()?;
        Ok(logger)
    }

    fn new_db(file_path: &Path) -> Result<(DataBaseReader, DataBaseWriter)> {
        let write_file = OpenOptions::new()
            .write(true)
            .create(true)
            .open(&file_path)?;
        let read_file = OpenOptions::new()
            .write(false)
            .read(true)
            .open(&file_path)?;

        let writer = BufWriter::new(write_file);
        let writer = DataBaseWriter::new(writer)?;

        let reader = BufReader::new(read_file);
        let reader = DataBaseReader::new(reader)?;

        Ok((reader, writer))
    }

    fn load_from_db(&mut self) -> Result<()> {
        let mut pos = self.reader.seek(SeekFrom::Start(0))?;
        let reader = self.reader.reader();
        let mut stream = Deserializer::from_reader(reader).into_iter::<Command>();

        while let Some(cmd) = stream.next() {
            let new_pos = stream.byte_offset() as u64;
            match cmd? {
                Command::Set { key, value } => {
                    self.index.insert(key, OffSet::new(0, pos, new_pos));
                }
                Command::Remove { key } => {
                    self.index.remove(&key);
                }
                _ => return Err(Error::invalid_command("invalid command parsed".to_string())),
            }
            pos = new_pos;
        }

        Ok(())
    }

    // fn compact(&mut self) -> Result<()> {
    //     self.writer.seek(SeekFrom::Start(0))?;
    //     let map = self.index.clone();
    //     self.writer.reset()?;

    //     for (key, offset) in map.into_iter() {
    //         if let Some(value) = offset.value() {
    //             let new_offset = self.writer.write_buffer(Command::Set {
    //                 key: key.to_owned(),
    //                 value,
    //             })?;
    //             self.index.insert(key, new_offset);
    //         } else {
    //             self.index.remove(&key);
    //         }
    //     }

    //     // println!("compacting!!!!");
    //     self.writer.flush()?;

    //     Ok(())
    // }
}

impl DataBase for LogDataBase {
    fn set(&mut self, key: String, value: String) -> Result<()> {
        let command = Command::Set {
            key: key.to_owned(),
            value,
        };
        let offset = self.writer.write(command)?;
        // update pointer of this command
        self.index.insert(key, offset);

        if self.writer.should_compact() {}

        Ok(())
    }

    fn remove(&mut self, key: String) -> Result<()> {
        // remove this key from index at first
        if let None = self.index.remove(&key) {
            return Err(Error::from(ErrorKind::KeyNotFound(format!(
                "key: {} you want to remove not found",
                key
            ))));
        }

        let command = Command::Remove { key };
        self.writer.write(command)?;

        Ok(())
    }

    fn get(&mut self, key: String) -> Result<Option<String>> {
        if let Some(offset) = self.index.get(&key) {
            match self.reader.read(offset)? {
                Command::Set {
                    key: log_key,
                    value: log_value,
                } => {
                    if log_key != key {
                        Err(Error::from(ErrorKind::KeyNotFound(format!(
                            "key: {} you want to get not found",
                            key
                        ))))
                    } else {
                        Ok(Some(log_value))
                    }
                }
                Command::Remove { key: _ } => Err(Error::from(ErrorKind::KeyNotFound(format!(
                    "key: {} you want to get not found",
                    key
                )))),
                _ => {
                    return Err(Error::invalid_command(
                        "command should not be written".to_string(),
                    ))
                }
            }
        } else {
            Ok(None)
        }
    }
}
