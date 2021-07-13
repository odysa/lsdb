use crate::common::{Command, KvsEngine, OffSet};
use crate::database::LogDataBase;
use crate::error::{Error, Result};
use crate::reader::{DataBaseReader, PosReader};
use crate::writer::PosWriter;
use serde_json::Deserializer;
use std::ffi::OsStr;
use std::fs::{self, DirEntry, File, OpenOptions};
use std::io::{BufReader, BufWriter, SeekFrom};
use std::path::PathBuf;
use std::{collections::HashMap, path::Path};

/// Used to store key and value
/// # Example
///
/// ```
/// use kvs::kvs_store::KvStore;
/// use std::path::Path;
/// use kvs::common::KvsEngine;
///
/// let path = Path::new("");
/// let mut kvs = KvStore::open(path).unwrap();
/// kvs.set("key".to_string(), "value".to_string()).unwrap();
/// assert_eq!(kvs.get("key".to_string()).unwrap(),Some("value".to_string()));
/// ```
pub struct KvStore {
    path: PathBuf,
    map: HashMap<String, String>,
    writer: PosWriter<File>,
    readers: HashMap<u64, PosReader<File>>,
    index: HashMap<String, OffSet>,
    // current number of database file
    current_no: u64,
    // how many bytes not compacted
    wild: u64,
}

impl KvStore {
    pub fn open(path: &Path) -> Result<Self> {
        let path = path.join("kvs.db");
        let maintainer = LogDataBase::new(path)?;
        let (reader, writer) = Self::new_db(&path)?;
        let readers = HashMap::<u64, PosReader<File>>::new();
        readers.insert(0, reader);
        let store = KvStore {
            path,
            readers,
            writer,
            map: HashMap::new(),
            index: HashMap::new(),
            current_no: 0,
            wild: 0,
        };
        store.load_from_db()?;
        Ok(store)
    }

    fn load_from_db(&mut self) -> Result<()> {
        // move to start of file
        let mut pos = self.reader.seek(SeekFrom::Start(0))?;
        let reader = self.reader.reader();
        let mut stream = Deserializer::from_reader(reader).into_iter::<Command>();
        // parse command from file
        while let Some(cmd) = stream.next() {
            let new_pos = stream.byte_offset() as u64;
            match cmd? {
                Command::Set { key, value } => {
                    self.index
                        .insert(key, OffSet::new(0, pos, new_pos, Some(value)));
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

    fn new_db(file_path: &Path) -> Result<(PosReader<File>, PosWriter<File>)> {
        let write_file = OpenOptions::new()
            .write(true)
            .create(true)
            .open(&file_path)?;
        let read_file = OpenOptions::new()
            .write(false)
            .read(true)
            .open(&file_path)?;

        let writer = BufWriter::new(write_file);
        let writer = PosWriter::new(writer)?;

        let reader = BufReader::new(read_file);
        let reader = PosReader::new(reader)?;

        Ok((reader, writer))
    }

    pub fn compact(&mut self) -> Result<()> {
        let new_no = self.current_no + 1;
        // skip current new file
        self.current_no += 1;

        Ok(())
    }

    // get writer of new db file
    fn new_log_writer(&mut self, no: u64) -> Result<PosWriter<File>> {
        let path = self.db_path(no);
        let writer = OpenOptions::new()
            .create(true)
            .write(true)
            .append(true)
            .open(&path)?;
        let writer = BufWriter::new(writer);
        let writer = PosWriter::new(writer)?;
        Ok(writer)
    }
    // get db file path by no
    fn db_path(&mut self, no: u64) -> PathBuf {
        self.path.join(format!("{}.db", no))
    }
    // get list of db files in path
    fn db_list(&self) -> Result<Vec<u64>> {
        //
        let take_entry =
            |res: std::result::Result<DirEntry, std::io::Error>| -> Result<_> { Ok(res?.path()) };

        let filter_invalid_file =
            |path: &PathBuf| path.is_file() && path.extension() == Some("db".as_ref());

        // remove file extension which is .db
        let remove_extension = |s: &str| s.trim_end_matches(".db");

        let str_to_u64 = str::parse::<u64>;

        // get number of file
        let file_name_to_no = |path: PathBuf| {
            path.file_name()
                .and_then(OsStr::to_str)
                .map(remove_extension)
                .map(str_to_u64)
        };

        let mut list = fs::read_dir(self.path)?
            .flat_map(take_entry)
            .filter(filter_invalid_file)
            .flat_map(file_name_to_no)
            .flatten()
            .collect();

        Ok(list)
    }
}

impl KvsEngine for KvStore {
    /// new a key-value store
    /// ```
    /// ```

    /// set the value of a given key
    /// ```
    /// ```
    fn set(&mut self, key: String, value: String) -> Result<()> {
        self.map.insert(key.to_owned(), value.to_owned());
        self.maintainer.set(key, value)?;
        Ok(())
    }
    /// set the value of a given key
    /// ```
    /// ```
    fn get(&mut self, key: String) -> Result<Option<String>> {
        if let Some(v) = self.map.get(&key) {
            return Ok(Some(v.to_owned()));
        }
        match self.maintainer.get(key) {
            Ok(res) => {
                if let Some(value) = res {
                    Ok(Some(value))
                } else {
                    Ok(None)
                }
            }
            Err(e) => Err(e),
        }
    }
    /// remove a given key in store
    /// ```
    /// ```
    fn remove(&mut self, key: String) -> Result<String> {
        self.map.remove(&key).unwrap_or_default();

        match self.maintainer.get(key.to_owned()) {
            Ok(res) => match res {
                None => Err(Error::key_not_found(format!(
                    "key {} you want to remove does not exist",
                    key
                ))),
                Some(value) => {
                    self.maintainer.remove(key)?;
                    Ok(value)
                }
            },
            Err(e) => Err(e),
        }
    }
}
