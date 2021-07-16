use crate::common::{Command, KvsEngine, OffSet};
use crate::error::{Error, ErrorKind, Result};
use crate::reader::PosReader;
use crate::writer::PosWriter;
use serde_json::Deserializer;
use std::ffi::OsStr;
use std::fs::{self, DirEntry, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom};
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
/// let path = Path::new("./db");
/// let mut kvs = KvStore::open(path).unwrap();
/// kvs.set("key".to_string(), "value".to_string()).unwrap();
/// assert_eq!(kvs.get("key".to_string()).unwrap(),Some("value".to_string()));
/// ```
pub struct KvStore {
    path: PathBuf,
    writer: PosWriter<File>,
    readers: HashMap<u64, PosReader<File>>,
    index: HashMap<String, OffSet>,
    // current number of database file
    current_no: u64,
    // how many bytes not compacted
    wild: u64,
}

impl KvStore {
    const COMPACT_THRESHOLD: u64 = 8 * 1024 * 1024;
    pub fn open(path: &Path) -> Result<Self> {
        let path = path.join("");
        // create dir
        fs::create_dir_all(&path)?;
        // list of all db file numbers
        let db_list = KvStore::db_list(&path)?;

        // get current db file, always create new file when create a new db
        let current_no = db_list.last().unwrap_or(&0) + 1;

        let writer = new_db_writer(&path, current_no)?;
        let readers = HashMap::<u64, PosReader<File>>::new();
        // store all key and it's pointer in memory
        let index: HashMap<String, OffSet> = HashMap::new();

        let mut store = KvStore {
            path,
            readers,
            writer,
            index,
            current_no,
            wild: 0,
        };
        // insert current new db reader to readers
        let current_reader = store.new_db_reader(current_no)?;
        store.readers.insert(current_no, current_reader);

        // read data into memory from db files
        for &db in &db_list {
            let file = File::open(KvStore::db_path(&mut store, db))?;
            let mut reader = PosReader::new(file)?;
            KvStore::load_from_db(&mut store, db, &mut reader)?;
            store.readers.insert(db, reader);
        }

        Ok(store)
    }

    fn load_from_db(&mut self, no: u64, reader: &mut PosReader<File>) -> Result<u64> {
        // move to start of file
        let mut pos = reader.seek(SeekFrom::Start(0))?;
        let reader = reader.reader();
        let mut stream = Deserializer::from_reader(reader).into_iter::<Command>();

        // parse command from file
        while let Some(cmd) = stream.next() {
            let new_pos = stream.byte_offset() as u64;
            match cmd? {
                Command::Set { key, .. } => {
                    if let Some(old_cmd) = self.index.insert(key, OffSet::new(no, pos, new_pos)) {
                        // size needed to be compacted
                        self.wild += old_cmd.len();
                    }
                }
                Command::Remove { key } => {
                    if let Some(old_cmd) = self.index.remove(&key) {
                        self.wild += old_cmd.len();
                    }
                    self.wild += new_pos - pos;
                }
                _ => return Err(Error::invalid_command("invalid command parsed".to_string())),
            }
            pos = new_pos;
        }

        Ok(self.wild)
    }

    pub fn compact(&mut self) -> Result<()> {
        // number of file to compact
        let compact_no = self.current_no + 1;
        let mut compact_writer = self.next_nth_db(1)?;

        // skip compact file
        self.writer = self.next_nth_db(1)?;

        // self.new_db(self.current_no)?;

        let mut new_pos = 0;

        for cmd in &mut self.index.values_mut() {
            // get reader of given file
            let reader = self
                .readers
                .get_mut(&cmd.no())
                .expect(format!("unable to read file {}", cmd.no()).as_str());

            // to the start of given offset
            reader.seek(SeekFrom::Start(cmd.start()))?;

            // read only length of offset
            let mut cmd_reader = reader.take(cmd.len());
            // write to
            let len = std::io::copy(&mut cmd_reader, &mut compact_writer)?;
            // update offset in memory
            *cmd = OffSet::new(compact_no, new_pos, len + new_pos);

            new_pos += len;
        }
        compact_writer.flush()?;

        // remove trash file
        let trash_db: Vec<_> = self
            .readers
            .keys()
            .filter(|&&no| no < compact_no)
            .cloned()
            .collect();

        for trash in trash_db {
            self.readers.remove(&trash);
            fs::remove_file(db_path(&self.path, trash))?;
        }
        Ok(())
    }

    // create next nth db file
    fn next_nth_db(&mut self, n: u64) -> Result<PosWriter<File>> {
        self.current_no += n;
        self.new_db(self.current_no)
    }
    // create a new db file and return writer to it
    fn new_db(&mut self, no: u64) -> Result<PosWriter<File>> {
        let writer = self.new_db_writer(no)?;
        let reader = self.new_db_reader(no)?;
        self.readers.insert(no, reader);

        Ok(writer)
    }
    // get writer of new db file
    fn new_db_writer(&mut self, no: u64) -> Result<PosWriter<File>> {
        Ok(new_db_writer(&self.path, no)?)
    }

    fn new_db_reader(&mut self, no: u64) -> Result<PosReader<File>> {
        Ok(new_db_reader(&self.path, no)?)
    }
    // get db file path by no
    fn db_path(&mut self, no: u64) -> PathBuf {
        self.path.join(format!("{}.db", no))
    }
    // get list of db files in path
    fn db_list(path: &PathBuf) -> Result<Vec<u64>> {
        //
        let take_entry =
            |res: std::result::Result<DirEntry, std::io::Error>| -> Result<_> { Ok(res?.path()) };

        let filter_invalid_file =
            |path: &PathBuf| path.is_file() && path.extension() == Some("db".as_ref());

        let str_to_u64 = str::parse::<u64>;

        // get number of file
        let file_name_to_no = |path: PathBuf| {
            path.file_name()
                .and_then(OsStr::to_str)
                .map(|s: &str| s.trim_end_matches(".db"))
                .map(str_to_u64)
        };

        let list = fs::read_dir(path)?
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
        let cmd = Command::Set {
            key: key.to_owned(),
            value,
        };
        let current_pos = self.writer.pos();
        serde_json::to_writer(&mut self.writer, &cmd)?;
        self.writer.flush()?;

        let offset = OffSet::new(self.current_no, current_pos, self.writer.pos());
        if let Some(old_cmd) = self.index.insert(key, offset) {
            self.wild += old_cmd.len();
        }

        if self.wild > KvStore::COMPACT_THRESHOLD {
            self.compact()?;
        }

        Ok(())
    }
    /// set the value of a given key
    /// ```
    /// ```
    fn get(&mut self, key: String) -> Result<Option<String>> {
        if let Some(offset) = self.index.get(&key) {
            if let Some(reader) = self.readers.get_mut(&offset.no()) {
                reader.seek(SeekFrom::Start(offset.start()))?;
                let cmd_reader = reader.take(offset.len());
                if let Command::Set { value, .. } = serde_json::from_reader(cmd_reader)? {
                    return Ok(Some(value));
                }
                return Err(Error::from(ErrorKind::InvalidCommand(format!(
                    "invalid command at db file:{}, position:{}",
                    offset.no(),
                    offset.start()
                ))));
            }
        }

        Ok(None)
    }
    /// remove a given key in store
    /// ```
    /// ```
    fn remove(&mut self, key: String) -> Result<String> {
        let result = self.get(key.to_owned())?;
        if let Some(value) = result {
            let cmd = Command::Remove {
                key: key.to_owned(),
            };
            serde_json::to_writer(&mut self.writer, &cmd)?;
            self.writer.flush()?;
            self.index.remove(&key);
            Ok(value)
        } else {
            Err(Error::from(ErrorKind::KeyNotFound(format!(
                "key {} not found",
                key
            ))))
        }
    }
}

fn new_db_writer(path: &PathBuf, no: u64) -> Result<PosWriter<File>> {
    let path = db_path(path, no);
    let writer = OpenOptions::new()
        .create(true)
        .write(true)
        .append(true)
        .open(&path)?;
    let writer = PosWriter::new(writer)?;
    Ok(writer)
}

fn new_db_reader(path: &PathBuf, no: u64) -> Result<PosReader<File>> {
    let path = db_path(path, no);
    let file = OpenOptions::new().read(true).write(false).open(&path)?;
    let reader = PosReader::new(file)?;
    Ok(reader)
}
// get path to given db file
fn db_path(path: &PathBuf, no: u64) -> PathBuf {
    path.join(format!("{}.db", no))
}
