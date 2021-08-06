use crate::common::{Command, KvsEngine, OffSet};
use crate::error::{Error, ErrorKind, Result};
use crate::reader::PosReader;
use crate::writer::PosWriter;
use serde_json::Deserializer;
use std::cell::RefCell;
use std::ffi::OsStr;
use std::fs::{self, DirEntry, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, RwLock};
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
    path: Arc<PathBuf>,
    writer: Arc<Mutex<PosWriter<File>>>,
    readers: RefCell<HashMap<u64, PosReader<File>>>,
    index: Arc<RwLock<HashMap<String, OffSet>>>,
    // current number of database file
    current_no: Arc<AtomicU64>,
    // how many bytes which are not compacted
    wild: Arc<AtomicU64>,
}

impl KvStore {
    const COMPACT_THRESHOLD: u64 = 100 * 1024 * 1024;
    pub fn open(path: &Path) -> Result<Self> {
        let path = path.join("");
        // create dir
        fs::create_dir_all(&path)?;
        // list of all db file numbers
        let db_list = KvStore::db_list(&path)?;

        // get current db file, always create new file when create a new db
        let no = db_list.last().unwrap_or(&0) + 1;
        let current_no = Arc::new(AtomicU64::new(no));

        let writer = Arc::new(Mutex::new(new_db_writer(&path, no)?));
        let readers = RefCell::new(HashMap::<u64, PosReader<File>>::new());
        // store all key and it's pointer in memory
        let index: Arc<RwLock<HashMap<String, OffSet>>> = Arc::new(RwLock::new(HashMap::new()));

        let path = Arc::new(path);

        let store = KvStore {
            path,
            readers,
            writer,
            index,
            current_no,
            wild: Arc::new(AtomicU64::new(0)),
        };
        // insert current new db reader to readers
        {
            let current_reader = store.new_db_reader(no)?;
            let mut readers = store.readers.borrow_mut();
            readers.insert(no, current_reader);

            // read data into memory from db files
            for &db in &db_list {
                let file = File::open(db_path(&store.path, db))?;
                let mut reader = PosReader::new(file)?;
                store.load_from_db(db, &mut reader)?;
                readers.insert(db, reader);
            }
        }

        Ok(store)
    }

    fn load_from_db(&self, no: u64, reader: &mut PosReader<File>) -> Result<()> {
        // move to start of file
        let mut pos = reader.seek(SeekFrom::Start(0))?;
        let reader = reader.reader();
        let mut stream = Deserializer::from_reader(reader).into_iter::<Command>();

        // parse command from file
        while let Some(cmd) = stream.next() {
            let new_pos = stream.byte_offset() as u64;
            match cmd? {
                Command::Set { key, .. } => {
                    if let Ok(mut index) = self.index.write() {
                        if let Some(old_cmd) = index.insert(key, OffSet::new(no, pos, new_pos)) {
                            // size needed to be compacted
                            self.wild.fetch_add(old_cmd.len(), Ordering::SeqCst);
                        }
                    }
                }
                Command::Remove { key } => {
                    if let Ok(mut index) = self.index.write() {
                        if let Some(old_cmd) = index.remove(&key) {
                            self.wild.fetch_add(old_cmd.len(), Ordering::SeqCst);
                        }
                        self.wild.fetch_add(new_pos - pos, Ordering::SeqCst);
                    }
                }
                _ => return Err(Error::invalid_command("invalid command parsed".to_string())),
            }
            pos = new_pos;
        }

        Ok(())
    }
    // compact db files to single one, and remove redundant entries
    pub fn compact(&self) -> Result<()> {
        // number of file to compact
        let compact_no = self.current_no.load(Ordering::SeqCst) + 1;

        let mut compact_writer = self.next_nth_db(1)?;

        // skip compact file
        {
            let next_pos_writer = self.next_nth_db(1)?;
            let mut writer = self.writer.lock().expect("unable get lock");
            *writer = next_pos_writer;
        }

        let mut new_pos = 0;
        let mut readers = self.readers.borrow_mut();

        if let Ok(mut index) = self.index.write() {
            for cmd in index.values_mut() {
                let reader = readers
                    .get_mut(&cmd.no())
                    .unwrap_or_else(|| panic!("unable to read file {}", cmd.no()));

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
        }
        compact_writer.flush()?;

        // remove trash file
        let trash_db: Vec<_> = readers
            .keys()
            .filter(|&&no| no < compact_no)
            .cloned()
            .collect();

        for trash in trash_db {
            readers.remove(&trash);
            fs::remove_file(db_path(&self.path, trash))?;
        }

        self.wild.store(0, Ordering::SeqCst);
        Ok(())
    }

    // create next nth db file
    fn next_nth_db(&self, n: u64) -> Result<PosWriter<File>> {
        self.current_no.fetch_add(n, Ordering::SeqCst);
        self.new_db(self.current_no.load(Ordering::SeqCst))
    }
    // create a new db file and return writer to it
    fn new_db(&self, no: u64) -> Result<PosWriter<File>> {
        let writer = self.new_db_writer(no)?;
        let reader = self.new_db_reader(no)?;
        let mut readers = self.readers.borrow_mut();
        readers.insert(no, reader);

        Ok(writer)
    }
    // get writer of new db file
    fn new_db_writer(&self, no: u64) -> Result<PosWriter<File>> {
        new_db_writer(&self.path, no)
    }

    fn new_db_reader(&self, no: u64) -> Result<PosReader<File>> {
        new_db_reader(&self.path, no)
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

        let mut list: Vec<u64> = fs::read_dir(path)?
            .flat_map(take_entry)
            .filter(filter_invalid_file)
            .flat_map(file_name_to_no)
            .flatten()
            .collect();

        // sort to process in sequence
        list.sort_unstable();
        Ok(list)
    }
    // append result to db file
    fn append(&self, writer: &mut MutexGuard<PosWriter<File>>, cmd: &Command) -> Result<()> {
        let vec = serde_json::to_vec(cmd)?;
        writer.write_all(&vec)?;
        writer.flush()?;
        Ok(())
    }
}

impl KvsEngine for KvStore {
    /// new a key-value store
    /// ```
    /// ```

    /// set the value of a given key
    /// ```
    /// ```
    fn set(&self, key: String, value: String) -> Result<()> {
        let cmd = Command::Set {
            key: key.to_owned(),
            value,
        };

        let mut writer = self.writer.lock().unwrap();
        let current_pos = writer.pos();
        // append command to db file
        self.append(&mut writer, &cmd)?;
        let new_pos = writer.pos();
        // unlock writer
        drop(writer);

        let offset = OffSet::new(self.current_no.load(Ordering::SeqCst), current_pos, new_pos);

        // try to compact db files
        if let Ok(mut index) = self.index.write() {
            if let Some(old_cmd) = index.insert(key, offset) {
                self.wild.fetch_add(old_cmd.len(), Ordering::SeqCst);
            }
        }

        if self.wild.load(Ordering::SeqCst) > KvStore::COMPACT_THRESHOLD {
            self.compact()?;
        }
        Ok(())
    }
    /// set the value of a given key
    /// ```
    /// ```
    fn get(&self, key: String) -> Result<Option<String>> {
        let mut readers = self.readers.borrow_mut();

        if let Ok(index) = self.index.write() {
            // check key in memory
            if let Some(offset) = index.get(&key) {
                // initialize a new reader
                if !readers.contains_key(&offset.no()) {
                    let no = offset.no();
                    let path = db_path(&self.path, no);
                    let file = File::open(path)?;
                    let reader = PosReader::new(file)?;
                    readers.insert(no, reader);
                }

                if let Some(reader) = readers.get_mut(&offset.no()) {
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
        }

        Ok(None)
    }
    /// remove a given key in store
    /// ```
    /// ```
    fn remove(&self, key: String) -> Result<String> {
        match self.get(key.to_owned())? {
            Some(value) => {
                let cmd = Command::Remove {
                    key: key.to_owned(),
                };

                {
                    let mut writer = self.writer.lock().unwrap();
                    self.append(&mut writer, &cmd)?;

                    let mut index = self.index.write().unwrap();
                    let offset = index.remove(&key).expect("key not found");
                    self.wild.fetch_add(offset.len(), Ordering::SeqCst);
                }

                if self.wild.load(Ordering::SeqCst) > KvStore::COMPACT_THRESHOLD {
                    self.compact()?;
                }

                Ok(value)
            }
            None => Err(Error::from(ErrorKind::KeyNotFound(format!(
                "key {} not found",
                key
            )))),
        }
    }
}

impl Clone for KvStore {
    fn clone(&self) -> Self {
        KvStore {
            readers: RefCell::new(HashMap::new()),
            path: Arc::clone(&self.path),
            writer: Arc::clone(&self.writer),
            index: Arc::clone(&self.index),
            current_no: Arc::clone(&self.current_no),
            wild: Arc::clone(&self.wild),
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
