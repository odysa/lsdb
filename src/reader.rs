use crate::{
    common::{Command, OffSet},
    error::Result,
};
use std::{
    fs::File,
    io::{BufReader, Read, Seek, SeekFrom},
};

pub struct DataBaseReader {
    reader: PosReader<File>,
}

impl DataBaseReader {
    pub fn new(reader: BufReader<File>) -> Result<DataBaseReader> {
        let reader = PosReader::new(reader)?;
        Ok(DataBaseReader { reader })
    }

    pub fn read(&mut self, offset: &OffSet) -> Result<Command> {
        Ok(self.reader.read_command(offset)?)
    }
    pub fn seek(&mut self, pos: SeekFrom) -> Result<u64> {
        Ok(self.reader.seek(pos)?)
    }
    pub fn reader(&mut self) -> &mut BufReader<File> {
        self.reader.reader()
    }
}

impl Read for DataBaseReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        Ok(self.reader.read(buf)?)
    }
}
struct PosReader<T: Seek + Read> {
    reader: BufReader<T>,
    pos: u64,
}

impl<T: Seek + Read> Seek for PosReader<T> {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        self.pos = self.reader.seek(pos)?;
        Ok(self.pos)
    }
}

impl<T: Seek + Read> PosReader<T> {
    fn new(mut reader: BufReader<T>) -> Result<Self> {
        let pos = reader.seek(SeekFrom::Start(0))?;
        Ok(PosReader { pos, reader })
    }

    fn deserialize(&self, v: &[u8]) -> Result<Command> {
        Ok(serde_json::from_slice(v)?)
    }

    fn reader(&mut self) -> &mut BufReader<T> {
        self.reader.by_ref()
    }
}

impl Read for PosReader<File> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        Ok(self.reader.read(buf)?)
    }
}

impl PosReader<File> {
    fn read_command(&mut self, offset: &OffSet) -> Result<Command> {
        self.reader.seek(SeekFrom::Start(offset.start()))?;
        let mut buffer = vec![0u8; offset.len() as usize];
        self.reader.read_exact(&mut buffer)?;

        match self.deserialize(&buffer) {
            Ok(value) => Ok(value),
            Err(e) => Err(e),
        }
    }
}
