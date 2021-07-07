use crate::error::Result;
use crate::Command;
use std::fs::File;
use std::io::{BufWriter, Seek, SeekFrom, Write};

pub struct LogWriter {
    writer: PosWriter<File>,
    // length of content which is not compacted
    wild: u64,
}

impl LogWriter {
    const COMPACT_THRESHOLD: u64 = 1 * 10 * 1024;

    pub fn new(writer: BufWriter<File>) -> Result<LogWriter> {
        let writer = PosWriter::new(writer)?;
        Ok(LogWriter { writer, wild: 0 })
    }

    pub fn flush(&mut self) -> Result<()> {
        Ok(self.writer.flush()?)
    }

    pub fn write(&mut self, command: Command) -> Result<OffSet> {
        self.write_base(command, false)
    }

    pub fn write_buffer(&mut self, command: Command) -> Result<OffSet> {
        self.write_base(command, true)
    }

    fn write_base(&mut self, command: Command, is_buffer: bool) -> Result<OffSet> {
        let pos = self.writer.pos;
        self.serialize(&command)?;

        let new_pos = self.writer.pos;

        let offset = match command {
            Command::Set { key: _, value } => OffSet::new(pos, new_pos, Some(value)),
            Command::Rem { key: _ } => OffSet::new(pos, new_pos, None),
        };

        self.wild += offset.len;

        if !is_buffer {
            self.writer.flush()?;
        }

        Ok(offset)
    }

    pub fn seek(&mut self, pos: SeekFrom) -> Result<u64> {
        Ok(self.writer.seek(pos)?)
    }

    pub fn reset(&mut self) -> Result<u64> {
        self.wild = 0;
        Ok(self.writer.reset()?)
    }

    pub fn should_compact(&self) -> bool {
        self.wild >= Self::COMPACT_THRESHOLD
    }

    fn serialize(&mut self, cmd: &Command) -> Result<()> {
        Ok(serde_json::to_writer(&mut self.writer, cmd)?)
    }
}
struct PosWriter<T: Write + Seek> {
    writer: BufWriter<T>,
    pos: u64,
}

impl<T: Write + Seek> PosWriter<T> {
    fn new(mut writer: BufWriter<T>) -> Result<Self> {
        let pos = writer.seek(SeekFrom::End(0))?;
        Ok(PosWriter { writer, pos })
    }

    fn flush(&mut self) -> Result<()> {
        Ok(self.writer.flush()?)
    }

    fn reset(&mut self) -> Result<u64> {
        Ok(self.seek(SeekFrom::Start(0))?)
    }
}

impl<T: Seek + Write> Seek for PosWriter<T> {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        self.pos = self.writer.seek(pos)?;
        Ok(self.pos)
    }
}

impl<T: Write + Seek> Write for PosWriter<T> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let size = self.writer.write(buf)?;
        self.pos += size as u64;
        Ok(size)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.writer.flush()
    }
}

pub struct OffSet {
    start: u64,
    len: u64,
    value: Option<String>,
}

impl Clone for OffSet {
    fn clone(&self) -> Self {
        OffSet {
            start: self.start,
            len: self.len,
            value: self.value.to_owned(),
        }
    }
}

impl OffSet {
    pub fn new(start: u64, end: u64, value: Option<String>) -> OffSet {
        OffSet {
            start,
            len: end - start,
            value,
        }
    }

    pub fn start(&self) -> u64 {
        self.start
    }

    pub fn len(&self) -> u64 {
        self.len
    }

    pub fn value(&self) -> Option<String> {
        self.value.to_owned()
    }
}
