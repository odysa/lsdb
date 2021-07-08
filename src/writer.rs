use crate::common::{Command, OffSet};
use crate::error::{Error, Result};
use std::fs::File;
use std::io::{BufWriter, Seek, SeekFrom, Write};

pub struct DataBaseWriter {
    writer: PosWriter<File>,
    // length of content which is not compacted
    wild: u64,
}

impl DataBaseWriter {
    const COMPACT_THRESHOLD: u64 = 1 * 10 * 1024;

    pub fn new(writer: BufWriter<File>) -> Result<Self> {
        let writer = PosWriter::new(writer)?;
        Ok(DataBaseWriter { writer, wild: 0 })
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
            Command::Remove { key: _ } => OffSet::new(pos, new_pos, None),
            _ => {
                return Err(Error::invalid_command(
                    "command should not be written".to_string(),
                ))
            }
        };

        self.wild += offset.len();

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
