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
    fn new(writer: PosWriter<File>) -> LogWriter {
        LogWriter { writer, wild: 0 }
    }

    fn write(&mut self, command: Command) -> Result<OffSet> {
        let pos = self.writer.pos;
        self.serialize(command)?;

        let new_pos = self.writer.pos;
        let offset = OffSet::new(pos, new_pos);
        self.wild += offset.len;

        self.writer.flush()?;

        Ok(offset)
    }

    fn serialize(&mut self, cmd: Command) -> Result<()> {
        Ok(serde_json::to_writer(&mut self.writer, &cmd)?)
    }

    fn compact(&mut self) -> Result<()> {
        println!("unimplemented!");
        Ok(())
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

    fn write(&mut self, content: &[u8]) -> Result<u64> {
        self.pos += self.writer.write(content)? as u64;
        Ok(self.pos)
    }

    fn flush(&mut self) -> Result<()> {
        Ok(self.writer.flush()?)
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
}

impl OffSet {
    pub fn new(start: u64, end: u64) -> OffSet {
        OffSet {
            start,
            len: end - start,
        }
    }

    pub fn start(&self) -> u64 {
        self.start
    }

    pub fn len(&self) -> u64 {
        self.len
    }
}
