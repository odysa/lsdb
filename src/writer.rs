use crate::error::Result;
use std::io::{BufWriter, Seek, SeekFrom, Write};
pub struct PosWriter<T: Write + Seek> {
    writer: BufWriter<T>,
    pos: u64,
}

impl<T: Write + Seek> PosWriter<T> {
    pub fn new(content: T) -> Result<Self> {
        let mut writer = BufWriter::new(content);
        let pos = writer.seek(SeekFrom::End(0))?;
        Ok(PosWriter { writer, pos })
    }

    pub fn flush(&mut self) -> Result<()> {
        Ok(self.writer.flush()?)
    }

    pub fn reset(&mut self) -> Result<u64> {
        Ok(self.seek(SeekFrom::Start(0))?)
    }

    pub fn pos(&self) -> u64 {
        self.pos
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
