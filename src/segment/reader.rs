use crate::id::SegmentId;
use byteorder::{BigEndian, ReadBytesExt};
use std::{
    fs::File,
    io::{BufReader, Read},
    path::PathBuf,
};

/// Reads through a segment in order.
pub struct Reader {
    pub(crate) segment_id: SegmentId,
    inner: BufReader<File>,
    item_count: u64,
}

impl Reader {
    /// Initializes a new segment reader.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn new<P: Into<PathBuf>>(
        path: P,
        segment_id: SegmentId,
        item_count: u64,
    ) -> std::io::Result<Self> {
        let path = path.into();
        let file_reader = BufReader::new(File::open(path)?);

        Ok(Self {
            segment_id,
            inner: file_reader,
            item_count,
        })
    }
}

impl Iterator for Reader {
    type Item = std::io::Result<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.item_count == 0 {
            return None;
        }

        let key_len = match self.inner.read_u16::<BigEndian>() {
            Ok(v) => v,
            Err(e) => {
                if e.kind() == std::io::ErrorKind::UnexpectedEof {
                    return None;
                }
                return Some(Err(e));
            }
        };

        let mut key = vec![0; key_len.into()];
        if let Err(e) = self.inner.read_exact(&mut key) {
            return Some(Err(e));
        };

        // TODO: handle crc
        let _crc = match self.inner.read_u32::<BigEndian>() {
            Ok(v) => v,
            Err(e) => {
                if e.kind() == std::io::ErrorKind::UnexpectedEof {
                    return None;
                }
                return Some(Err(e));
            }
        };

        let val_len = match self.inner.read_u32::<BigEndian>() {
            Ok(v) => v,
            Err(e) => {
                if e.kind() == std::io::ErrorKind::UnexpectedEof {
                    return None;
                }
                return Some(Err(e));
            }
        };

        let mut val = vec![0; val_len as usize];
        if let Err(e) = self.inner.read_exact(&mut val) {
            return Some(Err(e));
        };

        self.item_count -= 1;

        Some(Ok((key, val)))
    }
}
