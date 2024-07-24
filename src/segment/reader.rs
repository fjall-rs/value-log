use super::{meta::METADATA_HEADER_MAGIC, writer::BLOB_HEADER_MAGIC};
use crate::id::SegmentId;
use byteorder::{BigEndian, ReadBytesExt};
use std::{
    fs::File,
    io::{BufReader, Read},
    path::PathBuf,
    sync::Arc,
};

/// Reads through a segment in order.
pub struct Reader {
    pub(crate) segment_id: SegmentId,
    inner: BufReader<File>,
    is_terminated: bool,
}

impl Reader {
    /// Initializes a new segment reader.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn new<P: Into<PathBuf>>(path: P, segment_id: SegmentId) -> crate::Result<Self> {
        let path = path.into();
        let file_reader = BufReader::new(File::open(path)?);

        Ok(Self {
            segment_id,
            inner: file_reader,
            is_terminated: false,
        })
    }
}

impl Iterator for Reader {
    type Item = crate::Result<(Arc<[u8]>, Arc<[u8]>)>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.is_terminated {
            return None;
        }

        {
            let mut buf = [0; BLOB_HEADER_MAGIC.len()];

            if let Err(e) = self.inner.read_exact(&mut buf) {
                return Some(Err(e.into()));
            };

            if buf == METADATA_HEADER_MAGIC {
                self.is_terminated = true;
                return None;
            }

            if buf != BLOB_HEADER_MAGIC {
                return Some(Err(crate::Error::Deserialize(
                    crate::serde::DeserializeError::InvalidHeader("Blob"),
                )));
            }
        }

        let key_len = match self.inner.read_u16::<BigEndian>() {
            Ok(v) => v,
            Err(e) => {
                if e.kind() == std::io::ErrorKind::UnexpectedEof {
                    return None;
                }
                return Some(Err(e.into()));
            }
        };

        let mut key = vec![0; key_len.into()];
        if let Err(e) = self.inner.read_exact(&mut key) {
            return Some(Err(e.into()));
        };

        // TODO: handle crc
        let _crc = match self.inner.read_u32::<BigEndian>() {
            Ok(v) => v,
            Err(e) => {
                if e.kind() == std::io::ErrorKind::UnexpectedEof {
                    return None;
                }
                return Some(Err(e.into()));
            }
        };

        let val_len = match self.inner.read_u32::<BigEndian>() {
            Ok(v) => v,
            Err(e) => {
                if e.kind() == std::io::ErrorKind::UnexpectedEof {
                    return None;
                }
                return Some(Err(e.into()));
            }
        };

        let mut val = vec![0; val_len as usize];
        if let Err(e) = self.inner.read_exact(&mut val) {
            return Some(Err(e.into()));
        };

        Some(Ok((key.into(), val.into())))
    }
}
