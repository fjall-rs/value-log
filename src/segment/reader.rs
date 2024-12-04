// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use super::{meta::METADATA_HEADER_MAGIC, writer::BLOB_HEADER_MAGIC};
use crate::{coding::DecodeError, id::SegmentId, value::UserKey, Compressor, UserValue};
use byteorder::{BigEndian, ReadBytesExt};
use std::{
    fs::File,
    io::{BufReader, Read, Seek},
    path::Path,
};

/// Reads through a segment in order.
pub struct Reader<C: Compressor + Clone> {
    pub(crate) segment_id: SegmentId,
    inner: BufReader<File>,
    is_terminated: bool,
    compression: Option<C>,
}

impl<C: Compressor + Clone> Reader<C> {
    /// Initializes a new segment reader.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn new<P: AsRef<Path>>(path: P, segment_id: SegmentId) -> crate::Result<Self> {
        let file_reader = BufReader::new(File::open(path)?);

        Ok(Self::with_reader(segment_id, file_reader))
    }

    pub(crate) fn get_offset(&mut self) -> std::io::Result<u64> {
        self.inner.stream_position()
    }

    /// Initializes a new segment reader.
    #[must_use]
    pub fn with_reader(segment_id: SegmentId, file_reader: BufReader<File>) -> Self {
        Self {
            segment_id,
            inner: file_reader,
            is_terminated: false,
            compression: None,
        }
    }

    pub(crate) fn use_compression(mut self, compressor: C) -> Self {
        self.compression = Some(compressor);
        self
    }
}

impl<C: Compressor + Clone> Iterator for Reader<C> {
    type Item = crate::Result<(UserKey, UserValue, u64)>;

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
                return Some(Err(crate::Error::Decode(DecodeError::InvalidHeader(
                    "Blob",
                ))));
            }
        }

        let checksum = match self.inner.read_u64::<BigEndian>() {
            Ok(v) => v,
            Err(e) => {
                if e.kind() == std::io::ErrorKind::UnexpectedEof {
                    return None;
                }
                return Some(Err(e.into()));
            }
        };

        let key_len = match self.inner.read_u16::<BigEndian>() {
            Ok(v) => v,
            Err(e) => {
                if e.kind() == std::io::ErrorKind::UnexpectedEof {
                    return None;
                }
                return Some(Err(e.into()));
            }
        };

        // TODO: optimize using Slice::from_reader
        let mut key = vec![0; key_len.into()];
        if let Err(e) = self.inner.read_exact(&mut key) {
            return Some(Err(e.into()));
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

        // TODO: optimize using Slice::from_reader
        let mut val = vec![0; val_len as usize];
        if let Err(e) = self.inner.read_exact(&mut val) {
            return Some(Err(e.into()));
        };

        let val = match &self.compression {
            Some(compressor) => match compressor.decompress(&val) {
                Ok(val) => val,
                Err(e) => return Some(Err(e)),
            },
            None => val,
        };

        Some(Ok((key.into(), val.into(), checksum)))
    }
}
