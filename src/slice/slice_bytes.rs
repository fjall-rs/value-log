// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use bytes::{BufMut, Bytes, BytesMut};
use std::io::Read;

/// An immutable byte slice that can be cloned without additional heap allocation
///
/// There is no guarantee of any sort of alignment for zero-copy (de)serialization.
#[derive(Debug, Clone, Eq, Hash, Ord)]
pub struct Slice(pub(super) Bytes);

impl Slice {
    /// Construct a [`Slice`] from a byte slice.
    #[must_use]
    pub fn new(bytes: &[u8]) -> Self {
        Self(Bytes::copy_from_slice(bytes))
    }

    #[doc(hidden)]
    #[must_use]
    pub fn empty() -> Self {
        Self(Bytes::from_static(&[]))
    }

    #[doc(hidden)]
    #[must_use]
    pub fn slice(&self, range: impl std::ops::RangeBounds<usize>) -> Self {
        Self(self.0.slice(range))
    }

    #[must_use]
    #[doc(hidden)]
    pub fn with_size(len: usize) -> Self {
        let bytes = vec![0; len];
        Self(Bytes::from(bytes))
    }

    /// Constructs a [`Slice`] from an I/O reader by pulling in `len` bytes.
    #[doc(hidden)]
    pub fn from_reader<R: std::io::Read>(reader: &mut R, len: usize) -> std::io::Result<Self> {
        // Use `BytesMut::with_capacity` + `BytesMut::writer` in order to skip
        // zeroing out the buffer before reading
        let mut writer = BytesMut::with_capacity(len).writer();
        let mut taker = reader.take(len as u64);

        let n = std::io::copy(&mut taker, &mut writer)?;
        if n != len as u64 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "failed to read enough bytes",
            ));
        }

        Ok(Self(writer.into_inner().freeze()))

        // ALTERNATIVE unsafe version:

        // let mut builder = BytesMut::with_capacity(len);

        // // SAFETY: we just allocated `len` bytes, and `read_exact` will fail if it
        // // doesn't fill the buffer
        // unsafe {
        //     builder.set_len(len);
        // }

        // reader.read_exact(&mut builder)?;

        // Ok(Self(builder.freeze()))
    }
}

impl From<Bytes> for Slice {
    fn from(value: Bytes) -> Self {
        Self(value)
    }
}

impl From<Slice> for Bytes {
    fn from(value: Slice) -> Self {
        value.0
    }
}

// Bytes::from<Vec<u8>> is zero-copy optimized
impl From<Vec<u8>> for Slice {
    fn from(value: Vec<u8>) -> Self {
        Self(Bytes::from(value))
    }
}

// Bytes::from<String> is zero-copy optimized
impl From<String> for Slice {
    fn from(value: String) -> Self {
        Self(Bytes::from(value))
    }
}
