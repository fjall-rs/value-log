// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use bytes::{Bytes, BytesMut};
use std::sync::Arc;

/// An immutable byte slice that can be cloned without additional heap allocation
#[derive(Debug, Clone, Eq, Hash, Ord)]
pub struct Slice(pub(super) Bytes);

impl Slice {
    /// Construct a [`Slice`] from a byte slice.
    #[must_use]
    pub fn new(bytes: &[u8]) -> Self {
        Self(Bytes::copy_from_slice(bytes))
    }

    #[doc(hidden)]
    pub fn from_reader<R: std::io::Read>(reader: &mut R, len: usize) -> std::io::Result<Self> {
        let mut builder = BytesMut::zeroed(len);
        reader.read_exact(&mut builder)?;
        Ok(builder.freeze().into())
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

// Needed because slice_arc specializes this impl
impl From<Arc<[u8]>> for Slice {
    fn from(value: Arc<[u8]>) -> Self {
        Self::new(value.as_ref())
    }
}
