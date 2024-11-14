// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use std::sync::Arc;

use bytes::{BufMut, Bytes, BytesMut};

/// An immutable byte slice that can be cloned without additional heap allocation
#[derive(Debug, Clone, Eq, Hash, Ord)]
pub struct Slice(Bytes);

impl Slice {
    /// Construct a [`Slice`] from a byte slice.
    #[must_use]
    pub fn new(bytes: &[u8]) -> Self {
        Self::from(bytes)
    }

    #[doc(hidden)]
    pub fn from_reader<R: std::io::Read>(reader: &mut R, len: usize) -> std::io::Result<Self> {
        let mut builder = BytesMut::zeroed(len);
        reader.read_exact(&mut builder)?;
        Ok(builder.freeze().into())
    }
}

impl AsRef<[u8]> for Slice {
    fn as_ref(&self) -> &[u8] {
        &self.0
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

impl From<&[u8]> for Slice {
    fn from(value: &[u8]) -> Self {
        Self(Bytes::copy_from_slice(value))
    }
}

impl From<Arc<[u8]>> for Slice {
    fn from(value: Arc<[u8]>) -> Self {
        value.as_ref().into()
    }
}

impl From<Vec<u8>> for Slice {
    fn from(value: Vec<u8>) -> Self {
        Self(value.into())
    }
}

impl From<&str> for Slice {
    fn from(value: &str) -> Self {
        Self::from(value.as_bytes())
    }
}

impl From<String> for Slice {
    fn from(value: String) -> Self {
        Self(value.into())
    }
}

impl From<Arc<str>> for Slice {
    fn from(value: Arc<str>) -> Self {
        Self::from(&*value)
    }
}

impl<const N: usize> From<[u8; N]> for Slice {
    fn from(value: [u8; N]) -> Self {
        Self::from(value.as_slice())
    }
}

impl FromIterator<u8> for Slice {
    fn from_iter<T>(iter: T) -> Self
    where
        T: IntoIterator<Item = u8>,
    {
        Self(Bytes::from_iter(iter))
    }
}

impl std::ops::Deref for Slice {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::borrow::Borrow<[u8]> for Slice {
    fn borrow(&self) -> &[u8] {
        self
    }
}

impl<T> PartialEq<T> for Slice
where
    T: AsRef<[u8]>,
{
    fn eq(&self, other: &T) -> bool {
        self.as_ref() == other.as_ref()
    }
}

impl PartialEq<Slice> for &[u8] {
    fn eq(&self, other: &Slice) -> bool {
        *self == other.as_ref()
    }
}

impl<T> PartialOrd<T> for Slice
where
    T: AsRef<[u8]>,
{
    fn partial_cmp(&self, other: &T) -> Option<std::cmp::Ordering> {
        self.as_ref().partial_cmp(other.as_ref())
    }
}

impl PartialOrd<Slice> for &[u8] {
    fn partial_cmp(&self, other: &Slice) -> Option<std::cmp::Ordering> {
        (*self).partial_cmp(other.as_ref())
    }
}
