// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use byteview::ByteView;
use std::{hash::Hash, sync::Arc};

/// An immutable byte slice that can be cloned without additional heap allocation
#[derive(Debug, Clone, PartialEq, Eq, Hash, Ord, PartialOrd)]
pub struct Slice(ByteView);

impl Slice {
    /// Construct a [`Slice`] from a byte slice.
    #[must_use]
    pub fn new(bytes: &[u8]) -> Self {
        Self::from(bytes)
    }

    #[doc(hidden)]
    #[must_use]
    pub fn with_size(len: usize) -> Self {
        Self(ByteView::with_size(len))
    }

    #[doc(hidden)]
    pub fn from_reader<R: std::io::Read>(reader: &mut R, len: usize) -> std::io::Result<Self> {
        use std::ops::DerefMut;

        let mut view = Self::with_size(len);
        let mut builder = view.0.get_mut().expect("we are the owner");
        reader.read_exact(builder.deref_mut())?;

        Ok(view)
    }

    #[doc(hidden)]
    #[must_use]
    pub fn slice(&self, range: impl std::ops::RangeBounds<usize>) -> Self {
        Self(self.0.slice(range))
    }
}

impl std::borrow::Borrow<[u8]> for Slice {
    fn borrow(&self) -> &[u8] {
        self
    }
}

impl std::ops::Deref for Slice {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl PartialEq<[u8]> for Slice {
    fn eq(&self, other: &[u8]) -> bool {
        self.0.as_ref() == other
    }
}

impl PartialEq<Slice> for &[u8] {
    fn eq(&self, other: &Slice) -> bool {
        *self == other.0.as_ref()
    }
}

impl PartialOrd<[u8]> for Slice {
    fn partial_cmp(&self, other: &[u8]) -> Option<std::cmp::Ordering> {
        self.0.as_ref().partial_cmp(other)
    }
}

impl PartialOrd<Slice> for &[u8] {
    fn partial_cmp(&self, other: &Slice) -> Option<std::cmp::Ordering> {
        self.partial_cmp(&other.0.as_ref())
    }
}

impl FromIterator<u8> for Slice {
    fn from_iter<T>(iter: T) -> Self
    where
        T: IntoIterator<Item = u8>,
    {
        Self::from(iter.into_iter().collect::<Vec<u8>>())
    }
}

impl AsRef<[u8]> for Slice {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl From<&[u8]> for Slice {
    fn from(value: &[u8]) -> Self {
        Self(value.into())
    }
}

impl From<Arc<[u8]>> for Slice {
    fn from(value: Arc<[u8]>) -> Self {
        Self(ByteView::from(value))
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
        Self::from(value.as_bytes())
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

#[cfg(feature = "serde")]
mod serde {
    use super::Slice;
    use serde::de::{self, Visitor};
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use std::fmt;
    use std::ops::Deref;

    impl Serialize for Slice {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            serializer.serialize_bytes(self.deref())
        }
    }

    impl<'de> Deserialize<'de> for Slice {
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: Deserializer<'de>,
        {
            struct SliceVisitor;

            impl<'de> Visitor<'de> for SliceVisitor {
                type Value = Slice;

                fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                    formatter.write_str("a byte array")
                }

                fn visit_bytes<E>(self, v: &[u8]) -> Result<Slice, E>
                where
                    E: de::Error,
                {
                    Ok(Slice::from(v))
                }
            }

            deserializer.deserialize_bytes(SliceVisitor)
        }
    }
}
