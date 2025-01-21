// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use byteview::ByteView;
use std::sync::Arc;

/// An immutable byte slice that can be cloned without additional heap allocation
#[derive(Debug, Clone, Eq, Hash, Ord)]
pub struct Slice(pub(super) ByteView);

impl Slice {
    /// Construct a [`Slice`] from a byte slice.
    #[must_use]
    pub fn new(bytes: &[u8]) -> Self {
        Self(bytes.into())
    }

    #[doc(hidden)]
    #[must_use]
    pub fn slice(&self, range: impl std::ops::RangeBounds<usize>) -> Self {
        Self(self.0.slice(range))
    }

    #[must_use]
    #[doc(hidden)]
    pub fn with_size(len: usize) -> Self {
        Self(ByteView::with_size(len))
    }

    #[doc(hidden)]
    pub fn from_reader<R: std::io::Read>(reader: &mut R, len: usize) -> std::io::Result<Self> {
        let view = ByteView::from_reader(reader, len)?;
        Ok(Self(view))
    }
}

// Arc::from<Vec<T>> is specialized
impl From<Vec<u8>> for Slice {
    fn from(value: Vec<u8>) -> Self {
        Self(ByteView::from(value))
    }
}

// Arc::from<Vec<T>> is specialized
impl From<String> for Slice {
    fn from(value: String) -> Self {
        Self(ByteView::from(value.into_bytes()))
    }
}

// direct conversion
impl From<Arc<[u8]>> for Slice {
    fn from(value: Arc<[u8]>) -> Self {
        Self::from(&*value)
    }
}
