// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use std::sync::Arc;

/// An immutable byte slice that can be cloned without additional heap allocation
#[derive(Debug, Clone, Eq, Hash, Ord)]
pub struct Slice(pub(super) Arc<[u8]>);

impl Slice {
    /// Construct a [`Slice`] from a byte slice.
    #[must_use]
    pub fn new(bytes: &[u8]) -> Self {
        Self(bytes.into())
    }

    #[must_use]
    #[doc(hidden)]
    pub fn with_size(len: usize) -> Self {
        // TODO: optimize this with byteview to remove the reallocation
        let v = vec![0; len];
        Self(v.into())
    }

    #[doc(hidden)]
    pub fn from_reader<R: std::io::Read>(reader: &mut R, len: usize) -> std::io::Result<Self> {
        let mut view = Self::with_size(len);
        let builder = Arc::get_mut(&mut view.0).expect("we are the owner");
        reader.read_exact(builder)?;
        Ok(view)
    }
}

// Arc::from<Vec<T>> is specialized
impl From<Vec<u8>> for Slice {
    fn from(value: Vec<u8>) -> Self {
        Self(Arc::from(value))
    }
}

// Arc::from<Vec<T>> is specialized
impl From<String> for Slice {
    fn from(value: String) -> Self {
        Self(Arc::from(value.into_bytes()))
    }
}

// direct conversion
impl From<Arc<[u8]>> for Slice {
    fn from(value: Arc<[u8]>) -> Self {
        Self(value)
    }
}
