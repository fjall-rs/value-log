// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{
    serde::{Deserializable, DeserializeError, Serializable, SerializeError},
    value::UserKey,
    Slice,
};
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use std::{
    io::{Read, Write},
    ops::Deref,
};

/// A key range in the format of [min, max] (inclusive on both sides)
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
pub struct KeyRange((UserKey, UserKey));

impl std::ops::Deref for KeyRange {
    type Target = (UserKey, UserKey);

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl KeyRange {
    #[must_use]
    /// Creates a new key range
    pub fn new(range: (UserKey, UserKey)) -> Self {
        Self(range)
    }
}

impl Serializable for KeyRange {
    fn serialize<W: Write>(&self, writer: &mut W) -> Result<(), SerializeError> {
        // NOTE: Max key size = u16
        #[allow(clippy::cast_possible_truncation)]
        writer.write_u16::<BigEndian>(self.deref().0.len() as u16)?;
        writer.write_all(&self.deref().0)?;

        // NOTE: Max key size = u16
        #[allow(clippy::cast_possible_truncation)]
        writer.write_u16::<BigEndian>(self.deref().1.len() as u16)?;
        writer.write_all(&self.deref().1)?;

        Ok(())
    }
}

impl Deserializable for KeyRange {
    fn deserialize<R: Read>(reader: &mut R) -> Result<Self, DeserializeError> {
        let key_min_len = reader.read_u16::<BigEndian>()?;
        let mut key_min = vec![0; key_min_len.into()];
        reader.read_exact(&mut key_min)?;
        let key_min: UserKey = Slice::from(key_min);

        let key_max_len = reader.read_u16::<BigEndian>()?;
        let mut key_max = vec![0; key_max_len.into()];
        reader.read_exact(&mut key_max)?;
        let key_max: UserKey = Slice::from(key_max);

        Ok(Self::new((key_min, key_max)))
    }
}
