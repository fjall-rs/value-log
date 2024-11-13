// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{
    coding::{Decode, DecodeError, Encode, EncodeError},
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

impl Encode for KeyRange {
    fn encode_into<W: Write>(&self, writer: &mut W) -> Result<(), EncodeError> {
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

impl Decode for KeyRange {
    fn decode_from<R: Read>(reader: &mut R) -> Result<Self, DecodeError> {
        let key_min_len = reader.read_u16::<BigEndian>()?;
        let key_min: UserKey = Slice::from_reader(reader, key_min_len.into())?;

        let key_max_len = reader.read_u16::<BigEndian>()?;
        let key_max: UserKey = Slice::from_reader(reader, key_max_len.into())?;

        Ok(Self::new((key_min, key_max)))
    }
}
