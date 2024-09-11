// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use std::io::{Read, Write};

/// Error during serialization
#[derive(Debug)]
pub enum EncodeError {
    /// I/O error
    Io(std::io::Error),
}

/// Error during deserialization
#[derive(Debug)]
pub enum DecodeError {
    /// I/O error
    Io(std::io::Error),

    /// Invalid enum tag
    InvalidTag((&'static str, u8)),

    InvalidTrailer,

    /// Invalid block header
    InvalidHeader(&'static str),
}

impl From<std::io::Error> for EncodeError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

impl From<std::io::Error> for DecodeError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

/// Trait to serialize stuff
pub trait Encode {
    /// Serializes into writer.
    fn encode_into<W: Write>(&self, writer: &mut W) -> Result<(), EncodeError>;

    /// Serializes into vector.
    fn encode_into_vec(&self) -> Result<Vec<u8>, EncodeError> {
        let mut v = vec![];
        self.encode_into(&mut v)?;
        Ok(v)
    }
}

/// Trait to deserialize stuff
pub trait Decode {
    /// Deserializes from reader.
    fn decode_from<R: Read>(reader: &mut R) -> Result<Self, DecodeError>
    where
        Self: Sized;
}
