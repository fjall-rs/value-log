use crate::{
    compression::{CompressError, DecompressError},
    serde::{DeserializeError, SerializeError},
    version::Version,
};

/// Represents errors that can occur in the value log
#[derive(Debug)]
pub enum Error {
    /// I/O error
    Io(std::io::Error),

    /// Invalid data format version
    InvalidVersion(Option<Version>),

    /// Serialization failed
    Serialize(SerializeError),

    /// Deserialization failed
    Deserialize(DeserializeError),

    /// Compression failed
    Compress(CompressError),

    /// Decompression failed
    Decompress(DecompressError),
    // TODO:
    // /// Checksum check failed
    // ChecksumMismatch,
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ValueLogError: {self:?}")
    }
}

impl std::error::Error for Error {}

impl From<std::io::Error> for Error {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

impl From<SerializeError> for Error {
    fn from(value: SerializeError) -> Self {
        Self::Serialize(value)
    }
}

impl From<DeserializeError> for Error {
    fn from(value: DeserializeError) -> Self {
        Self::Deserialize(value)
    }
}

impl From<CompressError> for Error {
    fn from(value: CompressError) -> Self {
        Self::Compress(value)
    }
}

impl From<DecompressError> for Error {
    fn from(value: DecompressError) -> Self {
        Self::Decompress(value)
    }
}

/// Value log result
pub type Result<T> = std::result::Result<T, Error>;
