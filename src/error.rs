use crate::version::Version;

/// Represents errors that can occur in the value-log
#[derive(Debug)]
pub enum Error {
    /// I/O error
    Io(std::io::Error),

    /// Invalid data format version
    InvalidVersion(Option<Version>),

    /// CRC check failed
    CrcMismatch,
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

/// Tree result
pub type Result<T> = std::result::Result<T, Error>;
