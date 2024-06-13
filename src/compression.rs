/// Compression error
#[derive(Debug)]
pub struct CompressError(pub String);

/// Decompression error
#[derive(Debug)]
pub struct DecompressError(pub String);

/// Generic compression trait
pub trait Compressor {
    /// Compresses a value
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    fn compress(&self, bytes: &[u8]) -> Result<Vec<u8>, CompressError>;

    /// Decompresses a value
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    fn decompress(&self, bytes: &[u8]) -> Result<Vec<u8>, DecompressError>;
}
