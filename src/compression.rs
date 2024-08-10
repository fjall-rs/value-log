/// Generic compression trait
pub trait Compressor {
    /// Compresses a value
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    fn compress(&self, bytes: &[u8]) -> crate::Result<Vec<u8>>;

    /// Decompresses a value
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    fn decompress(&self, bytes: &[u8]) -> crate::Result<Vec<u8>>;
}
