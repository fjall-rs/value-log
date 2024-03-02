/// Value log configuration
#[derive(Debug)]
pub struct Config {
    pub(crate) segment_size_bytes: u64,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            segment_size_bytes: 256 * 1_024 * 1_024,
        }
    }
}

impl Config {
    /// Sets the maximum size of value log segments.
    ///
    /// This heavily influences space amplification, as
    /// space reclamation works on a per-segment basis.
    ///
    /// Like `blob_file_size` in `RocksDB`.
    ///
    /// Default = 256 MiB
    #[must_use]
    pub fn segment_size_bytes(mut self, bytes: u64) -> Self {
        self.segment_size_bytes = bytes;
        self
    }
}
