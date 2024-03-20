use crate::ValueHandle;

/// External index trait
///
/// An index should point into the value log using [`ValueHandle`].
pub trait Index {
    /// Returns a value handle for a given key.
    ///
    /// This method is used to index back into the index to check for
    /// stale values when scanning through the value log's segments.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    fn get(&self, key: &[u8]) -> std::io::Result<Option<ValueHandle>>;
}

/// Trait that allows writing into an external index
///
/// The write process should be atomic.
pub trait Writer {
    /// Inserts a value handle into the index.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    fn insert_indirection(&self, key: &[u8], value: ValueHandle) -> std::io::Result<()>;

    /// Finishes the write batch.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    fn finish(&self) -> std::io::Result<()>;
}
