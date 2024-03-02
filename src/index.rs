use crate::ValueHandle;

/// External index trait
///
/// An index should point into the value log using [`ValueHandle`].
pub trait Index {
    /// Returns a value habdle for a given key.
    ///
    /// This method is used to index back into the index to check for
    /// stale values when scanning through the value log's segments.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    fn get(&self, key: &[u8]) -> std::io::Result<Option<ValueHandle>>;

    /// Inserts an value handle into the index.
    ///
    /// This method is called during value log garbage collection.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    fn insert_indirection(&self, key: &[u8], value: ValueHandle) -> std::io::Result<()>;
}
