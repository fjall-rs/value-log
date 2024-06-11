use crate::ValueHandle;

/// Trait that allows reading from an external index
///
/// An index should point into the value log using [`ValueHandle`].
#[allow(clippy::module_name_repetitions)]
pub trait Reader {
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
/// The write process should be atomic meaning that until `finish` is called
/// no written value handles should be handed out by the index.
/// When `finish` fails, no value handles should be written into the index.
pub trait Writer {
    /// Inserts a value handle into the write batch.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    fn insert_indirection(
        &mut self,
        key: &[u8],
        handle: ValueHandle,
        size: u32,
    ) -> std::io::Result<()>;

    /// Finishes the write batch.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    fn finish(self) -> std::io::Result<()>;
}
