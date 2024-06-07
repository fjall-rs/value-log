use std::sync::Arc;

/// User defined key
pub type UserKey = Arc<[u8]>;

/// User defined data (blob of bytes)
#[allow(clippy::module_name_repetitions)]
pub type UserValue = Arc<[u8]>;
