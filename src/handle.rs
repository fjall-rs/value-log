use serde::{Deserialize, Serialize};
use std::hash::Hash;
use std::sync::Arc;

/// A value handle points into the value log.
#[allow(clippy::module_name_repetitions)]
#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub struct ValueHandle {
    /// Segment ID
    pub segment_id: Arc<str>,

    /// Offset in file
    pub offset: u64,
}
