use crate::id::SegmentId;
use std::hash::Hash;

/// A value handle points into the value log
#[allow(clippy::module_name_repetitions)]
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
pub struct ValueHandle {
    /// Segment ID
    pub segment_id: SegmentId,

    /// Offset in file
    pub offset: u64,
}
