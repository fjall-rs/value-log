// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

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
