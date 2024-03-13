use serde::{Deserialize, Serialize};
use std::sync::atomic::AtomicU64;

#[derive(Debug, Deserialize, Serialize)]
pub struct Stats {
    pub(crate) item_count: u64,
    pub(crate) dead_items: AtomicU64,

    pub total_bytes: u64,
    pub(crate) dead_bytes: AtomicU64,
    // TODO: key range
}

impl Stats {
    /// Returns the percent of dead items in the segment
    pub fn dead_ratio(&self) -> f32 {
        let dead = self.get_dead_items() as f32;
        if dead == 0.0 {
            return 0.0;
        }

        dead / self.item_count as f32
    }

    /// Returns the amount of dead items in the segment
    ///
    /// This value may not be fresh, as it is only set after running [`ValueLog::refresh_stats`].
    pub fn get_dead_items(&self) -> u64 {
        self.dead_items.load(std::sync::atomic::Ordering::Acquire)
    }

    /// Returns the amount of dead bytes in the segment
    ///
    /// This value may not be fresh, as it is only set after running [`ValueLog::refresh_stats`].
    pub fn get_dead_bytes(&self) -> u64 {
        self.dead_bytes.load(std::sync::atomic::Ordering::Acquire)
    }
}
