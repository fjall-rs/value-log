use std::sync::atomic::AtomicU64;

#[derive(Debug, Default)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
pub struct Stats {
    pub(crate) item_count: AtomicU64,
    pub(crate) stale_items: AtomicU64,

    pub total_bytes: AtomicU64,
    pub(crate) stale_bytes: AtomicU64,
    // TODO: key range
}

impl Stats {
    pub(crate) fn mark_as_stale(&self) {
        self.stale_items
            .store(self.item_count(), std::sync::atomic::Ordering::Release);

        self.stale_bytes
            .store(self.total_bytes(), std::sync::atomic::Ordering::Release);
    }

    pub fn item_count(&self) -> u64 {
        self.item_count.load(std::sync::atomic::Ordering::Acquire)
    }

    pub fn total_bytes(&self) -> u64 {
        self.total_bytes.load(std::sync::atomic::Ordering::Acquire)
    }

    pub fn is_stale(&self) -> bool {
        self.stale_items() == self.item_count()
    }

    /// Returns the percent of dead items in the segment
    pub fn stale_ratio(&self) -> f32 {
        let dead = self.stale_items() as f32;
        if dead == 0.0 {
            return 0.0;
        }

        dead / self.item_count() as f32
    }

    /// Returns the amount of dead items in the segment
    ///
    /// This value may not be fresh, as it is only set after running [`ValueLog::refresh_stats`].
    pub fn stale_items(&self) -> u64 {
        self.stale_items.load(std::sync::atomic::Ordering::Acquire)
    }

    /// Returns the amount of dead bytes in the segment
    ///
    /// This value may not be fresh, as it is only set after running [`ValueLog::refresh_stats`].
    pub fn stale_bytes(&self) -> u64 {
        self.stale_bytes.load(std::sync::atomic::Ordering::Acquire)
    }
}
