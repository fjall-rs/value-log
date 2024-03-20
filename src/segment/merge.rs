use crate::{id::SegmentId, SegmentReader};
use std::cmp::Reverse;

// TODO: replace with MinHeap
use min_max_heap::MinMaxHeap;

type IteratorIndex = usize;

#[derive(Debug)]
struct IteratorValue {
    index: IteratorIndex,
    key: Vec<u8>,
    value: Vec<u8>,
    segment_id: SegmentId,
}

impl PartialEq for IteratorValue {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
    }
}
impl Eq for IteratorValue {}

impl PartialOrd for IteratorValue {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some((&self.key, Reverse(&self.segment_id)).cmp(&(&other.key, Reverse(&other.segment_id))))
    }
}

impl Ord for IteratorValue {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (&self.key, Reverse(&self.segment_id)).cmp(&(&other.key, Reverse(&other.segment_id)))
    }
}

/// Interleaves multiple segment readers into a single, sorted stream
#[allow(clippy::module_name_repetitions)]
pub struct MergeReader {
    readers: Vec<SegmentReader>,
    heap: MinMaxHeap<IteratorValue>,
}

impl MergeReader {
    /// Initializes a new merging reader
    pub fn new(readers: Vec<SegmentReader>) -> Self {
        Self {
            readers,
            heap: MinMaxHeap::new(),
        }
    }

    fn advance_reader(&mut self, idx: usize) -> crate::Result<()> {
        let reader = self.readers.get_mut(idx).expect("iter should exist");

        if let Some(value) = reader.next() {
            let (k, v) = value?;
            let segment_id = reader.segment_id;

            self.heap.push(IteratorValue {
                index: idx,
                key: k,
                value: v,
                segment_id,
            });
        }

        Ok(())
    }

    fn push_next(&mut self) -> crate::Result<()> {
        for idx in 0..self.readers.len() {
            self.advance_reader(idx)?;
        }

        Ok(())
    }
}

impl Iterator for MergeReader {
    type Item = crate::Result<(Vec<u8>, Vec<u8>, SegmentId)>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.heap.is_empty() {
            if let Err(e) = self.push_next() {
                return Some(Err(e));
            };
        }

        if let Some(head) = self.heap.pop_min() {
            if let Err(e) = self.advance_reader(head.index) {
                return Some(Err(e));
            }

            // Discard old items
            while let Some(next) = self.heap.pop_min() {
                if next.key == head.key {
                    if let Err(e) = self.advance_reader(next.index) {
                        return Some(Err(e));
                    }
                } else {
                    // Reached next user key now
                    // Push back non-conflicting item and exit
                    self.heap.push(next);
                    break;
                }
            }

            return Some(Ok((head.key, head.value, head.segment_id)));
        }

        None
    }
}
