use std::fmt::{Display, Formatter};

use crate::{
    execution_chain::{LONDON_HARD_FORK_BLOCK_NUMBER, MERGE_BLOCK_NUMBER},
    time_frames::{GrowingTimeFrame, TimeFrame},
};

use super::BlockNumber;

/// A range of blocks. The range is inclusive of both the first and last.
#[derive(Debug, Clone)]
pub struct BlockRange {
    pub start: BlockNumber,
    pub end: BlockNumber,
}

impl BlockRange {
    pub fn new(first: BlockNumber, last: BlockNumber) -> Self {
        if first > last {
            panic!("tried to create negative block range")
        }

        Self {
            start: first,
            end: last,
        }
    }

    pub fn from_last_plus_time_frame(last: &BlockNumber, time_frame: &TimeFrame) -> Self {
        use GrowingTimeFrame::*;
        use TimeFrame::*;

        match time_frame {
            Limited(limited_time_frame) => {
                let first = last - limited_time_frame.slot_count() as i32 + 1;
                Self {
                    start: first,
                    end: *last,
                }
            }
            Growing(SinceMerge) => Self {
                start: MERGE_BLOCK_NUMBER,
                end: *last,
            },
            Growing(SinceBurn) => Self {
                start: LONDON_HARD_FORK_BLOCK_NUMBER,
                end: *last,
            },
        }
    }
}

impl Display for BlockRange {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}-{}", self.start, self.end)
    }
}

pub struct BlockRangeIntoIterator {
    block_range: BlockRange,
    index: usize,
}

impl IntoIterator for BlockRange {
    type Item = BlockNumber;
    type IntoIter = BlockRangeIntoIterator;

    fn into_iter(self) -> Self::IntoIter {
        Self::IntoIter {
            block_range: self,
            index: 0,
        }
    }
}

impl Iterator for BlockRangeIntoIterator {
    type Item = BlockNumber;

    fn next(&mut self) -> Option<Self::Item> {
        if self.block_range.start + self.index as BlockNumber <= self.block_range.end {
            let block_number: BlockNumber = self.block_range.start + self.index as BlockNumber;
            self.index += 1;
            Some(block_number)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::time_frames::LimitedTimeFrame;

    use super::*;

    impl BlockRange {
        pub fn count(&self) -> usize {
            (self.end - self.start + 1) as usize
        }
    }

    #[test]
    fn block_range_iterable_test() {
        let range = (BlockRange::new(1, 4))
            .into_iter()
            .collect::<Vec<BlockNumber>>();
        assert_eq!(range, vec![1, 2, 3, 4]);
    }

    #[test]
    fn block_range_from_time_frame_test() {
        // For a 5 minute time frame with a 12 second block time there should be at any point 25
        // blocks within the time frame.
        // time:  t0, t1, ~ t0+5min, t1+5min
        // block: 1,  2,  ~ 26,      27
        let first_outside_before = 1;
        let first_inside = 2;
        let last_inside = 26;
        let first_outside_after = 27;

        let block_range = BlockRange::from_last_plus_time_frame(
            &last_inside,
            &TimeFrame::Limited(LimitedTimeFrame::Minute5),
        );

        assert_eq!(block_range.count(), 25);
        assert!(block_range.start > first_outside_before);
        assert!(block_range.start <= first_inside);
        assert!(block_range.end >= last_inside);
        assert!(block_range.end < first_outside_after);
    }
}
