use std::fmt::{Display, Formatter};

use crate::{
    execution_chain::{LONDON_HARD_FORK_BLOCK_NUMBER, MERGE_BLOCK_NUMBER},
    time_frames::{GrowingTimeFrame, TimeFrame},
};

use super::{block_store_next::BlockStore, BlockNumber, ExecutionNodeBlock};

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

    pub async fn from_block_and_time_frame(
        block_store: &impl BlockStore,
        block: &ExecutionNodeBlock,
        time_frame: &TimeFrame,
    ) -> Option<Self> {
        use GrowingTimeFrame::*;
        use TimeFrame::*;

        let range = match time_frame {
            Limited(limited_time_frame) => {
                let time_barrier = block.timestamp - limited_time_frame.duration();
                let first = block_store.first_number_after_or_at(&time_barrier).await?;
                Self {
                    start: first,
                    end: block.number,
                }
            }
            Growing(SinceMerge) => Self {
                start: MERGE_BLOCK_NUMBER,
                end: block.number,
            },
            Growing(SinceBurn) => Self {
                start: LONDON_HARD_FORK_BLOCK_NUMBER,
                end: block.number,
            },
        };

        Some(range)
    }

    /// Estimate the block range based on a block and time frame.
    ///
    /// We assume zero missed blocks, which is not a safe assumption, and means the start number
    /// will be too early by as many blocks as slots have been missed.
    #[allow(dead_code)]
    pub fn estimate_from_block_and_time_frame(
        block: &ExecutionNodeBlock,
        time_frame: &TimeFrame,
    ) -> Self {
        use GrowingTimeFrame::*;
        use TimeFrame::*;

        match time_frame {
            Limited(limited_time_frame) => {
                let first = block.number - limited_time_frame.slot_count() as i32 + 1;
                Self {
                    start: first,
                    end: block.number,
                }
            }
            Growing(SinceMerge) => Self {
                start: MERGE_BLOCK_NUMBER,
                end: block.number,
            },
            Growing(SinceBurn) => Self {
                start: LONDON_HARD_FORK_BLOCK_NUMBER,
                end: block.number,
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
    use async_trait::async_trait;
    use chrono::{DateTime, Utc};

    use crate::{execution_chain::ExecutionNodeBlock, time_frames::LimitedTimeFrame};

    use super::*;

    impl BlockRange {
        pub fn count(&self) -> usize {
            (self.end - self.start + 1) as usize
        }
    }

    struct MockBlockStore {
        first_number_after_or_at: BlockNumber,
    }

    #[async_trait]
    impl BlockStore for MockBlockStore {
        async fn number_exists(&self, _number: &BlockNumber) -> bool {
            true
        }

        async fn first_number_after_or_at(
            &self,
            _timestamp: &DateTime<Utc>,
        ) -> Option<BlockNumber> {
            Some(self.first_number_after_or_at)
        }

        async fn hash_from_number(&self, number: &BlockNumber) -> Option<String> {
            Some(number.to_string())
        }

        async fn last(&self) -> ExecutionNodeBlock {
            ExecutionNodeBlock {
                base_fee_per_gas: 0,
                difficulty: 0,
                gas_used: 0,
                hash: "".to_string(),
                number: 27,
                parent_hash: "".to_string(),
                timestamp: Utc::now(),
                total_difficulty: 0,
                transactions: vec![],
                blob_gas_used: None,
                excess_blob_gas: None,
            }
        }
    }

    #[test]
    fn block_range_iterable_test() {
        let range = (BlockRange::new(1, 4))
            .into_iter()
            .collect::<Vec<BlockNumber>>();
        assert_eq!(range, vec![1, 2, 3, 4]);
    }

    #[tokio::test]
    async fn from_block_and_time_frame_test() {
        // For a 5 minute time frame with a 12 second block time there should be at any point 25
        // blocks within the time frame.
        // time:  t0, t1, ~ t0+5min, t1+5min
        // block: 1,  2,  ~ 26,      27
        let first_outside_before = 1;
        let first_inside = 2;
        let last_inside = 26;
        let first_outside_after = 27;

        let block_range = BlockRange::from_block_and_time_frame(
            &MockBlockStore {
                first_number_after_or_at: first_inside,
            },
            &ExecutionNodeBlock {
                base_fee_per_gas: 0,
                difficulty: 0,
                gas_used: 0,
                hash: "".to_string(),
                number: last_inside,
                parent_hash: "".to_string(),
                timestamp: Utc::now(),
                total_difficulty: 0,
                transactions: vec![],
                blob_gas_used: None,
                excess_blob_gas: None,
            },
            &TimeFrame::Limited(LimitedTimeFrame::Minute5),
        )
        .await
        .unwrap();

        assert_eq!(block_range.count(), 25);
        assert!(block_range.start > first_outside_before);
        assert!(block_range.start <= first_inside);
        assert!(block_range.end >= last_inside);
        assert!(block_range.end < first_outside_after);
    }

    #[test]
    fn estimate_from_block_and_time_frame_test() {
        // For a 5 minute time frame with a 12 second block time there should be at any point 25
        // blocks within the time frame.
        // time:  t0, t1, ~ t0+5min, t1+5min
        // block: 1,  2,  ~ 26,      27
        let first_outside_before = 1;
        let first_inside = 2;
        let last_inside = 26;
        let first_outside_after = 27;

        let block_range = BlockRange::estimate_from_block_and_time_frame(
            &ExecutionNodeBlock {
                base_fee_per_gas: 0,
                difficulty: 0,
                gas_used: 0,
                hash: "".to_string(),
                number: last_inside,
                parent_hash: "".to_string(),
                timestamp: Utc::now(),
                total_difficulty: 0,
                transactions: vec![],
                blob_gas_used: None,
                excess_blob_gas: None,
            },
            &TimeFrame::Limited(LimitedTimeFrame::Minute5),
        );

        assert_eq!(block_range.count(), 25);
        assert!(block_range.start > first_outside_before);
        assert!(block_range.start <= first_inside);
        assert!(block_range.end >= last_inside);
        assert!(block_range.end < first_outside_after);
    }
}
