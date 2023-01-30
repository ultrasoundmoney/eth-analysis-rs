use super::BlockNumber;

/// A range of blocks. The range is inclusive of both the lowest and highest.
#[derive(Clone)]
pub struct BlockRange {
    pub lowest: BlockNumber,
    pub highest: BlockNumber,
}

impl BlockRange {
    pub fn new(lowest: BlockNumber, highest: BlockNumber) -> Self {
        if lowest > highest {
            panic!("tried to create negative block range")
        }

        Self { lowest, highest }
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
        if self.block_range.lowest + self.index as BlockNumber <= self.block_range.highest {
            let block_number: BlockNumber = self.block_range.lowest + self.index as BlockNumber;
            self.index += 1;
            Some(block_number)
        } else {
            None
        }
    }
}
