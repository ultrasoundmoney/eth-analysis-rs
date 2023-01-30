use super::BlockNumber;

#[derive(Clone)]
pub struct BlockRange {
    pub greater_than_or_equal: BlockNumber,
    pub less_than_or_equal: BlockNumber,
}

impl BlockRange {
    pub fn new(greater_than_or_equal: BlockNumber, less_than_or_equal: BlockNumber) -> Self {
        if greater_than_or_equal > less_than_or_equal {
            panic!("tried to create slot range with negative range")
        }

        Self {
            greater_than_or_equal,
            less_than_or_equal,
        }
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
        match (self.block_range.greater_than_or_equal + self.index as BlockNumber)
            .cmp(&(self.block_range.less_than_or_equal))
        {
            Ordering::Less => {
                let current = self.block_range.greater_than_or_equal + self.index as BlockNumber;
                self.index += 1;
                Some(current)
            }
            Ordering::Equal => {
                let current = self.block_range.greater_than_or_equal + self.index as BlockNumber;
                self.index += 1;
                Some(current)
            }
            Ordering::Greater => None,
        }
    }
}
