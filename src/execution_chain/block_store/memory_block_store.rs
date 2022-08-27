use std::collections::HashMap;

use async_trait::async_trait;

use crate::execution_chain::node::{BlockNumber, ExecutionNodeBlock};

use super::BlockStore;

pub struct MemoryBlockStore {
    blocks: Vec<ExecutionNodeBlock>,
    number_index: HashMap<u32, usize>,
    hash_index: HashMap<String, usize>,
}

const MAX_BLOCKS_STORED: usize = 100;

impl MemoryBlockStore {
    fn trim_blocks(&mut self) {
        if self.blocks.len() > MAX_BLOCKS_STORED {
            let over_capacity = self.blocks.len() - 100;
            for index_to_drop in 0..over_capacity {
                let block = self
                    .blocks
                    .get(index_to_drop)
                    .expect("over capacity blocks to exist when over capacity");
                self.hash_index.remove(&block.hash);
                self.number_index.remove(&block.number);
                self.blocks.remove(index_to_drop);
            }
        }
    }
}

#[async_trait]
impl BlockStore for MemoryBlockStore {
    fn new() -> Self {
        Self {
            blocks: Vec::new(),
            number_index: HashMap::new(),
            hash_index: HashMap::new(),
        }
    }

    async fn delete_block_by_number(&mut self, block_number: &BlockNumber) {
        let block = self
            .get_block_by_number(block_number)
            .await
            .expect("block to delete to exist");
        let hash = block.hash.clone();
        let number = block.number.clone();

        let index = self
            .hash_index
            .get(&block.hash)
            .expect("block in number index to exist in hash index");
        self.blocks.remove(*index);

        self.hash_index.remove(&hash);
        self.number_index.remove(&number);
    }

    async fn delete_blocks(&mut self, greater_than_or_equal: &BlockNumber) {
        let block = self.get_last_block_number().await;

        match block {
            None => {
                tracing::warn!(
                    "asked to delete blocks gte {greater_than_or_equal}, but no blocks are stored"
                )
            }
            Some(last_block_number) => {
                for block_number in (*greater_than_or_equal..=last_block_number).rev() {
                    self.delete_block_by_number(&block_number).await
                }
            }
        }
    }

    async fn get_block_by_number(&self, block_number: &BlockNumber) -> Option<&ExecutionNodeBlock> {
        self.number_index
            .get(block_number)
            .and_then(|index| self.blocks.get(*index))
    }

    async fn get_block_by_hash(&self, hash: &str) -> Option<&ExecutionNodeBlock> {
        self.hash_index
            .get(hash)
            .and_then(|index| self.blocks.get(*index))
    }

    async fn get_is_parent_hash_known(&self, hash: &str) -> bool {
        // Currently, we only store blocks in memory, therefore we don't have any parents on start,
        // and that's okay, if we have zero blocks, any parent is fine.
        if self.blocks.is_empty() {
            true
        } else {
            self.hash_index.contains_key(hash)
        }
    }

    async fn get_is_number_known(&self, block_number: &BlockNumber) -> bool {
        self.number_index.contains_key(block_number)
    }

    async fn store_block(&mut self, block: ExecutionNodeBlock) {
        if !self.is_empty().await && !self.hash_index.contains_key(&block.parent_hash) {
            panic!(
                "trying to store execution block with missing parent, block: {}, parent: {:?}",
                block.hash, block.parent_hash
            )
        }

        let index = self.len().await;
        self.number_index.insert(block.number, index);
        self.hash_index.insert(block.hash.clone(), index);
        self.blocks.insert(index, block);

        self.trim_blocks();
    }

    async fn get_last_block_number(&self) -> Option<BlockNumber> {
        self.blocks.last().map(|block| block.number)
    }

    async fn is_empty(&self) -> bool {
        self.blocks.is_empty()
    }

    async fn len(&self) -> usize {
        self.blocks.len()
    }
}

#[cfg(test)]
mod tests {
    use chrono::Utc;

    use super::*;

    #[tokio::test]
    async fn store_block_test() {
        let mut block_store = MemoryBlockStore::new();
        let test_block = ExecutionNodeBlock {
            hash: "0xtest".to_string(),
            number: 0,
            parent_hash: "0xparent".to_string(),
            timestamp: Utc::now(),
            difficulty: 0,
            total_difficulty: 0,
        };

        assert_eq!(block_store.len().await, 0);

        block_store.store_block(test_block.clone()).await;
        assert_eq!(block_store.blocks[0], test_block);
    }

    #[tokio::test]
    async fn delete_block_by_number_test() {
        let mut block_store = MemoryBlockStore::new();
        let test_block = ExecutionNodeBlock {
            hash: "0xtest".to_string(),
            number: 0,
            parent_hash: "0xparent".to_string(),
            timestamp: Utc::now(),
            difficulty: 0,
            total_difficulty: 0,
        };

        block_store.store_block(test_block.clone()).await;
        assert_eq!(block_store.len().await, 1);

        block_store.delete_block_by_number(&test_block.number).await;
        assert_eq!(block_store.len().await, 0);
    }

    #[tokio::test]
    async fn delete_blocks_test() {
        let mut block_store = MemoryBlockStore::new();
        block_store
            .store_block(ExecutionNodeBlock {
                hash: "0xtest".to_string(),
                number: 0,
                parent_hash: "0xparent".to_string(),
                timestamp: Utc::now(),
                difficulty: 0,
                total_difficulty: 0,
            })
            .await;
        block_store
            .store_block(ExecutionNodeBlock {
                hash: "0xtest1".to_string(),
                number: 1,
                parent_hash: "0xtest".to_string(),
                timestamp: Utc::now(),
                difficulty: 0,
                total_difficulty: 0,
            })
            .await;

        assert_eq!(block_store.len().await, 2);

        block_store.delete_blocks(&0).await;
        assert_eq!(block_store.len().await, 0);
    }

    #[tokio::test]
    async fn get_block_by_hash_test() {
        let mut block_store = MemoryBlockStore::new();
        let test_block = ExecutionNodeBlock {
            hash: "0xtest".to_string(),
            number: 0,
            parent_hash: "0xparent".to_string(),
            timestamp: Utc::now(),
            difficulty: 0,
            total_difficulty: 0,
        };

        block_store.store_block(test_block.clone()).await;
        let stored_block = block_store.get_block_by_hash(&test_block.hash).await;
        assert_eq!(stored_block, Some(&test_block));
    }

    #[tokio::test]
    async fn get_block_by_number_test() {
        let mut block_store = MemoryBlockStore::new();
        let test_block = ExecutionNodeBlock {
            hash: "0xtest".to_string(),
            number: 0,
            parent_hash: "0xparent".to_string(),
            timestamp: Utc::now(),
            difficulty: 0,
            total_difficulty: 0,
        };

        block_store.store_block(test_block.clone()).await;
        let stored_block = block_store.get_block_by_number(&test_block.number).await;
        assert_eq!(stored_block, Some(&test_block));
    }

    #[tokio::test]
    async fn get_is_first_parent_hash_known_test() {
        let block_store = MemoryBlockStore::new();
        let is_parent_known = block_store.get_is_parent_hash_known("0xnotthere").await;
        assert!(is_parent_known);
    }

    #[tokio::test]
    async fn get_is_parent_hash_known_test() {
        let mut block_store = MemoryBlockStore::new();
        let test_block = ExecutionNodeBlock {
            hash: "0xtest".to_string(),
            number: 0,
            parent_hash: "0xparent".to_string(),
            timestamp: Utc::now(),
            difficulty: 0,
            total_difficulty: 0,
        };

        block_store.store_block(test_block.clone()).await;
        assert!(!block_store.get_is_parent_hash_known("0xnotthere").await);
        assert!(block_store.get_is_parent_hash_known(&test_block.hash).await);
    }

    #[tokio::test]
    async fn get_is_number_known_test() {
        let mut block_store = MemoryBlockStore::new();
        let test_block = ExecutionNodeBlock {
            hash: "0xtest".to_string(),
            number: 0,
            parent_hash: "0xparent".to_string(),
            timestamp: Utc::now(),
            difficulty: 0,
            total_difficulty: 0,
        };

        block_store.store_block(test_block.clone()).await;
        assert!(!block_store.get_is_number_known(&1).await);
        assert!(block_store.get_is_number_known(&0).await);
    }

    #[tokio::test]
    async fn get_empty_last_block_number_test() {
        let block_store = MemoryBlockStore::new();
        assert_eq!(block_store.get_last_block_number().await, None);
    }

    #[tokio::test]
    async fn get_last_block_number_test() {
        let mut block_store = MemoryBlockStore::new();
        let test_block = ExecutionNodeBlock {
            hash: "0xtest".to_string(),
            number: 0,
            parent_hash: "0xparent".to_string(),
            timestamp: Utc::now(),
            difficulty: 0,
            total_difficulty: 0,
        };

        block_store.store_block(test_block.clone()).await;
        let last_block_number = block_store.get_last_block_number().await;
        assert_eq!(last_block_number, Some(0));
    }

    #[tokio::test]
    async fn is_empty_empty_test() {
        let block_store = MemoryBlockStore::new();
        assert!(block_store.is_empty().await);
    }

    #[tokio::test]
    async fn is_empty_not_empty_test() {
        let mut block_store = MemoryBlockStore::new();
        let test_block = ExecutionNodeBlock {
            hash: "0xtest".to_string(),
            number: 0,
            parent_hash: "0xparent".to_string(),
            timestamp: Utc::now(),
            difficulty: 0,
            total_difficulty: 0,
        };

        block_store.store_block(test_block.clone()).await;
        assert!(!block_store.is_empty().await);
    }
}
