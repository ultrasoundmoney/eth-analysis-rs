use async_trait::async_trait;
use chrono::{DateTime, Utc};
use mockall::{automock, predicate::*};
use sqlx::{Pool, Postgres};

use crate::beacon_chain::Slot;

use super::MevBlock;

#[automock]
#[async_trait]
pub trait MevBlocksStore {
    async fn last_synced_slot(&self) -> Option<Slot>;
    async fn store_blocks(&self, blocks: &[MevBlock]);
}

pub struct MevBlocksStorePostgres {
    db_pool: Pool<Postgres>,
}

impl MevBlocksStorePostgres {
    pub fn new(db_pool: Pool<Postgres>) -> Self {
        Self { db_pool }
    }
}

#[async_trait]
impl MevBlocksStore for MevBlocksStorePostgres {
    async fn last_synced_slot(&self) -> Option<Slot> {
        sqlx::query!(
            "
            SELECT slot
            FROM mev_blocks
            ORDER BY slot DESC
            LIMIT 1
            "
        )
        .fetch_optional(&self.db_pool)
        .await
        .unwrap()
        .map(|row| Slot(row.slot))
    }

    async fn store_blocks(&self, blocks: &[MevBlock]) {
        let slots: Vec<i32> = blocks.iter().map(|b| b.slot).collect();
        let block_numbers: Vec<i32> = blocks.iter().map(|b| b.block_number).collect();
        let block_hashes: Vec<String> = blocks.iter().map(|b| b.block_hash.clone()).collect();
        let timestamps: Vec<DateTime<Utc>> = blocks.iter().map(|b| Slot(b.slot).into()).collect();
        let payments: Vec<String> = blocks.iter().map(|b| b.bid.to_string()).collect();

        sqlx::query!(
            "
            INSERT INTO mev_blocks (slot, block_number, block_hash, bid_wei, timestamp)
            SELECT * FROM UNNEST($1::int[], $2::int[], $3::text[], $4::numeric[], $5::timestamptz[])
            ON CONFLICT (block_hash) DO UPDATE SET
                bid_wei = excluded.bid_wei,
                block_number = excluded.block_number,
                slot = excluded.slot,
                timestamp = excluded.timestamp
            ",
            &slots,
            &block_numbers,
            &block_hashes,
            &payments as &Vec<String>,
            &timestamps,
        )
        .execute(&self.db_pool)
        .await
        .unwrap();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{db::tests::TestDb, units::WeiNewtype};
    use test_context::test_context;

    #[test_context(TestDb)]
    #[tokio::test]
    async fn last_synced_slot_test(test_db: &TestDb) {
        let mev_block_store = MevBlocksStorePostgres::new(test_db.pool.clone());

        let block_1 = MevBlock {
            slot: 10,
            block_number: 100,
            block_hash: String::from("abc"),
            bid: WeiNewtype::from_eth(1),
        };

        let block_2 = MevBlock {
            slot: 20,
            block_number: 200,
            block_hash: String::from("def"),
            bid: WeiNewtype::from_eth(2),
        };

        mev_block_store.store_blocks(&[block_1, block_2]).await;

        let last_synced_slot = mev_block_store.last_synced_slot().await;
        assert_eq!(last_synced_slot, Some(Slot(20)));
    }

    #[test_context(TestDb)]
    #[tokio::test]
    async fn store_blocks_test(test_db: &TestDb) {
        let mev_block_store = MevBlocksStorePostgres::new(test_db.pool.clone());

        let block_1 = MevBlock {
            slot: 10,
            block_number: 100,
            block_hash: String::from("abc"),
            bid: WeiNewtype::from_eth(1),
        };

        let block_2 = MevBlock {
            slot: 20,
            block_number: 200,
            block_hash: String::from("def"),
            bid: WeiNewtype::from_eth(2),
        };

        mev_block_store.store_blocks(&[block_1, block_2]).await;

        let last_synced_slot = mev_block_store.last_synced_slot().await;
        assert_eq!(last_synced_slot, Some(Slot(20)));

        // Now insert a block with the same hash as block_2 but different slot and block_number
        let block_3 = MevBlock {
            slot: 30,
            block_number: 300,
            block_hash: String::from("def"), // Same hash as block_2
            bid: WeiNewtype::from_eth(3),
        };

        mev_block_store.store_blocks(&[block_3]).await;

        let last_synced_slot = mev_block_store.last_synced_slot().await;
        assert_eq!(last_synced_slot, Some(Slot(30)));
    }
}
