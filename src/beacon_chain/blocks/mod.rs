//! Handles storage and retrieval of beacon blocks in our DB.
pub mod backfill;
mod heal_block_hashes;

use anyhow::Context;
use sqlx::{PgExecutor, Row};

use crate::units::GweiNewtype;

use super::{
    node::{BeaconBlock, BeaconHeaderSignedEnvelope},
    Slot,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct StoreBlockParams {
    pub deposit_sum: GweiNewtype,
    pub deposit_sum_aggregated: GweiNewtype,
    pub withdrawal_sum: GweiNewtype,
    pub withdrawal_sum_aggregated: GweiNewtype,
    pub pending_deposits_sum: Option<GweiNewtype>,
}

pub use heal_block_hashes::heal_block_hashes;

pub const GENESIS_PARENT_ROOT: &str =
    "0x0000000000000000000000000000000000000000000000000000000000000000";

pub async fn get_deposit_sum_from_block_root(
    executor: impl PgExecutor<'_>,
    block_root: &str,
) -> GweiNewtype {
    sqlx::query!(
        "
        SELECT
            deposit_sum_aggregated
        FROM
            beacon_blocks
        WHERE
            block_root = $1
        ",
        block_root
    )
    .fetch_one(executor)
    .await
    .unwrap()
    .deposit_sum_aggregated
    .into()
}

pub async fn get_withdrawal_sum_from_block_root(
    executor: impl PgExecutor<'_>,
    block_root: &str,
) -> GweiNewtype {
    sqlx::query!(
        "
        SELECT
            withdrawal_sum_aggregated
        FROM
            beacon_blocks
        WHERE
            block_root = $1
        ",
        block_root
    )
    .fetch_one(executor)
    .await
    .unwrap()
    .withdrawal_sum_aggregated
    .unwrap_or_default()
    .into()
}

pub async fn get_is_hash_known(
    executor: impl PgExecutor<'_>,
    block_root: &str,
) -> anyhow::Result<bool> {
    if block_root == GENESIS_PARENT_ROOT {
        return Ok(true);
    }

    sqlx::query(
        "
        SELECT EXISTS (
            SELECT 1 FROM beacon_blocks
            WHERE block_root = $1
        )
        ",
    )
    .bind(block_root)
    .fetch_one(executor)
    .await
    .map(|row| row.get::<bool, _>("exists"))
    .with_context(|| format!("failed to check if block root is known: {}", block_root))
}

/// Gets the highest slot from the beacon_blocks table.
pub async fn get_latest_beacon_block_slot(executor: impl PgExecutor<'_>) -> anyhow::Result<Slot> {
    sqlx::query_scalar!(
        r#"
        SELECT MAX(slot) AS slot
        FROM beacon_blocks
        "#
    )
    .fetch_optional(executor)
    .await
    .map(|row| row.expect("can't call get_latest_beacon_block_slot on empty table"))
    .map(|slot| slot.expect("expect all beacon blocks to have a slot"))
    .map(Slot)
    .context("failed to get latest beacon block slot from beacon_blocks table")
}

pub async fn store_block(
    executor: impl PgExecutor<'_>,
    block: &BeaconBlock,
    sums: StoreBlockParams,
    header: &BeaconHeaderSignedEnvelope,
) {
    let slot_value: i32 = block.slot.into();
    sqlx::query(
        "
        INSERT INTO beacon_blocks (
            block_hash,
            block_root,
            slot,
            deposit_sum,
            deposit_sum_aggregated,
            withdrawal_sum,
            withdrawal_sum_aggregated,
            pending_deposits_sum_gwei,
            parent_root,
            state_root
        )
        VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10
        )
        ",
    )
    .bind(block.block_hash())
    .bind(&header.root)
    .bind(slot_value)
    .bind(i64::from(sums.deposit_sum))
    .bind(i64::from(sums.deposit_sum_aggregated))
    .bind(i64::from(sums.withdrawal_sum))
    .bind(i64::from(sums.withdrawal_sum_aggregated))
    .bind(sums.pending_deposits_sum.map(i64::from))
    .bind(header.parent_root())
    .bind(header.state_root())
    .execute(executor)
    .await
    .unwrap();
}

pub async fn delete_blocks(executor: impl PgExecutor<'_>, greater_than_or_equal: Slot) {
    sqlx::query!(
        "
        DELETE FROM beacon_blocks
        WHERE state_root IN (
            SELECT
                state_root
            FROM
                beacon_states
            WHERE beacon_states.slot >= $1
        )
        ",
        greater_than_or_equal.0
    )
    .execute(executor)
    .await
    .unwrap();
}

#[derive(Debug, PartialEq, Eq)]
pub struct BeaconBlockFromDb {
    pub block_root: String,
    deposit_sum: GweiNewtype,
    deposit_sum_aggregated: GweiNewtype,
    pub parent_root: String,
    pub block_hash: Option<String>,
    pub state_root: String,
}

pub async fn update_block_hash(executor: impl PgExecutor<'_>, block_root: &str, block_hash: &str) {
    sqlx::query!(
        "
        UPDATE
            beacon_blocks
        SET
            block_hash = $1
        WHERE
            block_root = $2
        ",
        block_hash,
        block_root
    )
    .execute(executor)
    .await
    .unwrap();
}

struct BlockDbRow {
    block_root: String,
    deposit_sum: i64,
    deposit_sum_aggregated: i64,
    parent_root: String,
    pub block_hash: Option<String>,
    pub state_root: String,
    #[allow(dead_code)]
    pub slot: Option<i32>,
}

impl From<BlockDbRow> for BeaconBlockFromDb {
    fn from(row: BlockDbRow) -> Self {
        Self {
            block_hash: row.block_hash,
            block_root: row.block_root,
            deposit_sum: row.deposit_sum.into(),
            deposit_sum_aggregated: row.deposit_sum_aggregated.into(),
            parent_root: row.parent_root,
            state_root: row.state_root,
        }
    }
}

pub async fn get_block_by_slot(
    executor: impl PgExecutor<'_>,
    slot: Slot,
) -> anyhow::Result<Option<BeaconBlockFromDb>> {
    let row = sqlx::query_as!(
        BlockDbRow,
        r#"
        SELECT
            block_root,
            beacon_blocks.state_root,
            parent_root,
            deposit_sum,
            deposit_sum_aggregated,
            block_hash,
            slot
        FROM
            beacon_blocks
        WHERE
            slot = $1
        "#,
        slot.0
    )
    .fetch_optional(executor)
    .await
    .context(format!("failed to get block for slot {}", slot))?;

    Ok(row.map(|row| row.into()))
}

#[cfg(test)]
mod tests {
    use sqlx::Acquire;
    use test_context::test_context;

    use super::*;
    use crate::{
        beacon_chain::{
            node::types::BeaconBlockVersionedEnvelope,
            store_state,
            tests::{store_custom_test_block, store_test_block},
            BeaconBlockBuilder, BeaconHeaderSignedEnvelopeBuilder,
        },
        db::{self, tests::TestDb},
    };

    pub async fn get_last_block_slot(executor: impl PgExecutor<'_>) -> Option<Slot> {
        sqlx::query!(
            "
            SELECT
                beacon_states.slot
            FROM
                beacon_blocks
            JOIN
                beacon_states
            ON
                beacon_states.state_root = beacon_blocks.state_root
            ORDER BY slot DESC
            LIMIT 1
            ",
        )
        .fetch_optional(executor)
        .await
        .unwrap()
        .map(|row| Slot(row.slot))
    }

    #[tokio::test]
    async fn get_is_genesis_known_test() {
        let mut connection = db::tests::get_test_db_connection().await;
        let mut transaction = connection.begin().await.unwrap();

        let is_hash_known = get_is_hash_known(&mut *transaction, GENESIS_PARENT_ROOT)
            .await
            .unwrap();

        assert!(is_hash_known);
    }

    #[tokio::test]
    async fn get_is_hash_not_known_test() {
        let mut connection = db::tests::get_test_db_connection().await;
        let mut transaction = connection.begin().await.unwrap();

        let is_hash_known = get_is_hash_known(&mut *transaction, "0xnot_there")
            .await
            .unwrap();

        assert!(!is_hash_known);
    }

    #[tokio::test]
    async fn get_is_hash_known_test() {
        let mut connection = db::tests::get_test_db_connection().await;
        let mut transaction = connection.begin().await.unwrap();

        store_test_block(&mut transaction, "is_hash_known_test").await;

        let is_hash_known = get_is_hash_known(&mut *transaction, "0xis_hash_known_test_block_root")
            .await
            .unwrap();

        assert!(is_hash_known);
    }

    #[test_context(TestDb)]
    #[tokio::test]
    async fn store_block_test(db: &TestDb) {
        let mut transaction = db.pool.begin().await.unwrap();

        let slot = Slot(0);
        let block = BeaconBlockBuilder::default().build();
        let header: BeaconHeaderSignedEnvelope =
            BeaconHeaderSignedEnvelopeBuilder::from(&block).build();

        store_state(&mut *transaction, &block.state_root, slot).await;

        let sums = StoreBlockParams {
            deposit_sum: GweiNewtype(0),
            deposit_sum_aggregated: GweiNewtype(0),
            withdrawal_sum: GweiNewtype(0),
            withdrawal_sum_aggregated: GweiNewtype(0),
            pending_deposits_sum: None,
        };

        store_block(&mut *transaction, &block, sums, &header).await;

        let is_hash_known = get_is_hash_known(&mut *transaction, &header.root)
            .await
            .unwrap();

        assert!(is_hash_known);
    }

    #[tokio::test]
    async fn get_last_block_number_none_test() {
        let mut connection = db::tests::get_test_db_connection().await;
        let mut transaction = connection.begin().await.unwrap();

        let block_number = get_last_block_slot(&mut *transaction).await;
        assert_eq!(block_number, None);
    }

    #[tokio::test]
    async fn get_last_block_number_some_test() {
        let mut connection = db::tests::get_test_db_connection().await;
        let mut transaction = connection.begin().await.unwrap();

        let test_id = "last_block_number_some_test";
        let slot = Slot(5923);
        let test_header = BeaconHeaderSignedEnvelopeBuilder::new(test_id)
            .slot(slot)
            .build();
        let test_block = Into::<BeaconBlockBuilder>::into(&test_header).build();

        store_custom_test_block(&mut transaction, &test_header, &test_block).await;

        let last_block_slot = get_last_block_slot(&mut *transaction).await;
        assert_eq!(last_block_slot, Some(slot));
    }

    #[tokio::test]
    async fn delete_block_test() {
        let mut connection = db::tests::get_test_db_connection().await;
        let mut transaction = connection.begin().await.unwrap();

        store_test_block(&mut transaction, "delete_block_test").await;

        let block_slot = get_last_block_slot(&mut *transaction).await;
        assert_eq!(block_slot, Some(Slot(0)));

        delete_blocks(&mut *transaction, Slot(0)).await;

        let block_slot = get_last_block_slot(&mut *transaction).await;
        assert_eq!(block_slot, None);
    }

    #[test_context(TestDb)]
    #[tokio::test]
    async fn update_block_hash_test(db: &TestDb) {
        let block = BeaconBlockBuilder::new().build();
        let header: BeaconHeaderSignedEnvelope = (&block).into();

        store_state(&db.pool, &block.state_root, block.slot).await;
        store_block(&db.pool, &block, StoreBlockParams::default(), &header).await;

        let target_block_hash = "0xupdate_block_hash_test_block_hash".to_string();
        update_block_hash(&db.pool, &header.root, &target_block_hash).await;

        let block_hash_after = get_block_by_slot(&db.pool, block.slot)
            .await
            .unwrap()
            .unwrap()
            .block_hash;
        assert_eq!(block_hash_after, Some(target_block_hash));
    }

    #[tokio::test]
    async fn get_block_by_slot_test() {
        let mut connection = db::tests::get_test_db_connection().await;
        let mut transaction = connection.begin().await.unwrap();

        let test_id = "get_block_by_slot";
        let header = BeaconHeaderSignedEnvelopeBuilder::new(test_id).build();
        let block = Into::<BeaconBlockBuilder>::into(&header).build();

        let block_not_there = get_block_by_slot(&mut *transaction, Slot(0)).await.unwrap();

        assert_eq!(None, block_not_there);

        store_custom_test_block(&mut transaction, &header, &block).await;

        let block_there = get_block_by_slot(&mut *transaction, Slot(0))
            .await
            .unwrap()
            .unwrap();

        assert_eq!(header.root, block_there.block_root);
    }

    #[tokio::test]
    async fn decode_deposits_block_test() {
        let raw_block = include_str!("../data_samples/block_11650102.json");
        let block: BeaconBlock = serde_json::from_str::<BeaconBlockVersionedEnvelope>(raw_block)
            .unwrap()
            .into();

        let deposits = block.deposits();
        let total_deposit_amount: GweiNewtype = deposits
            .iter()
            .fold(GweiNewtype(0), |acc, d| acc + d.amount);

        assert_eq!(deposits.len(), 16);
        assert_eq!(total_deposit_amount, GweiNewtype(512000000000));
    }

    #[tokio::test]
    async fn decode_execution_request_deposits_block_test() {
        let raw_block = include_str!("../data_samples/block_11678488.json");
        let block: BeaconBlock = serde_json::from_str::<BeaconBlockVersionedEnvelope>(raw_block)
            .unwrap()
            .into();

        let execution_request_deposits = block.execution_request_deposits();
        let total_execution_deposit_amount: GweiNewtype = execution_request_deposits
            .iter()
            .fold(GweiNewtype(0), |acc, d| acc + d.amount);

        assert_eq!(execution_request_deposits.len(), 60);
        assert_eq!(total_execution_deposit_amount, GweiNewtype(60000000000));
    }
}
