//! Handles storage and retrieval of beacon blocks in our DB.
mod backfill;
mod heal;

use std::num::TryFromIntError;

use anyhow::Result;
use sqlx::{PgConnection, PgExecutor, Row};

use crate::eth_units::GweiNewtype;

use super::{
    node::{BeaconBlock, BeaconHeaderSignedEnvelope, BlockRoot},
    Slot,
};

pub use backfill::backfill_historic_slots;
pub use heal::heal_block_hashes;

pub const GENESIS_PARENT_ROOT: &str =
    "0x0000000000000000000000000000000000000000000000000000000000000000";

pub async fn get_deposit_sum_from_block_root<'a>(
    executor: impl PgExecutor<'a>,
    block_root: &str,
) -> sqlx::Result<GweiNewtype> {
    let deposit_sum_aggregated = sqlx::query!(
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
    .await?
    .deposit_sum_aggregated
    .try_into()
    .unwrap();

    Ok(deposit_sum_aggregated)
}

pub async fn get_is_hash_known(executor: impl PgExecutor<'_>, block_root: &str) -> bool {
    if block_root == GENESIS_PARENT_ROOT {
        return true;
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
    .unwrap()
    .get("exists")
}

pub async fn store_block(
    executor: impl PgExecutor<'_>,
    block: &BeaconBlock,
    deposit_sum: &GweiNewtype,
    deposit_sum_aggregated: &GweiNewtype,
    header: &BeaconHeaderSignedEnvelope,
) -> sqlx::Result<()> {
    sqlx::query!(
        "
            INSERT INTO beacon_blocks (
                block_hash,
                block_root,
                deposit_sum,
                deposit_sum_aggregated,
                parent_root,
                slot,
                state_root
            )
            VALUES (
                $1, $2, $3, $4, $5, $6, $7
            )
        ",
        block.block_hash(),
        header.root,
        i64::from(deposit_sum.to_owned()),
        i64::from(deposit_sum_aggregated.to_owned()),
        header.parent_root(),
        header.slot() as i32,
        header.state_root(),
    )
    .execute(executor)
    .await?;

    Ok(())
}

#[allow(dead_code)]
pub async fn get_last_block_slot(connection: &mut PgConnection) -> Result<Option<u32>> {
    let row = sqlx::query!(
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
    .fetch_optional(connection)
    .await?;

    let slot = row.map(|row| row.slot as u32);

    Ok(slot)
}

pub async fn delete_blocks(connection: impl PgExecutor<'_>, greater_than_or_equal: &Slot) {
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
        *greater_than_or_equal as i32
    )
    .execute(connection)
    .await
    .unwrap();
}

pub async fn delete_block(connection: impl PgExecutor<'_>, slot: &Slot) {
    sqlx::query(
        "
            DELETE FROM beacon_blocks
            WHERE state_root IN (
                SELECT
                    state_root
                FROM
                    beacon_states
                WHERE slot = $1
            )
        ",
    )
    .bind(*slot as i32)
    .execute(connection)
    .await
    .unwrap();
}

pub async fn get_block_root_before_slot(
    executor: impl PgExecutor<'_>,
    less_than: &Slot,
) -> Result<BlockRoot> {
    let row = sqlx::query!(
        "
            SELECT
                block_root
            FROM
                beacon_states 
            JOIN
                beacon_blocks 
            ON
                beacon_states.state_root = beacon_blocks.state_root 
            WHERE beacon_states.slot < $1
            ORDER BY beacon_states.slot DESC 
            LIMIT 1
        ",
        *less_than as i32
    )
    .fetch_one(executor)
    .await?;

    Ok(row.block_root)
}

struct BlockRow {
    block_hash: Option<String>,
    block_root: String,
    deposit_sum: i64,
    deposit_sum_aggregated: i64,
    parent_root: String,
    state_root: String,
}

#[derive(Debug, PartialEq)]
pub struct Block {
    pub block_hash: Option<String>,
    block_root: String,
    deposit_sum: GweiNewtype,
    deposit_sum_aggregated: GweiNewtype,
    parent_root: String,
    pub state_root: String,
}

impl TryFrom<BlockRow> for Block {
    type Error = TryFromIntError;
    fn try_from(value: BlockRow) -> Result<Self, Self::Error> {
        let block = Block {
            block_hash: value.block_hash,
            block_root: value.block_root,
            state_root: value.state_root,
            deposit_sum: value.deposit_sum.try_into()?,
            deposit_sum_aggregated: value.deposit_sum_aggregated.try_into()?,
            parent_root: value.parent_root,
        };

        Ok(block)
    }
}

pub async fn get_block_by_block_root(
    executor: impl PgExecutor<'_>,
    block_root: &str,
) -> Result<Block> {
    let row = sqlx::query_as!(
        BlockRow,
        "
            SELECT
                block_root,
                state_root,
                parent_root,
                deposit_sum,
                deposit_sum_aggregated,
                block_hash
            FROM
                beacon_blocks
            WHERE
                block_root = $1
        ",
        block_root
    )
    .fetch_one(executor)
    .await?;

    let block = row.try_into()?;

    Ok(block)
}

pub async fn update_block_hash(
    executor: impl PgExecutor<'_>,
    block_root: &str,
    block_hash: &str,
) -> Result<()> {
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
    .await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use sqlx::Acquire;

    use super::*;
    use crate::{
        beacon_chain::{
            node::{BeaconBlockBody, BeaconHeader, BeaconHeaderEnvelope, ExecutionPayload},
            states::store_state,
            tests::{store_custom_test_block, store_test_block},
            BeaconBlockBuilder, BeaconHeaderSignedEnvelopeBuilder,
        },
        db,
    };

    #[tokio::test]
    async fn get_is_genesis_known_test() {
        let mut connection = db::get_test_db().await;
        let mut transaction = connection.begin().await.unwrap();

        let is_hash_known = get_is_hash_known(&mut transaction, GENESIS_PARENT_ROOT).await;

        assert_eq!(is_hash_known, true);
    }

    #[tokio::test]
    async fn get_is_hash_not_known_test() {
        let mut connection = db::get_test_db().await;
        let mut transaction = connection.begin().await.unwrap();

        let is_hash_known = get_is_hash_known(&mut transaction, "0xnot_there").await;

        assert_eq!(is_hash_known, false);
    }

    #[tokio::test]
    async fn get_is_hash_known_test() {
        let mut connection = db::get_test_db().await;
        let mut transaction = connection.begin().await.unwrap();

        store_test_block(&mut transaction, "is_hash_known_test").await;

        let is_hash_known =
            get_is_hash_known(&mut transaction, "0xis_hash_known_test_block_root").await;

        assert_eq!(is_hash_known, true);
    }

    #[tokio::test]
    async fn store_block_test() {
        let mut connection = db::get_test_db().await;
        let mut transaction = connection.begin().await.unwrap();

        let state_root = format!("0xblock_test_state_root");
        let slot = 0;

        store_state(&mut transaction, &state_root, &slot, "")
            .await
            .unwrap();

        store_block(
            &mut transaction,
            &BeaconBlock {
                body: BeaconBlockBody {
                    deposits: vec![],
                    execution_payload: None,
                },
                parent_root: GENESIS_PARENT_ROOT.to_string(),
                slot,
                state_root: state_root.clone(),
            },
            &GweiNewtype(0),
            &GweiNewtype(0),
            &BeaconHeaderSignedEnvelope {
                root: "0xblock_root".to_string(),
                header: BeaconHeaderEnvelope {
                    message: BeaconHeader {
                        slot,
                        parent_root: GENESIS_PARENT_ROOT.to_string(),
                        state_root,
                    },
                },
            },
        )
        .await
        .unwrap();

        let is_hash_known = get_is_hash_known(&mut transaction, "0xblock_root").await;

        assert_eq!(is_hash_known, true);
    }

    #[tokio::test]
    async fn get_last_block_number_none_test() {
        let mut connection = db::get_test_db().await;
        let mut transaction = connection.begin().await.unwrap();

        let block_number = get_last_block_slot(&mut transaction).await.unwrap();
        assert_eq!(block_number, None);
    }

    #[tokio::test]
    async fn get_last_block_number_some_test() {
        let mut connection = db::get_test_db().await;
        let mut transaction = connection.begin().await.unwrap();

        let test_id = "last_block_number_some_test";
        let slot = 5923;
        let test_header = BeaconHeaderSignedEnvelopeBuilder::new(test_id)
            .slot(&slot)
            .build();
        let test_block = Into::<BeaconBlockBuilder>::into(&test_header).build();

        store_custom_test_block(&mut transaction, &test_header, &test_block).await;

        let last_block_slot = get_last_block_slot(&mut transaction).await.unwrap();
        assert_eq!(last_block_slot, Some(slot));
    }

    #[tokio::test]
    async fn delete_block_test() {
        let mut connection = db::get_test_db().await;
        let mut transaction = connection.begin().await.unwrap();

        store_test_block(&mut transaction, "delete_block_test").await;

        let block_slot = get_last_block_slot(&mut transaction).await.unwrap();
        assert_eq!(block_slot, Some(0));

        delete_blocks(&mut transaction, &0).await;

        let block_slot = get_last_block_slot(&mut transaction).await.unwrap();
        assert_eq!(block_slot, None);
    }

    #[tokio::test]
    async fn get_block_root_before_slot_test() -> Result<()> {
        let mut connection = db::get_test_db().await;
        let mut transaction = connection.begin().await.unwrap();

        let test_id_before = "last_block_before_slot_before";
        let test_header_before = BeaconHeaderSignedEnvelopeBuilder::new(test_id_before).build();
        let test_block_before = Into::<BeaconBlockBuilder>::into(&test_header_before).build();

        let test_id_after = "last_block_before_slot_after";
        let test_header_after = BeaconHeaderSignedEnvelopeBuilder::new(test_id_after)
            .parent_header(&test_header_before)
            .build();
        let test_block_after = Into::<BeaconBlockBuilder>::into(&test_header_after).build();

        store_custom_test_block(&mut transaction, &test_header_before, &test_block_before).await;

        store_custom_test_block(&mut transaction, &test_header_after, &test_block_after).await;

        let last_block_before = get_block_root_before_slot(&mut transaction, &1).await?;

        assert_eq!(test_header_before.root, last_block_before);

        Ok(())
    }

    #[tokio::test]
    async fn get_block_test() -> Result<()> {
        let mut connection = db::get_test_db().await;
        let mut transaction = connection.begin().await.unwrap();

        let test_id = "get_block_test";
        let slot = 374;
        let block_hash = format!("0x{test_id}_block_hash");
        let header = BeaconHeaderSignedEnvelopeBuilder::new(test_id)
            .slot(&slot)
            .build();

        store_custom_test_block(
            &mut transaction,
            &header,
            &BeaconBlock {
                body: BeaconBlockBody {
                    deposits: vec![],
                    execution_payload: Some(ExecutionPayload {
                        block_hash: block_hash.clone(),
                    }),
                },
                parent_root: GENESIS_PARENT_ROOT.to_string(),
                slot,
                state_root: header.state_root().to_string(),
            },
        )
        .await;

        let block = get_block_by_block_root(&mut transaction, &header.root).await?;

        assert_eq!(
            Block {
                block_root: header.root.to_owned(),
                state_root: header.state_root().to_owned(),
                parent_root: GENESIS_PARENT_ROOT.to_string(),
                deposit_sum: GweiNewtype(0),
                deposit_sum_aggregated: GweiNewtype(0),
                block_hash: Some(block_hash)
            },
            block
        );

        Ok(())
    }

    #[tokio::test]
    async fn update_block_hash_test() -> Result<()> {
        let mut connection = db::get_test_db().await;
        let mut transaction = connection.begin().await.unwrap();

        let test_id = "get_block_test";
        let slot = 374;
        let state_root = format!("0x{test_id}_state_root");
        let block_root = format!("0x{test_id}_block_root");
        let block_hash = format!("0x{test_id}_block_hash");
        let block_hash_after = format!("0x{test_id}_block_hash_after");
        let header = BeaconHeaderSignedEnvelopeBuilder::new(test_id).build();

        store_custom_test_block(
            &mut transaction,
            &header,
            &BeaconBlock {
                body: BeaconBlockBody {
                    deposits: vec![],
                    execution_payload: Some(ExecutionPayload {
                        block_hash: block_hash.clone(),
                    }),
                },
                parent_root: GENESIS_PARENT_ROOT.to_string(),
                slot,
                state_root: state_root.clone(),
            },
        )
        .await;

        update_block_hash(&mut transaction, &block_root, &block_hash_after).await?;

        let block = get_block_by_block_root(&mut transaction, &block_root).await?;

        assert_eq!(block_hash_after, block.block_hash.unwrap());

        Ok(())
    }
}
