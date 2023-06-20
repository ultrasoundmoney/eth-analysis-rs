//! Handles storage and retrieval of beacon blocks in our DB.
mod heal;

use sqlx::{PgExecutor, Row};

use crate::units::GweiNewtype;

use super::{
    node::{BeaconBlock, BeaconHeaderSignedEnvelope},
    Slot,
};

pub use heal::heal_block_hashes;

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
    .try_into()
    .unwrap()
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
    .try_into()
    .unwrap()
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
    withdrawal_sum: &GweiNewtype,
    withdrawal_sum_aggregated: &GweiNewtype,
    header: &BeaconHeaderSignedEnvelope,
) {
    sqlx::query!(
        "
        INSERT INTO beacon_blocks (
            block_hash,
            block_root,
            deposit_sum,
            deposit_sum_aggregated,
            withdrawal_sum,
            withdrawal_sum_aggregated,
            parent_root,
            state_root
        )
        VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8
        )
        ",
        block.block_hash(),
        header.root,
        i64::from(deposit_sum.to_owned()),
        i64::from(deposit_sum_aggregated.to_owned()),
        i64::from(withdrawal_sum.to_owned()),
        i64::from(withdrawal_sum_aggregated.to_owned()),
        header.parent_root(),
        header.state_root(),
    )
    .execute(executor)
    .await
    .unwrap();
}

pub async fn delete_blocks(executor: impl PgExecutor<'_>, greater_than_or_equal: &Slot) {
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

pub async fn delete_block(executor: impl PgExecutor<'_>, slot: &Slot) {
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
    .bind(slot.0)
    .execute(executor)
    .await
    .unwrap();
}

pub async fn get_block_before_slot(executor: impl PgExecutor<'_>, less_than: &Slot) -> DbBlock {
    sqlx::query_as!(
        BlockDbRow,
        "
        SELECT
            block_root,
            beacon_blocks.state_root,
            parent_root,
            deposit_sum,
            deposit_sum_aggregated,
            block_hash
        FROM
            beacon_blocks 
        JOIN beacon_states ON
            beacon_blocks.state_root = beacon_states.state_root 
        WHERE slot < $1
        ORDER BY slot DESC 
        LIMIT 1
        ",
        less_than.0
    )
    .fetch_one(executor)
    .await
    .unwrap()
    .into()
}

#[derive(Debug, PartialEq, Eq)]
pub struct DbBlock {
    block_root: String,
    deposit_sum: GweiNewtype,
    deposit_sum_aggregated: GweiNewtype,
    parent_root: String,
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
}

impl From<BlockDbRow> for DbBlock {
    fn from(row: BlockDbRow) -> Self {
        Self {
            block_hash: row.block_hash,
            block_root: row.block_root,
            deposit_sum: row.deposit_sum.try_into().unwrap(),
            deposit_sum_aggregated: row.deposit_sum_aggregated.try_into().unwrap(),
            parent_root: row.parent_root,
            state_root: row.state_root,
        }
    }
}

pub async fn get_block_by_slot(executor: impl PgExecutor<'_>, slot: &Slot) -> Option<DbBlock> {
    sqlx::query_as!(
        BlockDbRow,
        r#"
        SELECT
            block_root,
            beacon_blocks.state_root,
            parent_root,
            deposit_sum,
            deposit_sum_aggregated,
            block_hash
        FROM
            beacon_blocks
        JOIN beacon_states ON
            beacon_blocks.state_root = beacon_states.state_root
        WHERE
            slot = $1
        "#,
        slot.0
    )
    .fetch_optional(executor)
    .await
    .unwrap()
    .map(|row| row.into())
}

#[cfg(test)]
mod tests {
    use sqlx::Acquire;

    use super::*;
    use crate::{
        beacon_chain::{
            node::{BeaconBlockBody, BeaconHeader, BeaconHeaderEnvelope, ExecutionPayload},
            store_state,
            tests::{store_custom_test_block, store_test_block},
            BeaconBlockBuilder, BeaconHeaderSignedEnvelopeBuilder,
        },
        db,
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

        let is_hash_known = get_is_hash_known(&mut transaction, GENESIS_PARENT_ROOT).await;

        assert!(is_hash_known);
    }

    #[tokio::test]
    async fn get_is_hash_not_known_test() {
        let mut connection = db::tests::get_test_db_connection().await;
        let mut transaction = connection.begin().await.unwrap();

        let is_hash_known = get_is_hash_known(&mut transaction, "0xnot_there").await;

        assert!(!is_hash_known);
    }

    #[tokio::test]
    async fn get_is_hash_known_test() {
        let mut connection = db::tests::get_test_db_connection().await;
        let mut transaction = connection.begin().await.unwrap();

        store_test_block(&mut transaction, "is_hash_known_test").await;

        let is_hash_known =
            get_is_hash_known(&mut transaction, "0xis_hash_known_test_block_root").await;

        assert!(is_hash_known);
    }

    #[tokio::test]
    async fn store_block_test() {
        let mut connection = db::tests::get_test_db_connection().await;
        let mut transaction = connection.begin().await.unwrap();

        let state_root = "0xblock_test_state_root".to_string();
        let slot = Slot(0);

        store_state(&mut transaction, &state_root, &slot).await;

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
        .await;

        let is_hash_known = get_is_hash_known(&mut transaction, "0xblock_root").await;

        assert!(is_hash_known);
    }

    #[tokio::test]
    async fn get_last_block_number_none_test() {
        let mut connection = db::tests::get_test_db_connection().await;
        let mut transaction = connection.begin().await.unwrap();

        let block_number = get_last_block_slot(&mut transaction).await;
        assert_eq!(block_number, None);
    }

    #[tokio::test]
    async fn get_last_block_number_some_test() {
        let mut connection = db::tests::get_test_db_connection().await;
        let mut transaction = connection.begin().await.unwrap();

        let test_id = "last_block_number_some_test";
        let slot = Slot(5923);
        let test_header = BeaconHeaderSignedEnvelopeBuilder::new(test_id)
            .slot(&slot)
            .build();
        let test_block = Into::<BeaconBlockBuilder>::into(&test_header).build();

        store_custom_test_block(&mut transaction, &test_header, &test_block).await;

        let last_block_slot = get_last_block_slot(&mut transaction).await;
        assert_eq!(last_block_slot, Some(slot));
    }

    #[tokio::test]
    async fn delete_block_test() {
        let mut connection = db::tests::get_test_db_connection().await;
        let mut transaction = connection.begin().await.unwrap();

        store_test_block(&mut transaction, "delete_block_test").await;

        let block_slot = get_last_block_slot(&mut transaction).await;
        assert_eq!(block_slot, Some(Slot(0)));

        delete_blocks(&mut transaction, &Slot(0)).await;

        let block_slot = get_last_block_slot(&mut transaction).await;
        assert_eq!(block_slot, None);
    }

    #[tokio::test]
    async fn get_block_before_slot_test() {
        let mut connection = db::tests::get_test_db_connection().await;
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

        let last_block_before = get_block_before_slot(&mut transaction, &Slot(1)).await;

        assert_eq!(test_header_before.root, last_block_before.block_root);
    }

    #[tokio::test]
    async fn get_block_before_missing_slot_test() {
        let mut connection = db::tests::get_test_db_connection().await;
        let mut transaction = connection.begin().await.unwrap();

        let test_id_before = "last_block_before_slot_before";
        let test_header_before = BeaconHeaderSignedEnvelopeBuilder::new(test_id_before).build();
        let test_block_before = Into::<BeaconBlockBuilder>::into(&test_header_before).build();

        let test_id_after = "last_block_before_slot_after";
        let test_header_after = BeaconHeaderSignedEnvelopeBuilder::new(test_id_after)
            .parent_header(&test_header_before)
            .build();

        store_custom_test_block(&mut transaction, &test_header_before, &test_block_before).await;

        store_state(
            &mut transaction,
            &test_header_after.state_root(),
            &test_header_after.slot(),
        )
        .await;

        let last_block_before = get_block_before_slot(&mut transaction, &Slot(1)).await;

        assert_eq!(test_header_before.root, last_block_before.block_root);
    }

    #[tokio::test]
    async fn update_block_hash_test() {
        let mut connection = db::tests::get_test_db_connection().await;
        let mut transaction = connection.begin().await.unwrap();

        let test_id = "get_block_test";
        let slot = Slot(374);
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
                        withdrawals: None,
                    }),
                },
                parent_root: GENESIS_PARENT_ROOT.to_string(),
                slot,
                state_root: state_root.clone(),
            },
        )
        .await;

        update_block_hash(&mut transaction, &block_root, &block_hash_after).await;

        let block = get_block_by_slot(&mut transaction, &header.slot())
            .await
            .unwrap();

        assert_eq!(block_hash_after, block.block_hash.unwrap());
    }

    #[tokio::test]
    async fn get_block_by_slot_test() {
        let mut connection = db::tests::get_test_db_connection().await;
        let mut transaction = connection.begin().await.unwrap();

        let test_id = "get_block_by_slot";
        let header = BeaconHeaderSignedEnvelopeBuilder::new(test_id).build();
        let block = Into::<BeaconBlockBuilder>::into(&header).build();

        let block_not_there = get_block_by_slot(&mut transaction, &Slot(0)).await;

        assert_eq!(None, block_not_there);

        store_custom_test_block(&mut transaction, &header, &block).await;

        let block_there = get_block_by_slot(&mut transaction, &Slot(0)).await.unwrap();

        assert_eq!(header.root, block_there.block_root);
    }
}
