use sqlx::{postgres::PgRow, PgConnection, PgExecutor, Row};

use crate::eth_units::GweiNewtype;

use super::{node::BeaconHeaderSignedEnvelope, Slot};

pub const GENESIS_PARENT_ROOT: &str =
    "0x0000000000000000000000000000000000000000000000000000000000000000";

pub async fn get_deposit_sum_from_block_root<'a>(
    executor: impl PgExecutor<'a>,
    block_root: &str,
) -> sqlx::Result<GweiNewtype> {
    let deposit_sum_aggregated = sqlx::query!(
        "
            SELECT deposit_sum_aggregated FROM beacon_blocks
            WHERE block_root = $1
        ",
        block_root
    )
    .fetch_one(executor)
    .await?
    .deposit_sum_aggregated
    .into();

    Ok(deposit_sum_aggregated)
}

pub async fn get_is_hash_known<'a>(executor: impl PgExecutor<'a>, block_root: &str) -> bool {
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

pub async fn store_block<'a>(
    executor: &mut PgConnection,
    state_root: &str,
    header: &BeaconHeaderSignedEnvelope,
    deposit_sum: &GweiNewtype,
    deposit_sum_aggregated: &GweiNewtype,
) {
    sqlx::query!(
        "
            INSERT INTO beacon_blocks (
                block_root,
                state_root,
                parent_root,
                deposit_sum,
                deposit_sum_aggregated
            )
            VALUES (
                $1, $2, $3, $4, $5
            )
        ",
        header.root,
        state_root,
        header.header.message.parent_root,
        i64::from(deposit_sum.to_owned()),
        i64::from(deposit_sum_aggregated.to_owned()),
    )
    .execute(executor)
    .await
    .unwrap();
}

pub async fn get_last_block_slot(connection: &mut PgConnection) -> Option<u32> {
    sqlx::query(
        "
            SELECT
                slot
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
    .map(|row: PgRow| row.get::<i32, _>("slot") as u32)
    .fetch_optional(connection)
    .await
    .unwrap()
}

pub async fn delete_blocks<'a>(connection: impl PgExecutor<'a>, greater_than_or_equal: &Slot) {
    sqlx::query!(
        "
            DELETE FROM beacon_blocks
            WHERE state_root IN (
                SELECT
                    state_root
                FROM
                    beacon_states
                WHERE slot >= $1
            )
        ",
        *greater_than_or_equal as i32
    )
    .execute(connection)
    .await
    .unwrap();
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        beacon_chain::{
            node::{BeaconHeader, BeaconHeaderEnvelope},
            states::store_state,
        },
        config,
    };
    use serial_test::serial;
    use sqlx::{Connection, PgConnection};

    #[tokio::test]
    async fn get_is_genesis_known_test() {
        let mut connection = PgConnection::connect(&config::get_db_url()).await.unwrap();
        let mut transaction = connection.begin().await.unwrap();

        let is_hash_known = get_is_hash_known(&mut transaction, GENESIS_PARENT_ROOT).await;

        assert_eq!(is_hash_known, true);
    }

    #[tokio::test]
    async fn get_is_hash_not_known_test() {
        let mut connection = PgConnection::connect(&config::get_db_url()).await.unwrap();
        let mut transaction = connection.begin().await.unwrap();

        let is_hash_known = get_is_hash_known(&mut transaction, "0xnot_there").await;

        assert_eq!(is_hash_known, false);
    }

    #[tokio::test]
    #[serial]
    async fn get_is_hash_known_test() {
        let mut connection = PgConnection::connect(&config::get_db_url()).await.unwrap();
        let mut transaction = connection.begin().await.unwrap();

        store_state(&mut transaction, "0xstate_root", &0)
            .await
            .unwrap();

        store_block(
            &mut transaction,
            "0xstate_root",
            &BeaconHeaderSignedEnvelope {
                root: "0xblock_root".to_string(),
                header: BeaconHeaderEnvelope {
                    message: BeaconHeader {
                        slot: 0,
                        parent_root: GENESIS_PARENT_ROOT.to_string(),
                        state_root: "0xstate_root".to_string(),
                    },
                },
            },
            &GweiNewtype(0),
            &GweiNewtype(0),
        )
        .await;

        let is_hash_known = get_is_hash_known(&mut transaction, "0xblock_root").await;

        assert_eq!(is_hash_known, true);
    }

    #[tokio::test]
    #[serial]
    async fn store_block_test() {
        let mut connection = PgConnection::connect(&config::get_db_url()).await.unwrap();
        let mut transaction = connection.begin().await.unwrap();

        store_state(&mut transaction, "0xstate_root", &0)
            .await
            .unwrap();

        store_block(
            &mut transaction,
            "0xstate_root",
            &BeaconHeaderSignedEnvelope {
                root: "0xblock_root".to_string(),
                header: BeaconHeaderEnvelope {
                    message: BeaconHeader {
                        slot: 0,
                        parent_root: GENESIS_PARENT_ROOT.to_string(),
                        state_root: "0xstate_root".to_string(),
                    },
                },
            },
            &GweiNewtype(0),
            &GweiNewtype(0),
        )
        .await;

        let is_hash_known = get_is_hash_known(&mut transaction, "0xblock_root").await;

        assert_eq!(is_hash_known, true);
    }

    #[tokio::test]
    async fn get_last_block_number_none_test() {
        let pool = sqlx::PgPool::connect(&config::get_db_url()).await.unwrap();
        let mut transaction = pool.begin().await.unwrap();

        let block_number = get_last_block_slot(&mut transaction).await;
        assert_eq!(block_number, None);
    }

    #[tokio::test]
    async fn get_last_block_number_some_test() {
        let pool = sqlx::PgPool::connect(&config::get_db_url()).await.unwrap();
        let mut transaction = pool.begin().await.unwrap();

        store_state(&mut transaction, "0xstate_root", &0)
            .await
            .unwrap();

        store_block(
            &mut transaction,
            "0xstate_root",
            &BeaconHeaderSignedEnvelope {
                root: "0xblock_root".to_string(),
                header: BeaconHeaderEnvelope {
                    message: BeaconHeader {
                        slot: 0,
                        parent_root: GENESIS_PARENT_ROOT.to_string(),
                        state_root: "0xstate_root".to_string(),
                    },
                },
            },
            &GweiNewtype(0),
            &GweiNewtype(0),
        )
        .await;

        let block_slot = get_last_block_slot(&mut transaction).await;
        assert_eq!(block_slot, Some(0));
    }

    #[tokio::test]
    async fn delete_block_test() {
        let pool = sqlx::PgPool::connect(&config::get_db_url()).await.unwrap();
        let mut transaction = pool.begin().await.unwrap();

        store_state(&mut transaction, "0xstate_root", &0)
            .await
            .unwrap();

        store_block(
            &mut transaction,
            "0xstate_root",
            &BeaconHeaderSignedEnvelope {
                root: "0xblock_root".to_string(),
                header: BeaconHeaderEnvelope {
                    message: BeaconHeader {
                        slot: 0,
                        parent_root: GENESIS_PARENT_ROOT.to_string(),
                        state_root: "0xstate_root".to_string(),
                    },
                },
            },
            &GweiNewtype(0),
            &GweiNewtype(0),
        )
        .await;

        let block_slot = get_last_block_slot(&mut transaction).await;
        assert_eq!(block_slot, Some(0));

        delete_blocks(&mut transaction, &0).await;

        let block_slot = get_last_block_slot(&mut transaction).await;
        assert_eq!(block_slot, None);
    }
}
