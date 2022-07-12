use sqlx::{PgConnection, PgExecutor, Row};

use crate::eth_units::GweiAmount;

use super::node::BeaconHeaderSignedEnvelope;

pub const GENESIS_PARENT_ROOT: &str =
    "0x0000000000000000000000000000000000000000000000000000000000000000";

pub async fn get_deposit_sum_from_block_root<'a>(
    executor: impl PgExecutor<'a>,
    block_root: &str,
) -> sqlx::Result<GweiAmount> {
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

pub async fn get_is_hash_known<'a>(executor: impl PgExecutor<'a>, hash: &str) -> bool {
    if hash == GENESIS_PARENT_ROOT {
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
    .bind(hash)
    .fetch_one(executor)
    .await
    .unwrap()
    .get("exists")
}

pub async fn store_block<'a>(
    executor: &mut PgConnection,
    state_root: &str,
    header: &BeaconHeaderSignedEnvelope,
    deposit_sum: &GweiAmount,
    deposit_sum_aggregated: &GweiAmount,
) {
    let parent_root = if header.header.message.parent_root == GENESIS_PARENT_ROOT {
        None
    } else {
        Some(header.header.message.parent_root.clone())
    };

    sqlx::query!(
        "
            INSERT INTO beacon_blocks (
                block_root,
                state_root,
                parent_root,
                deposit_sum,
                deposit_sum_aggregated
            ) VALUES ($1, $2, $3, $4, $5)
        ",
        header.root,
        state_root,
        parent_root,
        i64::from(deposit_sum.to_owned()),
        i64::from(deposit_sum_aggregated.to_owned()),
    )
    .execute(executor)
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

    async fn clean_tables<'a>(pg_exec: impl PgExecutor<'a>) {
        sqlx::query("TRUNCATE beacon_states CASCADE")
            .execute(pg_exec)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn get_is_genesis_known_test() {
        let mut connection: PgConnection = sqlx::Connection::connect(&config::get_db_url())
            .await
            .unwrap();

        let is_hash_known = get_is_hash_known(&mut connection, GENESIS_PARENT_ROOT).await;

        assert_eq!(is_hash_known, true);
    }

    #[tokio::test]
    async fn get_is_hash_not_known_test() {
        let mut connection: PgConnection = sqlx::Connection::connect(&config::get_db_url())
            .await
            .unwrap();

        let is_hash_known = get_is_hash_known(&mut connection, "0xnot_there").await;

        assert_eq!(is_hash_known, false);
    }

    #[tokio::test]
    #[serial]
    async fn get_is_hash_known_test() {
        let mut connection: PgConnection = sqlx::Connection::connect(&config::get_db_url())
            .await
            .unwrap();

        store_state(&mut connection, "0xstate_root", &0)
            .await
            .unwrap();

        store_block(
            &mut connection,
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
            &GweiAmount(0),
            &GweiAmount(0),
        )
        .await;

        let is_hash_known = get_is_hash_known(&mut connection, "0xblock_root").await;

        clean_tables(&mut connection).await;

        assert_eq!(is_hash_known, true);
    }

    #[tokio::test]
    #[serial]
    async fn store_block_test() {
        let mut connection: PgConnection = sqlx::Connection::connect(&config::get_db_url())
            .await
            .unwrap();

        store_state(&mut connection, "0xstate_root", &0)
            .await
            .unwrap();

        store_block(
            &mut connection,
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
            &GweiAmount(0),
            &GweiAmount(0),
        )
        .await;

        let is_hash_known = get_is_hash_known(&mut connection, "0xblock_root").await;

        clean_tables(&mut connection).await;

        assert_eq!(is_hash_known, true);
    }
}
