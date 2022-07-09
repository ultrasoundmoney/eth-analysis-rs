use sqlx::{PgConnection, PgExecutor, PgPool, Row};

use crate::eth_units::GweiAmount;

use super::node::BeaconHeaderSignedEnvelope;

const GENESIS_PARENT_ROOT: &str =
    "0x0000000000000000000000000000000000000000000000000000000000000000";

pub async fn get_deposit_sum_from_block_root(
    pool: &PgPool,
    block_root: &str,
) -> sqlx::Result<GweiAmount> {
    let deposit_sum_aggregated = sqlx::query!(
        "
            SELECT deposit_sum_aggregated FROM beacon_blocks
            WHERE block_root = $1
        ",
        block_root
    )
    .fetch_one(pool)
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
}
