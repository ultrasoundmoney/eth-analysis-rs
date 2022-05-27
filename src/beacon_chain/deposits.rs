use sqlx::PgPool;

use super::{blocks, node::BeaconBlock, GweiAmount};

pub fn get_deposit_sum_from_block(block: &BeaconBlock) -> GweiAmount {
    block
        .body
        .deposits
        .iter()
        .fold(GweiAmount(0), |sum, deposit| sum + deposit.data.amount)
}

pub async fn get_deposit_sum_aggregated(
    pool: &PgPool,
    block: &BeaconBlock,
) -> sqlx::Result<GweiAmount> {
    let parent_deposit_sum_aggregated = if block.slot == 0 {
        GweiAmount(0)
    } else {
        blocks::get_deposit_sum_from_block_root(pool, &block.parent_root).await?
    };

    let deposit_sum_aggregated = parent_deposit_sum_aggregated + get_deposit_sum_from_block(&block);

    Ok(deposit_sum_aggregated)
}
