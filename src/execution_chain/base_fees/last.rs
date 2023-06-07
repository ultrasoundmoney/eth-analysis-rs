use sqlx::PgPool;
use tracing::debug;

use crate::{
    caching::{self, CacheKey},
    execution_chain::{base_fees::BaseFeePerGas, ExecutionNodeBlock},
};

pub async fn update_last_base_fee(db_pool: &PgPool, block: &ExecutionNodeBlock) {
    debug!("updating current base fee");

    let base_fee_per_gas = BaseFeePerGas {
        timestamp: block.timestamp,
        wei: block.base_fee_per_gas,
    };

    caching::update_and_publish(db_pool, &CacheKey::BaseFeePerGas, base_fee_per_gas).await;
}
