//! Module responsible for storing and retrieving the account balances on the execution chain.
//! Mainly used to calculate the eth supply for any given slot.
//! TODO: Database table is referred to as execution_supply, to be more consistent with the beacon
//! chain it would be nice to term this execution_balances_sum.
use anyhow::Result;
use serde::Serialize;
use sqlx::{postgres::PgRow, PgExecutor, Row};

use crate::units::WeiNewtype;

use super::{BlockHash, BlockNumber};

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ExecutionBalancesSum {
    pub block_number: BlockNumber,
    pub balances_sum: WeiNewtype,
}

#[derive(Debug, PartialEq, Eq)]
pub struct ExecutionSupply {
    pub block_number: BlockNumber,
    pub block_hash: BlockHash,
    pub balances_sum: WeiNewtype,
}

pub async fn get_execution_balances_by_hash(
    executor: impl PgExecutor<'_>,
    block_hash: &str,
) -> Result<ExecutionSupply> {
    let row = sqlx::query(
        "
            SELECT
                balances_sum::TEXT,
                block_hash,
                block_number
            FROM
                execution_supply
            WHERE
                block_hash = $1
        ",
    )
    .bind(block_hash)
    .map(|row: PgRow| {
        let balances_sum: WeiNewtype = (row.get::<String, _>("balances_sum")).parse().unwrap();
        let block_hash = row.get::<String, _>("block_hash");
        let block_number = row.get::<i32, _>("block_number");

        ExecutionSupply {
            balances_sum,
            block_hash,
            block_number,
        }
    })
    .fetch_one(executor)
    .await?;

    Ok(row)
}

#[cfg(test)]
mod tests {
    use sqlx::Connection;

    use super::*;
    use crate::beacon_chain::tests::store_custom_test_block;
    use crate::beacon_chain::{BeaconBlockBuilder, BeaconHeaderSignedEnvelopeBuilder, Slot};
    use crate::db;
    use crate::execution_chain::supply_deltas::add_delta;
    use crate::execution_chain::SupplyDelta;

    #[tokio::test]
    async fn get_execution_supply_by_hash_test() {
        let mut connection = db::tests::get_test_db_connection().await;
        let mut transaction = connection.begin().await.unwrap();

        let test_id = "get_balances_by_hash";
        let block_hash = format!("0x{test_id}_block_hash");
        let header = BeaconHeaderSignedEnvelopeBuilder::new(test_id)
            .slot(Slot(10))
            .build();
        let block = Into::<BeaconBlockBuilder>::into(&header)
            .block_hash(&block_hash)
            .build();

        store_custom_test_block(&mut transaction, &header, &block).await;

        let supply_delta_test = SupplyDelta {
            supply_delta: 1,
            block_number: 0,
            block_hash: block_hash.clone(),
            fee_burn: 0,
            fixed_reward: 0,
            parent_hash: "0xtestparent".to_string(),
            self_destruct: 0,
            uncles_reward: 0,
        };

        add_delta(&mut transaction, &supply_delta_test).await;

        let balances = get_execution_balances_by_hash(&mut *transaction, &block_hash)
            .await
            .unwrap();

        assert_eq!(
            ExecutionSupply {
                block_hash,
                block_number: 0,
                balances_sum: WeiNewtype(1)
            },
            balances
        );
    }
}
