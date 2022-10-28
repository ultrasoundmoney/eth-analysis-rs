use chrono::{DateTime, Utc};
use serde::Serialize;
use sqlx::PgConnection;
use sqlx::{postgres::PgRow, PgExecutor, Row};
use std::str::FromStr;

use crate::eth_units::Wei;
use crate::json_codecs::to_i128_string;

use super::BlockNumber;

#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ExecutionBalancesSum {
    pub block_number: BlockNumber,
    #[serde(serialize_with = "to_i128_string")]
    pub balances_sum: Wei,
}

#[allow(dead_code)]
pub async fn get_balances_sum<'a>(executor: impl PgExecutor<'a>) -> ExecutionBalancesSum {
    sqlx::query(
        "
            SELECT block_number, balances_sum::TEXT FROM execution_supply
            ORDER BY block_number DESC
            LIMIT 1
        ",
    )
    .map(|row: PgRow| {
        let block_number = row.get::<i32, _>("block_number") as u32;
        let balances_sum = i128::from_str(row.get("balances_sum")).unwrap();
        ExecutionBalancesSum {
            block_number,
            balances_sum,
        }
    })
    .fetch_one(executor)
    .await
    .unwrap()
}

pub async fn get_closest_balances_sum(
    executor: impl PgExecutor<'_>,
    point_in_time: DateTime<Utc>,
) -> sqlx::Result<ExecutionBalancesSum> {
    sqlx::query(
        "
        SELECT
            block_number, balances_sum::TEXT
        FROM
            execution_supply
        JOIN
            blocks_next ON execution_supply.block_number = blocks_next.number
        ORDER BY ABS(EXTRACT(epoch FROM (blocks_next.timestamp - $1)))
    ",
    )
    .bind(point_in_time)
    .map(|row: PgRow| {
        let block_number = row.get::<i32, _>("block_number") as u32;
        let balances_sum = i128::from_str(row.get("balances_sum")).unwrap();
        ExecutionBalancesSum {
            block_number,
            balances_sum,
        }
    })
    .fetch_one(executor)
    .await
}

#[cfg(test)]
mod tests {
    use anyhow::Result;
    use chrono::{Duration, SubsecRound};
    use sqlx::Connection;

    use super::*;
    use crate::db;
    use crate::execution_chain::supply_deltas::add_delta;
    use crate::execution_chain::{block_store, ExecutionNodeBlock, SupplyDelta};

    // Replace with shared testing helper that helps easily build the right mock block.
    fn make_test_block() -> ExecutionNodeBlock {
        ExecutionNodeBlock {
            base_fee_per_gas: 0,
            difficulty: 0,
            gas_used: 0,
            hash: "0xtest".to_string(),
            number: 0,
            parent_hash: "0xparent".to_string(),
            timestamp: Utc::now().trunc_subsecs(0),
            total_difficulty: 10,
        }
    }

    #[tokio::test]
    async fn get_balances_sum_test() {
        let mut connection = db::get_test_db().await;
        let mut transaction = connection.begin().await.unwrap();

        let supply_delta_test = SupplyDelta {
            supply_delta: 1,
            block_number: 0,
            block_hash: "0xtest".to_string(),
            fee_burn: 0,
            fixed_reward: 0,
            parent_hash: "0xtestparent".to_string(),
            self_destruct: 0,
            uncles_reward: 0,
        };

        add_delta(&mut transaction, &supply_delta_test).await;
        let execution_balances_sum = get_balances_sum(&mut transaction).await;

        assert_eq!(
            execution_balances_sum,
            ExecutionBalancesSum {
                block_number: 0,
                balances_sum: 1,
            }
        );
    }

    #[tokio::test]
    async fn get_closest_balances_sum_test() -> Result<()> {
        let mut connection = db::get_test_db().await;
        let mut transaction = connection.begin().await.unwrap();

        let test_block_close = ExecutionNodeBlock {
            timestamp: Utc::now().trunc_subsecs(0) - Duration::hours(1),
            ..make_test_block()
        };
        let test_block_far = ExecutionNodeBlock {
            timestamp: Utc::now().trunc_subsecs(0),
            hash: "0xtest2".to_string(),
            number: 1,
            parent_hash: "0xtest".to_string(),
            ..make_test_block()
        };

        let mut block_store = block_store::BlockStore::new(&mut transaction);
        block_store.store_block(&test_block_close, 0.0).await;
        block_store.store_block(&test_block_far, 0.0).await;

        let supply_delta_close = SupplyDelta {
            supply_delta: 1,
            block_number: 0,
            block_hash: "0xtest".to_string(),
            fee_burn: 0,
            fixed_reward: 0,
            parent_hash: "0xtestparent".to_string(),
            self_destruct: 0,
            uncles_reward: 0,
        };

        let supply_delta_far = SupplyDelta {
            supply_delta: 2,
            block_number: 1,
            block_hash: "0xtest2".to_string(),
            fee_burn: 0,
            fixed_reward: 0,
            parent_hash: "0xtest".to_string(),
            self_destruct: 0,
            uncles_reward: 0,
        };

        add_delta(&mut transaction, &supply_delta_close).await;
        add_delta(&mut transaction, &supply_delta_far).await;

        let execution_balances_sum =
            get_closest_balances_sum(&mut transaction, test_block_close.timestamp).await?;

        assert_eq!(
            execution_balances_sum,
            ExecutionBalancesSum {
                block_number: 0,
                balances_sum: 1,
            }
        );

        Ok(())
    }
}
