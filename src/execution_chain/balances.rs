use serde::{Deserialize, Serialize};
use sqlx::{postgres::PgRow, PgExecutor, Row};
use std::str::FromStr;

use crate::eth_units::Wei;

#[derive(Debug, Deserialize, PartialEq, Serialize)]
pub struct ExecutionBalancesSum {
    pub block_number: u32,
    pub balances_sum: Wei,
}

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

#[cfg(test)]
mod tests {
    use serial_test::serial;
    use sqlx::{Connection, PgConnection};

    use super::super::supply_deltas::store_delta;
    use super::*;
    use crate::config;
    use crate::execution_chain::SupplyDelta;

    #[tokio::test]
    #[serial]
    async fn get_balances_sum_test() {
        let mut connection = PgConnection::connect(&config::get_db_url()).await.unwrap();
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

        store_delta(&mut transaction, &supply_delta_test).await;
        let total_supply = get_balances_sum(&mut transaction).await;

        assert_eq!(
            total_supply,
            ExecutionBalancesSum {
                block_number: 0,
                balances_sum: 1,
            }
        );
    }
}