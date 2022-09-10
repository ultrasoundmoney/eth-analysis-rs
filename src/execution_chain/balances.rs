use serde::Serialize;
use sqlx::{postgres::PgRow, PgExecutor, Row};
use std::str::FromStr;

use crate::eth_units::Wei;
use crate::json_codecs::to_i128_string;

#[derive(Debug, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ExecutionBalancesSum {
    pub block_number: u32,
    #[serde(serialize_with = "to_i128_string")]
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
    use sqlx::Connection;

    use super::super::supply_deltas::add_delta;
    use super::*;
    use crate::db_testing;
    use crate::execution_chain::SupplyDelta;

    #[tokio::test]
    async fn get_balances_sum_test() {
        let mut connection = db_testing::get_test_db().await;
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
}
