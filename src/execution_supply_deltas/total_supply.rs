use sqlx::{FromRow, PgExecutor};

use crate::total_supply::SupplyAtBlock;

#[derive(FromRow)]
struct ExecutionSupplyAtBlock {
    pub block_number: i32,
    pub total_supply: i64,
}

impl From<ExecutionSupplyAtBlock> for SupplyAtBlock {
    fn from(row: ExecutionSupplyAtBlock) -> Self {
        Self {
            block_number: row.block_number as u32,
            total_supply: row.total_supply as u64,
        }
    }
}

pub async fn get_latest_total_supply<'a>(executor: impl PgExecutor<'a>) -> SupplyAtBlock {
    let row: ExecutionSupplyAtBlock = sqlx::query_as(
        "
            SELECT block_number, total_supply FROM execution_supply
            ORDER BY block_number DESC
            LIMIT 1
        ",
    )
    .fetch_one(executor)
    .await
    .unwrap();

    row.into()
}

#[cfg(test)]
mod tests {
    use serial_test::serial;
    use sqlx::PgConnection;

    use super::super::sync::store_delta;
    use super::*;
    use crate::config;
    use crate::execution_node::SupplyDelta;

    async fn clean_tables<'a>(executor: impl PgExecutor<'a>) {
        sqlx::query("TRUNCATE execution_supply_deltas CASCADE")
            .execute(executor)
            .await
            .unwrap();
    }

    #[tokio::test]
    #[serial]
    async fn get_latest_total_supply_test() {
        let mut connection: PgConnection = sqlx::Connection::connect(&config::get_db_url())
            .await
            .unwrap();

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

        store_delta(&mut connection, &supply_delta_test).await;
        let total_supply = get_latest_total_supply(&mut connection).await;

        clean_tables(&mut connection).await;

        assert_eq!(
            total_supply,
            SupplyAtBlock {
                block_number: 0,
                total_supply: 1,
            }
        );
    }
}
