use crate::total_supply::SupplyAtBlock;
use sqlx::FromRow;
use sqlx::PgExecutor;

#[derive(FromRow)]
struct BeaconSupplyAtBlockRow {
    pub slot: i32,
    pub total_supply: i64,
}

impl From<BeaconSupplyAtBlockRow> for SupplyAtBlock {
    fn from(row: BeaconSupplyAtBlockRow) -> Self {
        Self {
            block_number: row.slot as u32,
            total_supply: row.total_supply as u64,
        }
    }
}

pub async fn get_latest_total_supply<'a>(executor: impl PgExecutor<'a>) -> SupplyAtBlock {
    let row: BeaconSupplyAtBlockRow = sqlx::query_as(
        "
            SELECT
                beacon_states.slot,
                beacon_issuance.gwei AS total_supply
            FROM beacon_issuance
            JOIN beacon_states ON beacon_states.state_root = beacon_issuance.state_root
            ORDER BY beacon_states.slot DESC
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

    use super::*;
    use crate::beacon_chain::beacon_time::FirstOfDaySlot;
    use crate::beacon_chain::issuance::store_issuance_for_day;
    use crate::beacon_chain::states::store_state;
    use crate::config;
    use crate::eth_units::GweiAmount;

    async fn clean_tables<'a>(pg_exec: impl PgExecutor<'a>) {
        sqlx::query("TRUNCATE beacon_states CASCADE")
            .execute(pg_exec)
            .await
            .unwrap();
    }

    #[tokio::test]
    #[serial]
    async fn get_latest_total_supply_test() {
        let mut connection: PgConnection = sqlx::Connection::connect(&config::get_db_url())
            .await
            .unwrap();

        store_state(&mut connection, "0xtest_issuance_1", &3599)
            .await
            .unwrap();

        store_issuance_for_day(
            &mut connection,
            "0xtest_issuance_1",
            &FirstOfDaySlot::new(&3599).unwrap(),
            &GweiAmount::new(1),
        )
        .await;

        let total_supply = get_latest_total_supply(&mut connection).await;

        clean_tables(&mut connection).await;

        assert_eq!(
            total_supply,
            SupplyAtBlock {
                block_number: 3599,
                total_supply: 1,
            }
        );
    }
}
