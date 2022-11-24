use serde::{Deserialize, Serialize};
use sqlx::postgres::PgRow;
use sqlx::{PgExecutor, Row};

use crate::eth_units::{to_gwei_string, GweiNewtype};

use super::blocks::get_deposit_sum_from_block_root;
use super::node::BeaconBlock;
use super::states::Slot;

pub fn get_deposit_sum_from_block(block: &BeaconBlock) -> GweiNewtype {
    block
        .body
        .deposits
        .iter()
        .fold(GweiNewtype(0), |sum, deposit| sum + deposit.data.amount)
}

pub async fn get_deposit_sum_aggregated<'a>(
    executor: impl PgExecutor<'a>,
    block: &BeaconBlock,
) -> sqlx::Result<GweiNewtype> {
    let parent_deposit_sum_aggregated = if block.slot == 0 {
        GweiNewtype(0)
    } else {
        get_deposit_sum_from_block_root(executor, &block.parent_root).await?
    };

    let deposit_sum_aggregated = parent_deposit_sum_aggregated + get_deposit_sum_from_block(&block);

    Ok(deposit_sum_aggregated)
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct BeaconDepositsSum {
    #[serde(serialize_with = "to_gwei_string")]
    pub deposits_sum: GweiNewtype,
    pub slot: Slot,
}

pub async fn get_deposits_sum(executor: impl PgExecutor<'_>) -> BeaconDepositsSum {
    sqlx::query(
        "
            SELECT beacon_states.slot, deposit_sum_aggregated FROM beacon_states
            JOIN beacon_blocks ON beacon_blocks.state_root = beacon_states.state_root
            ORDER BY beacon_states.slot DESC
            LIMIT 1
        ",
    )
    .map(|row: PgRow| {
        let slot = row.get::<i32, _>("slot") as u32;
        let deposits_sum = row
            .get::<i64, _>("deposit_sum_aggregated")
            .try_into()
            .unwrap();
        BeaconDepositsSum { deposits_sum, slot }
    })
    .fetch_one(executor)
    .await
    .unwrap()
}

#[cfg(test)]
mod tests {
    use sqlx::Acquire;

    use crate::{
        beacon_chain::{
            store_block, store_state,
            tests::{get_test_beacon_block, get_test_header},
            GENESIS_PARENT_ROOT,
        },
        db,
    };

    use super::*;

    #[tokio::test]
    async fn get_deposits_sum_test() {
        let mut connection = db::get_test_db().await;
        let mut transaction = connection.begin().await.unwrap();

        let test_id = "get_deposits_sum";
        let state_root = format!("0x{test_id}_state_root");
        let slot = 0;
        let test_header = get_test_header(test_id, &slot, GENESIS_PARENT_ROOT);
        let test_block = get_test_beacon_block(&state_root, &slot, GENESIS_PARENT_ROOT);

        store_state(&mut transaction, &state_root, &0, "")
            .await
            .unwrap();

        store_block(
            &mut transaction,
            &test_block,
            &GweiNewtype(0),
            &GweiNewtype(1),
            &test_header,
        )
        .await
        .unwrap();

        let deposits_sum = get_deposits_sum(&mut transaction).await;

        assert_eq!(
            deposits_sum,
            BeaconDepositsSum {
                deposits_sum: GweiNewtype(1),
                slot: 0
            }
        );
    }
}
