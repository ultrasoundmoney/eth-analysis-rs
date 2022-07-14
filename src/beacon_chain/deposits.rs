use serde::{Deserialize, Serialize};
use sqlx::postgres::PgRow;
use sqlx::{PgExecutor, Row};

use crate::eth_units::{to_gwei_string, GweiAmount};

use super::blocks::get_deposit_sum_from_block_root;
use super::node::BeaconBlock;
use super::states::Slot;

// Obtained by getting validator balances for slot 0, which had zero deposits.
pub const INITIAL_DEPOSITS: GweiAmount = GweiAmount(674144000000000);

pub fn get_deposit_sum_from_block(block: &BeaconBlock) -> GweiAmount {
    block
        .body
        .deposits
        .iter()
        .fold(GweiAmount(0), |sum, deposit| sum + deposit.data.amount)
}

pub async fn get_deposit_sum_aggregated<'a>(
    executor: impl PgExecutor<'a>,
    block: &BeaconBlock,
) -> sqlx::Result<GweiAmount> {
    let parent_deposit_sum_aggregated = if block.slot == 0 {
        GweiAmount(0)
    } else {
        get_deposit_sum_from_block_root(executor, &block.parent_root).await?
    };

    let deposit_sum_aggregated = parent_deposit_sum_aggregated + get_deposit_sum_from_block(&block);

    Ok(deposit_sum_aggregated)
}

#[derive(Debug, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct BeaconDepositsSum {
    #[serde(serialize_with = "to_gwei_string")]
    deposits_sum: GweiAmount,
    slot: Slot,
}

pub async fn get_deposits_sum<'a>(executor: impl PgExecutor<'a>) -> BeaconDepositsSum {
    sqlx::query(
        "
            SELECT slot, deposit_sum_aggregated FROM beacon_states
            JOIN beacon_blocks ON beacon_blocks.state_root = beacon_states.state_root
            ORDER BY beacon_states.slot DESC
            LIMIT 1
        ",
    )
    .map(|row: PgRow| {
        let slot = row.get::<i32, _>("slot") as u32;
        let deposits_sum = row.get::<i64, _>("deposit_sum_aggregated").into();
        BeaconDepositsSum { deposits_sum, slot }
    })
    .fetch_one(executor)
    .await
    .unwrap()
}

#[cfg(test)]
mod tests {
    use super::super::blocks::store_block;
    use super::super::states::store_state;
    use super::*;
    use crate::beacon_chain::blocks::GENESIS_PARENT_ROOT;
    use crate::beacon_chain::node::BeaconHeader;
    use crate::beacon_chain::node::BeaconHeaderEnvelope;
    use crate::beacon_chain::node::BeaconHeaderSignedEnvelope;
    use crate::config;
    use serial_test::serial;
    use sqlx::{Connection, PgConnection};

    #[tokio::test]
    #[serial]
    async fn get_deposits_sum_test() {
        let mut connection = PgConnection::connect(&config::get_db_url()).await.unwrap();
        let mut transaction = connection.begin().await.unwrap();

        store_state(&mut transaction, "0xstate_root", &0)
            .await
            .unwrap();

        store_block(
            &mut transaction,
            "0xstate_root",
            &BeaconHeaderSignedEnvelope {
                root: "0xblock_root".to_string(),
                header: BeaconHeaderEnvelope {
                    message: BeaconHeader {
                        slot: 0,
                        parent_root: GENESIS_PARENT_ROOT.to_string(),
                        state_root: "0xstate_root".to_string(),
                    },
                },
            },
            &GweiAmount(0),
            &GweiAmount(1),
        )
        .await;

        let deposits_sum = get_deposits_sum(&mut transaction).await;

        assert_eq!(
            deposits_sum,
            BeaconDepositsSum {
                deposits_sum: GweiAmount(1),
                slot: 0
            }
        );
    }
}
