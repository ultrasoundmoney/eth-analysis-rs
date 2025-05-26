use anyhow::Result;
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgRow;
use sqlx::{PgExecutor, Row};

use crate::units::GweiNewtype;

use super::node::BeaconBlock;
use super::{blocks, Slot};

pub fn get_deposit_sum_from_block(block: &BeaconBlock) -> GweiNewtype {
    let consensus_deposits_sum = block
        .deposits()
        .iter()
        .fold(GweiNewtype(0), |sum, deposit| sum + deposit.amount);

    let execution_request_deposits_sum = block
        .execution_request_deposits()
        .iter()
        .fold(GweiNewtype(0), |sum, deposit| sum + deposit.amount);

    consensus_deposits_sum + execution_request_deposits_sum
}

pub async fn get_deposit_sum_aggregated(
    executor: impl PgExecutor<'_>,
    block: &BeaconBlock,
) -> GweiNewtype {
    let parent_deposit_sum_aggregated = if block.slot == Slot::GENESIS {
        GweiNewtype(0)
    } else {
        blocks::get_deposit_sum_from_block_root(executor, &block.parent_root).await
    };

    parent_deposit_sum_aggregated + get_deposit_sum_from_block(block)
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct BeaconDepositsSum {
    pub deposits_sum: GweiNewtype,
    pub slot: Slot,
}

pub async fn get_deposits_sum_by_state_root(
    executor: impl PgExecutor<'_>,
    state_root: &str,
) -> Result<Option<GweiNewtype>> {
    let deposit_sum_aggregated = sqlx::query(
        "
            SELECT
                deposit_sum_aggregated
            FROM
                beacon_blocks
            WHERE
                state_root = $1
        ",
    )
    .bind(state_root)
    .map(|row: PgRow| row.get::<i64, _>("deposit_sum_aggregated").into())
    .fetch_optional(executor)
    .await?;

    Ok(deposit_sum_aggregated)
}

#[cfg(test)]
mod tests {
    use sqlx::Acquire;

    use crate::{
        beacon_chain::{
            store_block, store_state, BeaconBlockBuilder, BeaconHeaderSignedEnvelopeBuilder,
            StoreBlockParams,
        },
        db,
    };

    use super::*;

    #[tokio::test]
    async fn get_deposits_sum_by_state_root_test() {
        let mut connection = db::tests::get_test_db_connection().await;
        let mut transaction = connection.begin().await.unwrap();

        let test_id = "get_deposits_sum_by_state_root";
        let test_header = BeaconHeaderSignedEnvelopeBuilder::new(test_id).build();
        let test_block = Into::<BeaconBlockBuilder>::into(&test_header).build();

        store_state(
            &mut *transaction,
            &test_header.state_root(),
            test_header.slot(),
        )
        .await;

        store_block(
            &mut *transaction,
            &test_block,
            StoreBlockParams {
                deposit_sum: GweiNewtype(0),
                deposit_sum_aggregated: GweiNewtype(1),
                withdrawal_sum: GweiNewtype(0),
                withdrawal_sum_aggregated: GweiNewtype(1),
                pending_deposits_sum: None,
            },
            &test_header,
        )
        .await;

        let deposits_sum =
            get_deposits_sum_by_state_root(&mut *transaction, &test_header.state_root())
                .await
                .unwrap()
                .unwrap();

        assert_eq!(GweiNewtype(1), deposits_sum);
    }

    #[test]
    fn get_deposit_sum_from_block_no_deposits_test() {
        let block = BeaconBlockBuilder::default().build();
        assert_eq!(get_deposit_sum_from_block(&block), GweiNewtype(0));
    }

    #[test]
    fn get_deposit_sum_from_block_only_consensus_deposits_test() {
        let block = BeaconBlockBuilder::default()
            .deposits(vec![GweiNewtype(10), GweiNewtype(20)])
            .build();
        assert_eq!(get_deposit_sum_from_block(&block), GweiNewtype(30));
    }

    #[test]
    fn get_deposit_sum_from_block_only_execution_request_deposits_test() {
        let block = BeaconBlockBuilder::default()
            .execution_request_deposits(vec![GweiNewtype(5), GweiNewtype(15)])
            .build();
        assert_eq!(get_deposit_sum_from_block(&block), GweiNewtype(20));
    }

    #[test]
    fn get_deposit_sum_from_block_both_deposit_types_test() {
        let block = BeaconBlockBuilder::default()
            .deposits(vec![GweiNewtype(10), GweiNewtype(20)]) // 30
            .execution_request_deposits(vec![GweiNewtype(5), GweiNewtype(15)]) // 20
            .build();
        assert_eq!(get_deposit_sum_from_block(&block), GweiNewtype(50));
    }

    #[test]
    fn get_deposit_sum_from_block_empty_consensus_deposits_with_execution_test() {
        let block = BeaconBlockBuilder::default()
            .deposits(vec![])
            .execution_request_deposits(vec![GweiNewtype(5), GweiNewtype(15)])
            .build();
        assert_eq!(get_deposit_sum_from_block(&block), GweiNewtype(20));
    }

    #[test]
    fn get_deposit_sum_from_block_empty_execution_deposits_with_consensus_test() {
        let block = BeaconBlockBuilder::default()
            .deposits(vec![GweiNewtype(10), GweiNewtype(20)])
            .execution_request_deposits(vec![])
            .build();
        assert_eq!(get_deposit_sum_from_block(&block), GweiNewtype(30));
    }
}
