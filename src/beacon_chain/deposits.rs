use anyhow::Result;
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgRow;
use sqlx::{PgExecutor, Row};

use crate::units::GweiNewtype;

use super::node::BeaconBlock;
use super::{blocks, Slot};

/// Returns the sum of all **deposit *requests*** (EIP-6110 execution layer deposits)
/// contained in the given block.
///
/// From the Pectra (Electra) hard-fork onwards, new validator funding happens via
/// the `execution_requests.deposits[*]` list that lives inside
/// `block.body.execution_requests`. These requests are later credited on the
/// consensus layer after passing through the `pending_deposits` queue, so the
/// *receipt* list `block.deposits()` must **not** be counted here – doing so
/// would double-subtract the value in the issuance formula.
///
/// For pre-Pectra history (where `execution_requests` is absent) the list is
/// empty and we deliberately return zero; the legacy consensus deposit receipts
/// are already reflected in `beacon_balances` and therefore cancel out without
/// needing an explicit subtraction.
pub fn get_deposit_sum_from_block(block: &BeaconBlock) -> GweiNewtype {
    block
        .execution_request_deposits()
        .iter()
        .fold(GweiNewtype(0), |sum, deposit| sum + deposit.amount)
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
            &GweiNewtype(0),
            &GweiNewtype(1),
            &GweiNewtype(0),
            &GweiNewtype(1),
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
}
