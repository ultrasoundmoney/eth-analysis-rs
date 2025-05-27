use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::PgExecutor;

use crate::units::{GweiImprecise, GweiNewtype};

use super::{BeaconNode, Slot};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EffectiveBalanceSum {
    // This amount is larger than 9M ETH, so we lose precision when serializing. For now, we don't
    // care.
    pub sum: GweiImprecise,
    pub slot: Slot,
    pub timestamp: DateTime<Utc>,
}

impl EffectiveBalanceSum {
    pub fn new(slot: Slot, sum: GweiNewtype) -> Self {
        Self {
            sum: sum.into(),
            slot,
            timestamp: slot.date_time(),
        }
    }
}

pub async fn get_effective_balance_sum(
    beacon_node: &impl BeaconNode,
    state_root: &str,
) -> GweiNewtype {
    beacon_node
        .get_validators_by_state(state_root)
        .await
        .unwrap()
        .iter()
        .filter(|validator_envelope| validator_envelope.is_active())
        // Sum the effective balance of all validators in the state.
        .fold(GweiNewtype(0), |sum, validator_envelope| {
            sum + validator_envelope.effective_balance()
        })
}

pub async fn store_effective_balance_sum(
    executor: impl PgExecutor<'_>,
    state_root: &str,
    sum: &GweiNewtype,
) {
    sqlx::query!(
        "
        UPDATE
            beacon_states
        SET
            effective_balance_sum = $1
        WHERE
            state_root = $2
        ",
        sum.0,
        state_root
    )
    .execute(executor)
    .await
    .unwrap();
}

#[cfg(test)]
mod tests {
    use test_context::test_context;

    use crate::{
        beacon_chain::{
            self,
            node::{Validator, ValidatorEnvelope},
            MockBeaconNode,
        },
        db::tests::TestDb,
        units::GweiNewtype,
    };

    use super::*;

    const SLOT_0_STATE_ROOT: &str =
        "0x7e76880eb67bbdc86250aa578958e9d0675e64e714337855204fb5abaaf82c2b";

    #[tokio::test]
    async fn test_get_effective_balance_sum() {
        let mut mock_beacon_node = MockBeaconNode::new();
        let validator_envelopes = vec![
            ValidatorEnvelope {
                status: "active_ongoing".to_string(),
                validator: Validator {
                    effective_balance: GweiNewtype(32_000_000_000_000_000),
                },
            },
            ValidatorEnvelope {
                status: "active_ongoing".to_string(),
                validator: Validator {
                    effective_balance: GweiNewtype(32_000_000_000_000_000),
                },
            },
        ];
        mock_beacon_node
            .expect_get_validators_by_state()
            .return_once(move |_| Ok(validator_envelopes));
        let state_root = SLOT_0_STATE_ROOT.to_string();
        let expected_sum = GweiNewtype(64_000_000_000_000_000);

        let sum = get_effective_balance_sum(&mock_beacon_node, &state_root).await;
        assert_eq!(sum, expected_sum);
    }

    #[test_context(TestDb)]
    #[tokio::test]
    async fn test_store_effective_balance_sum(test_db: &TestDb) {
        let state_root = SLOT_0_STATE_ROOT;
        let sum = GweiNewtype(9500000);

        beacon_chain::store_state(&test_db.pool, state_root, Slot(0)).await;

        store_effective_balance_sum(&test_db.pool, state_root, &sum).await;

        // Query the database and verify that the sum was stored correctly.
        let stored_sum: i64 = sqlx::query_scalar!(
            "
            SELECT effective_balance_sum
            FROM beacon_states
            WHERE state_root = $1
            ",
            state_root
        )
        .fetch_one(&test_db.pool)
        .await
        .unwrap()
        .unwrap();

        assert_eq!(stored_sum, sum.0);
    }
}
