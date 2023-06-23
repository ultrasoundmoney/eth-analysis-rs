use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::PgExecutor;

use crate::units::{GweiImprecise, GweiNewtype};

use super::{BeaconNode, Slot, StateRoot};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EffectiveBalanceSum {
    // This amount is larger than 9M ETH, so we lose precision when serializing. For now, we don't
    // care.
    pub sum: GweiImprecise,
    pub slot: Slot,
    pub timestamp: DateTime<Utc>,
}

impl EffectiveBalanceSum {
    pub fn new(slot: &Slot, sum: GweiNewtype) -> Self {
        Self {
            sum: sum.into(),
            slot: *slot,
            timestamp: slot.date_time(),
        }
    }
}

pub async fn get_effective_balance_sum(
    beacon_node: &impl BeaconNode,
    state_root: &StateRoot,
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
    use anyhow::{anyhow, Result};
    use async_trait::async_trait;
    use test_context::test_context;

    use crate::{
        beacon_chain::{
            self,
            node::{
                BeaconBlock, FinalityCheckpoint, Validator, ValidatorBalance, ValidatorEnvelope,
            },
            BeaconHeaderSignedEnvelope, BlockId, StateRoot,
        },
        db::tests::TestDb,
        units::GweiNewtype,
    };

    use super::*;

    const SLOT_0_STATE_ROOT: &str =
        "0x7e76880eb67bbdc86250aa578958e9d0675e64e714337855204fb5abaaf82c2b";

    struct MockBeaconNode;

    #[async_trait]
    impl BeaconNode for MockBeaconNode {
        async fn get_block_by_block_root(&self, _block_root: &str) -> Result<Option<BeaconBlock>> {
            Ok(None)
        }

        async fn get_block_by_slot(&self, _slot: &Slot) -> Result<Option<BeaconBlock>> {
            Ok(None)
        }

        async fn get_header(
            &self,
            _block_id: &BlockId,
        ) -> Result<Option<BeaconHeaderSignedEnvelope>> {
            Ok(None)
        }

        async fn get_header_by_block_root(
            &self,
            _block_root: &str,
        ) -> Result<Option<BeaconHeaderSignedEnvelope>> {
            Ok(None)
        }

        async fn get_header_by_slot(
            &self,
            _slot: &Slot,
        ) -> Result<Option<BeaconHeaderSignedEnvelope>> {
            Ok(None)
        }

        async fn get_header_by_state_root(
            &self,
            _state_root: &str,
            _slot: &Slot,
        ) -> Result<Option<BeaconHeaderSignedEnvelope>> {
            Ok(None)
        }

        async fn get_last_block(&self) -> Result<BeaconBlock> {
            Err(anyhow!("Not implemented in the MockBeaconNode"))
        }

        async fn get_last_finality_checkpoint(&self) -> Result<FinalityCheckpoint> {
            Err(anyhow!("Not implemented in the MockBeaconNode"))
        }

        async fn get_last_finalized_block(&self) -> Result<BeaconBlock> {
            Err(anyhow!("Not implemented in the MockBeaconNode"))
        }

        async fn get_last_header(&self) -> Result<BeaconHeaderSignedEnvelope> {
            Err(anyhow!("Not implemented in the MockBeaconNode"))
        }

        async fn get_state_root_by_slot(&self, _slot: &Slot) -> Result<Option<StateRoot>> {
            Ok(None)
        }

        async fn get_validator_balances(
            &self,
            _state_root: &str,
        ) -> Result<Option<Vec<ValidatorBalance>>> {
            Ok(None)
        }

        async fn get_validators_by_state(
            &self,
            _state_root: &str,
        ) -> Result<Vec<ValidatorEnvelope>> {
            // Create some mock validator data to return
            let mock_validators = vec![
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
            Ok(mock_validators)
        }
    }

    #[tokio::test]
    async fn test_get_effective_balance_sum() {
        let mock_beacon_node = MockBeaconNode {};
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

        beacon_chain::store_state(&test_db.pool, state_root, &Slot(0)).await;

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
