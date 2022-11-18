mod balances;
pub mod beacon_time;
mod blocks;
mod deposits;
mod effective_balance_sum;
mod issuance;
mod node;
mod rewards;
mod states;
mod sync;

use crate::env;
pub use balances::{
    get_validator_balances_by_start_of_day, sum_validator_balances, BeaconBalancesSum,
};
pub use blocks::{store_block, GENESIS_PARENT_ROOT};
pub use deposits::{get_deposits_sum, BeaconDepositsSum};
pub use effective_balance_sum::{
    get_last_stored_effective_balance_sum, update_effective_balance_sum,
};
pub use issuance::{get_current_issuance, get_issuance_by_start_of_day, get_last_week_issuance};
use lazy_static::lazy_static;
pub use node::{BeaconHeader, BeaconHeaderEnvelope, BeaconHeaderSignedEnvelope, BeaconNode};
pub use rewards::update_validator_rewards;
pub use states::get_last_state;
pub use states::get_state_root_by_slot;
pub use states::heal_beacon_states;
pub use states::store_state;
pub use states::Slot;
pub use sync::sync_beacon_states;

lazy_static! {
    static ref BEACON_URL: String = env::get_env_var_unsafe("BEACON_URL");
}

#[cfg(test)]
pub mod tests {
    use sqlx::{Acquire, PgConnection};

    use crate::eth_units::GweiNewtype;

    use super::{
        node::{BeaconBlock, BeaconBlockBody},
        *,
    };

    pub fn get_test_header(
        test_id: &str,
        slot: &Slot,
        parent_root: &str,
    ) -> BeaconHeaderSignedEnvelope {
        let state_root = format!("0x{test_id}_state_root");
        let block_root = format!("0x{test_id}_block_root");

        BeaconHeaderSignedEnvelope {
            root: block_root,
            header: BeaconHeaderEnvelope {
                message: BeaconHeader {
                    slot: *slot,
                    parent_root: (*parent_root).to_string(),
                    state_root,
                },
            },
        }
    }

    pub fn get_test_beacon_block(state_root: &str, slot: &Slot, parent_root: &str) -> BeaconBlock {
        BeaconBlock {
            body: BeaconBlockBody {
                deposits: vec![],
                execution_payload: None,
            },
            parent_root: parent_root.to_string(),
            slot: *slot,
            state_root: state_root.to_owned(),
        }
    }

    pub async fn store_test_block(executor: &mut PgConnection, test_id: &str) {
        let state_root = format!("0x{test_id}_state_root");
        let block_root = format!("0x{test_id}_block_root");
        let slot = 0;

        store_state(
            executor.acquire().await.unwrap(),
            &state_root,
            &slot,
            &block_root,
        )
        .await
        .unwrap();

        store_block(
            executor,
            &get_test_beacon_block(&state_root, &slot, GENESIS_PARENT_ROOT),
            &GweiNewtype(0),
            &GweiNewtype(0),
            &BeaconHeaderSignedEnvelope {
                root: block_root,
                header: BeaconHeaderEnvelope {
                    message: BeaconHeader {
                        slot,
                        parent_root: GENESIS_PARENT_ROOT.to_string(),
                        state_root: state_root.clone(),
                    },
                },
            },
        )
        .await
        .unwrap();
    }

    pub async fn store_custom_test_block(
        executor: &mut PgConnection,
        test_header: &BeaconHeaderSignedEnvelope,
        test_block: &BeaconBlock,
    ) {
        store_state(
            executor.acquire().await.unwrap(),
            &test_header.header.message.state_root,
            &test_header.header.message.slot,
            &test_header.root,
        )
        .await
        .unwrap();

        store_block(
            executor,
            test_block,
            &GweiNewtype(0),
            &GweiNewtype(0),
            test_header,
        )
        .await
        .unwrap();
    }
}
