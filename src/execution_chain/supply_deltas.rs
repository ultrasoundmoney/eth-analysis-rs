mod export;
mod logs;
mod sync;

pub use self::export::write_deltas;
pub use self::logs::write_deltas_log;
pub use self::sync::store_delta;
pub use self::sync::sync_deltas;

use serde::Serialize;

use crate::eth_units::Wei;

#[derive(Debug, Serialize)]
pub struct SupplyDelta {
    pub block_hash: String,
    pub block_number: u32,
    pub fee_burn: Wei,
    pub fixed_reward: Wei,
    pub parent_hash: String,
    pub self_destruct: Wei,
    pub supply_delta: Wei,
    pub uncles_reward: Wei,
}
