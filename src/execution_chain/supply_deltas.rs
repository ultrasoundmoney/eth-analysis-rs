mod export;
mod logs;
mod node;
mod sync;

pub use export::write_deltas;
pub use logs::write_deltas_log;
pub use node::{stream_supply_delta_chunks, stream_supply_deltas};
pub use sync::store_delta;
pub use sync::sync_deltas;

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
