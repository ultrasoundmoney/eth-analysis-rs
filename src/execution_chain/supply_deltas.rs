mod export;
mod logs;
mod node;
pub mod snapshot;
mod sync;

pub use export::write_deltas;
pub use logs::write_deltas_log;
pub use node::{stream_supply_delta_chunks, stream_supply_deltas_from};
pub use sync::add_delta;
pub use sync::sync_deltas;

use serde::Serialize;

use crate::eth_units::Wei;

use super::blocks::BlockNumber;

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct SupplyDelta {
    pub block_hash: String,
    pub block_number: BlockNumber,
    pub fee_burn: Wei,
    pub fixed_reward: Wei,
    pub parent_hash: String,
    pub self_destruct: Wei,
    pub supply_delta: Wei,
    pub uncles_reward: Wei,
}
