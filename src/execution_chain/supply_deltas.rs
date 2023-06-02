mod backfill;
mod export;
mod logs;
mod node;
pub mod snapshot;
mod sync;

pub use backfill::backfill_execution_supply;

pub use export::export_deltas;
pub use export::summary_from_deltas_csv;

pub use logs::write_deltas_log;

pub use node::stream_supply_delta_chunks;
pub use node::stream_supply_deltas_from;

pub use sync::add_delta;
pub use sync::sync_deltas;

use serde::Serialize;

use crate::units::Wei;

use super::node::BlockNumber;

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
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
