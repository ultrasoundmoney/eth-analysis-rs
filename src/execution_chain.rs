mod balances;
mod block_store;
mod blocks;
mod logs;
mod node;
mod supply_deltas;
mod sync;

pub use balances::{get_balances_sum, ExecutionBalancesSum};
pub use blocks::get_latest_block;
use chrono::DateTime;
use chrono::TimeZone;
use chrono::Utc;
use lazy_static::lazy_static;
pub use logs::write_heads_log as write_execution_heads_log;
pub use node::{stream_new_heads, ExecutionNode};
pub use supply_deltas::stream_supply_delta_chunks;
pub use supply_deltas::sync_deltas as sync_execution_supply_deltas;
pub use supply_deltas::write_deltas as write_execution_supply_deltas;
pub use supply_deltas::write_deltas_log as write_execution_supply_deltas_log;
pub use supply_deltas::SupplyDelta;
pub use sync::sync_blocks as sync_execution_blocks;

lazy_static! {
    pub static ref LONDON_HARDFORK_TIMESTAMP: DateTime<Utc> = Utc.timestamp(1628166822, 0);
}
