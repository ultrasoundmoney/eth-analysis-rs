mod balances;
mod base_fees;
mod block_store;
mod export_blocks;
mod logs;
mod node;
mod supply_deltas;
mod sync;

pub use balances::get_execution_balances_by_hash;
pub use balances::ExecutionBalancesSum;

pub use block_store::delete_blocks;
pub use block_store::get_is_number_known;
pub use block_store::get_is_parent_hash_known;
pub use block_store::get_last_block_number;
pub use block_store::store_block;

pub use export_blocks::write_blocks_from_august;
pub use export_blocks::write_blocks_from_london;

pub use logs::write_heads_log as write_execution_heads_log;

pub use node::stream_new_heads;
pub use node::BlockHash;
pub use node::BlockNumber;
pub use node::ExecutionNode;
pub use node::ExecutionNodeBlock;

pub use supply_deltas::add_delta;
pub use supply_deltas::stream_supply_delta_chunks;
pub use supply_deltas::stream_supply_deltas_from;
pub use supply_deltas::summary_from_deltas_csv;
pub use supply_deltas::sync_deltas as sync_execution_supply_deltas;
pub use supply_deltas::write_deltas as write_execution_supply_deltas;
pub use supply_deltas::write_deltas_log as write_execution_supply_deltas_log;
pub use supply_deltas::SupplyDelta;

pub use sync::sync_blocks as sync_execution_blocks;

use chrono::DateTime;
use chrono::TimeZone;
use chrono::Utc;
use lazy_static::lazy_static;

lazy_static! {
    pub static ref LONDON_HARD_FORK_TIMESTAMP: DateTime<Utc> =
        Utc.timestamp_opt(1628166822, 0).unwrap();
    pub static ref BELLATRIX_HARD_FORK_TIMESTAMP: DateTime<Utc> =
        "2022-09-15T06:42:42Z".parse::<DateTime<Utc>>().unwrap();
}

#[allow(dead_code)]
pub const TOTAL_TERMINAL_DIFFICULTY: u128 = 58750000000000000000000;
