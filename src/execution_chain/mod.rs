mod balances;
mod base_fees;
mod block_range;
pub mod block_store;
mod block_store_next;
mod export_blocks;
mod logs;
mod node;
pub mod routes;
pub mod supply_deltas;
mod sync;

pub use balances::get_execution_balances_by_hash;
pub use balances::ExecutionBalancesSum;

pub use base_fees::routes as base_fees_routes;

pub use block_range::BlockRange;

pub use block_store::delete_blocks;
pub use block_store::get_last_block_number;
pub use block_store::store_block;

pub use block_store_next::BlockStore;
pub use block_store_next::BlockStorePostgres;

pub use export_blocks::export_blocks_from_august;
pub use export_blocks::export_blocks_from_london;

use lazy_static::lazy_static;
pub use logs::write_heads_log as write_execution_heads_log;

pub use node::stream_heads_from;
pub use node::stream_new_heads;
pub use node::BlockHash;
pub use node::BlockNumber;
pub use node::ExecutionNode;
pub use node::ExecutionNodeBlock;
pub use node::TotalDifficulty;

#[cfg(test)]
pub use node::ExecutionNodeBlockBuilder;

pub use supply_deltas::add_delta;
pub use supply_deltas::export_deltas as export_execution_supply_deltas;
pub use supply_deltas::stream_supply_deltas_from;
pub use supply_deltas::summary_from_deltas_csv;
pub use supply_deltas::sync_deltas as sync_execution_supply_deltas;
pub use supply_deltas::write_deltas_log as write_execution_supply_deltas_log;
pub use supply_deltas::SupplyDelta;

pub use sync::sync_blocks as sync_execution_blocks;

use chrono::DateTime;
use chrono::Utc;

use crate::units::WeiNewtype;

pub const LONDON_HARD_FORK_BLOCK_HASH: &str =
    "0x9b83c12c69edb74f6c8dd5d052765c1adf940e320bd1291696e6fa07829eee71";
pub const LONDON_HARD_FORK_BLOCK_NUMBER: BlockNumber = 12965000;
pub const MERGE_BLOCK_NUMBER: i32 = 15_537_394;
#[allow(dead_code)]
pub const TOTAL_TERMINAL_DIFFICULTY: u128 = 58750000000000000000000;

// This number was recorded before we has a rigorous definition of how to combine the execution and
// beacon chains to come up with a precise supply. After a rigorous supply is established for every
// block and slot it would be good to update this number.
#[allow(dead_code)]
const MERGE_SLOT_SUPPLY: WeiNewtype = WeiNewtype(120_521_140_924_621_298_474_538_089);

// Until we have an eth supply calculated by adding together per-block supply deltas, we're using
// an estimate based on glassnode data.
#[allow(dead_code)]
const LONDON_SLOT_SUPPLY_ESTIMATE: WeiNewtype = WeiNewtype(117_397_725_113_869_100_000_000_000);

pub const GENESIS_SUPPLY: WeiNewtype = WeiNewtype(72_009_990_499_480_000_000_000_000);

lazy_static! {
    pub static ref LONDON_HARD_FORK_TIMESTAMP: DateTime<Utc> =
        "2021-08-05T12:33:42Z".parse::<DateTime<Utc>>().unwrap();
    pub static ref PARIS_HARD_FORK_TIMESTAMP: DateTime<Utc> =
        "2022-09-15T06:42:59Z".parse::<DateTime<Utc>>().unwrap();
}
