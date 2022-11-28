use crate::execution_chain::BlockNumber;

use super::Wei;

pub struct SupplySnapshot {
    #[allow(dead_code)]
    pub accounts_count: u64,
    pub block_hash: &'static str,
    pub block_number: BlockNumber,
    pub root: &'static str,
    pub balances_sum: Wei,
}

pub const SUPPLY_SNAPSHOT_15082718: SupplySnapshot = SupplySnapshot {
    accounts_count: 176_496_428,
    block_hash: "0xba7baa960085d0997884135a9c0f04f6b6de53164604084be701f98a31c4124d",
    block_number: 15_082_718,
    root: "655618..cfe0e6",
    balances_sum: 118908973575220938641041929,
};
