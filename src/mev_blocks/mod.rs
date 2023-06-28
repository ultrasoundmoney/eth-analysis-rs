mod relay_api;
mod store;

use serde::Deserialize;

use crate::units::WeiNewtype;

pub use relay_api::MockRelayApi;
pub use relay_api::RelayApi;
pub use relay_api::RelayApiHttp;
pub use relay_api::EARLIEST_AVAILABLE_SLOT;

pub use store::MevBlocksStore;
pub use store::MevBlocksStorePostgres;
pub use store::MockMevBlocksStore;

#[derive(Clone, Deserialize, PartialEq, Debug)]
pub struct MevBlock {
    pub slot: i32,
    pub block_number: i32,
    pub block_hash: String,
    pub bid: WeiNewtype,
}
