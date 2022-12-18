mod eth;
mod gwei;
mod wei;

pub use gwei::Gwei;
pub use gwei::GweiImprecise;
pub use gwei::GweiNewtype;

pub use wei::Wei;
pub use wei::WeiF64;
pub use wei::WeiNewtype;

pub use eth::EthNewtype;

pub const GWEI_PER_ETH_F64: f64 = 1_000_000_000_f64;

pub const WEI_PER_ETH: i128 = 1_000_000_000_000_000_000;

pub type EthF64 = f64;
