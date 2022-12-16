mod gwei;
mod wei;

pub use gwei::Gwei;
pub use gwei::GweiNewtype;

pub use wei::Wei;
pub use wei::WeiNewtype;

pub const GWEI_PER_ETH: u64 = 1_000_000_000;

pub const GWEI_PER_ETH_F64: f64 = 1_000_000_000_f64;

pub const WEI_PER_ETH: i128 = 1_000_000_000_000_000_000;

pub type EthF64 = f64;

struct EthNewtype(pub i64);
