use std::{fmt::Display, ops::Add};

use serde::Serialize;

use super::{GweiNewtype, WeiNewtype};

/// This type tracks an amount of ETH. It is likely you'd want to a more precise type such as
/// GweiNewtype or WeiNewtype instead. Converting to ETH only at the last moment if imprecise is
/// fine.
#[derive(Clone, Copy, Debug, PartialEq, Serialize)]
#[serde(transparent)]
pub struct EthNewtype(pub f64);

impl EthNewtype {
    pub const GWEI_PER_ETH: i64 = 1_000_000_000;
    pub const WEI_PER_ETH: i128 = 1_000_000_000_000_000_000;
}

impl Add for EthNewtype {
    type Output = Self;

    fn add(self, EthNewtype(rhs): Self) -> Self::Output {
        let EthNewtype(lhs) = self;
        let result = lhs + rhs;
        EthNewtype(result)
    }
}

impl Display for EthNewtype {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let EthNewtype(amount) = self;
        write!(f, "{amount}")
    }
}

/// NOTE: this loses precision.
impl From<GweiNewtype> for EthNewtype {
    fn from(GweiNewtype(amount): GweiNewtype) -> Self {
        EthNewtype(amount as f64 / EthNewtype::GWEI_PER_ETH as f64)
    }
}

/// NOTE: this loses precision.
impl From<WeiNewtype> for EthNewtype {
    fn from(WeiNewtype(amount): WeiNewtype) -> Self {
        EthNewtype(amount as f64 / EthNewtype::WEI_PER_ETH as f64)
    }
}
