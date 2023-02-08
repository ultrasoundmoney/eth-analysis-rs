use std::{
    fmt::Display,
    ops::{Add, Sub},
};

use serde::Serialize;

use super::{EthNewtype, GweiNewtype, WeiNewtype};

/// An amount of USD.
/// We use the imprecise f64 here because most USD amounts we track are based on ETH amounts,
/// converted to USD, which is also imprecise.
#[derive(Clone, Copy, Debug, PartialEq, Serialize)]
#[serde(transparent)]
pub struct UsdNewtype(pub f64);

impl UsdNewtype {
    pub fn from_eth(eth: EthNewtype, eth_price: f64) -> Self {
        let usd = eth.0 * eth_price;
        UsdNewtype(usd)
    }

    pub fn from_gwei(gwei: GweiNewtype, eth_price: f64) -> Self {
        let eth: EthNewtype = gwei.into();
        Self::from_eth(eth, eth_price)
    }

    pub fn from_wei(wei: WeiNewtype, eth_price: f64) -> Self {
        let eth: EthNewtype = wei.into();
        Self::from_eth(eth, eth_price)
    }
}

impl Add for UsdNewtype {
    type Output = Self;

    fn add(self, UsdNewtype(rhs): Self) -> Self::Output {
        let UsdNewtype(lhs) = self;
        let result = lhs + rhs;
        UsdNewtype(result)
    }
}

impl Sub for UsdNewtype {
    type Output = Self;

    fn sub(self, UsdNewtype(rhs): Self) -> Self::Output {
        let UsdNewtype(lhs) = self;
        let result = lhs - rhs;
        UsdNewtype(result)
    }
}

impl Display for UsdNewtype {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let UsdNewtype(amount) = self;
        write!(f, "{amount}")
    }
}

impl From<f64> for UsdNewtype {
    fn from(amount: f64) -> Self {
        UsdNewtype(amount)
    }
}
