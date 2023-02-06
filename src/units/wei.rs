use std::{
    fmt::Display,
    num::ParseIntError,
    ops::{Add, Sub},
    str::FromStr,
};

use serde::{Deserialize, Serialize};

use super::{EthNewtype, GweiNewtype, WEI_PER_ETH};

pub type WeiF64 = f64;

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(into = "String")]
#[serde(try_from = "String")]
pub struct WeiNewtype(pub i128);

impl WeiNewtype {
    pub fn from_eth(eth: i128) -> Self {
        Self(eth * WEI_PER_ETH)
    }
}

impl Add<WeiNewtype> for WeiNewtype {
    type Output = Self;

    fn add(self, WeiNewtype(rhs): Self) -> Self::Output {
        let WeiNewtype(lhs) = self;
        let result = lhs
            .checked_add(rhs)
            .expect("caused overflow in wei addition");
        WeiNewtype(result)
    }
}

impl Sub<WeiNewtype> for WeiNewtype {
    type Output = Self;

    fn sub(self, WeiNewtype(rhs): WeiNewtype) -> Self::Output {
        let WeiNewtype(lhs) = self;
        let result = lhs
            .checked_sub(rhs)
            .expect("caused underflow in wei subtraction");
        WeiNewtype(result)
    }
}

impl Display for WeiNewtype {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let WeiNewtype(amount) = self;
        write!(f, "{amount}")
    }
}

impl From<WeiNewtype> for String {
    fn from(WeiNewtype(amount): WeiNewtype) -> Self {
        amount.to_string()
    }
}

impl FromStr for WeiNewtype {
    type Err = ParseIntError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        s.parse::<i128>().map(WeiNewtype)
    }
}

impl TryFrom<String> for WeiNewtype {
    type Error = ParseIntError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        value.parse::<i128>().map(WeiNewtype)
    }
}

impl From<i128> for WeiNewtype {
    fn from(amount: i128) -> Self {
        WeiNewtype(amount)
    }
}

impl From<GweiNewtype> for WeiNewtype {
    fn from(GweiNewtype(amount): GweiNewtype) -> Self {
        (amount as i128 * GweiNewtype::WEI_PER_GWEI as i128).into()
    }
}

impl From<&GweiNewtype> for WeiNewtype {
    fn from(GweiNewtype(amount): &GweiNewtype) -> Self {
        (*amount as i128 * GweiNewtype::WEI_PER_GWEI as i128).into()
    }
}

impl From<EthNewtype> for WeiNewtype {
    fn from(EthNewtype(amount): EthNewtype) -> Self {
        (amount as i128 * EthNewtype::WEI_PER_ETH).into()
    }
}

pub type Wei = i128;
