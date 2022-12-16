use std::{
    num::ParseIntError,
    ops::{Add, Sub},
    str::FromStr,
};

use serde::{Deserialize, Serialize};

use super::WEI_PER_ETH;

pub type WeiF64 = f64;

#[derive(Clone, Copy, Debug, Serialize, PartialEq)]
#[serde(into = "String")]
pub struct WeiNewtype(pub i128);

impl From<WeiNewtype> for String {
    fn from(WeiNewtype(amount): WeiNewtype) -> Self {
        amount.to_string()
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

impl WeiNewtype {
    pub fn from_eth(eth: i128) -> Self {
        Self(eth * WEI_PER_ETH)
    }
}

impl FromStr for WeiNewtype {
    type Err = ParseIntError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        s.parse::<i128>().map(WeiNewtype)
    }
}

pub type Wei = i128;

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(transparent)]
pub struct WeiString(pub String);
