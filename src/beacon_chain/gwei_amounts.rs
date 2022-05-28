use std::{
    fmt,
    ops::{Add, Sub},
};

use serde::{
    de::{self, Visitor},
    Deserialize, Serialize,
};

// Can handle at most 1.84e19 Gwei, or 9.22e18 when we need to convert to i64 sometimes. That is
// ~9_000_000_000 ETH, which is more than the entire supply.
#[derive(Clone, Copy, Debug, PartialEq, Serialize)]
#[serde(transparent)]
pub struct GweiAmount(pub u64);

impl From<GweiAmount> for i64 {
    fn from(GweiAmount(amount): GweiAmount) -> Self {
        amount as i64
    }
}

impl From<i64> for GweiAmount {
    fn from(num: i64) -> Self {
        if num < 0 {
            panic!("tried to convert negative i64 into GweiAmount")
        } else {
            GweiAmount(num as u64)
        }
    }
}

impl From<String> for GweiAmount {
    fn from(amount: String) -> Self {
        GweiAmount(
            amount
                .parse::<u64>()
                .expect("amount to be a string of a gwei amount that fits into u64"),
        )
    }
}

impl Add<GweiAmount> for GweiAmount {
    type Output = Self;

    fn add(self, GweiAmount(rhs): Self) -> Self::Output {
        let GweiAmount(lhs) = self;
        GweiAmount(lhs + rhs)
    }
}

impl Sub<GweiAmount> for GweiAmount {
    type Output = Self;

    fn sub(self, GweiAmount(rhs): GweiAmount) -> Self::Output {
        let GweiAmount(lhs) = self;
        GweiAmount(lhs - rhs)
    }
}

struct GweiAmountVisitor;

impl<'de> Visitor<'de> for GweiAmountVisitor {
    type Value = GweiAmount;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter
            .write_str("an number encoded as a string smaller than the total supply of ETH in Gwei")
    }

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        match v.parse::<u64>() {
            Err(error) => Err(E::custom(format!(
                "failed to parse amount as u64: {}",
                error
            ))),
            Ok(amount) => Ok(GweiAmount(amount)),
        }
    }
}

impl<'de> Deserialize<'de> for GweiAmount {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_str(GweiAmountVisitor)
    }
}
