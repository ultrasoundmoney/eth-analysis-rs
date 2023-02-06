use std::{
    fmt,
    ops::{Add, Div, Sub},
};

use serde::{de, de::Visitor, Deserialize, Serialize};

use super::{eth::EthNewtype, WeiNewtype};

// Can handle at most 1.84e19 Gwei, or 9.22e18 when we need to convert to i64 sometimes. That is
// ~9_000_000_000 ETH, which is more than the entire supply.
// When converting to f64 however, max safe is 2^53, so anything more than ~9M ETH will lose
// accuracy. i.e. don't put this into JSON for amounts >9M ETH.
// GweiNewtype should probably convert to and from f64 and be represented as f64 or to and from String and represented as i64.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(into = "String")]
pub struct GweiNewtype(pub i64);

impl fmt::Display for GweiNewtype {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl GweiNewtype {
    pub const WEI_PER_GWEI: u32 = 1_000_000_000;
}

impl Add<GweiNewtype> for GweiNewtype {
    type Output = Self;

    fn add(self, GweiNewtype(rhs): Self) -> Self::Output {
        let GweiNewtype(lhs) = self;
        let result = lhs
            .checked_add(rhs)
            .expect("caused overflow in gwei addition");
        GweiNewtype(result)
    }
}

impl Sub<GweiNewtype> for GweiNewtype {
    type Output = Self;

    fn sub(self, GweiNewtype(rhs): GweiNewtype) -> Self::Output {
        let GweiNewtype(lhs) = self;
        let result = lhs
            .checked_sub(rhs)
            .expect("caused underflow in gwei subtraction");
        GweiNewtype(result)
    }
}

impl Div<GweiNewtype> for GweiNewtype {
    type Output = Self;

    fn div(self, GweiNewtype(rhs): GweiNewtype) -> Self::Output {
        let GweiNewtype(lhs) = self;
        GweiNewtype(lhs / rhs)
    }
}

impl From<GweiNewtype> for i64 {
    fn from(GweiNewtype(amount): GweiNewtype) -> Self {
        amount
    }
}

impl From<GweiNewtype> for f64 {
    fn from(gwei: GweiNewtype) -> Self {
        gwei.0 as f64
    }
}

impl From<GweiNewtype> for String {
    fn from(GweiNewtype(amount): GweiNewtype) -> Self {
        amount.to_string()
    }
}

impl From<EthNewtype> for GweiNewtype {
    fn from(EthNewtype(amount): EthNewtype) -> Self {
        ((amount * EthNewtype::GWEI_PER_ETH as f64).trunc() as i64).into()
    }
}

impl From<i64> for GweiNewtype {
    fn from(amount: i64) -> Self {
        GweiNewtype(amount)
    }
}

impl From<WeiNewtype> for GweiNewtype {
    fn from(WeiNewtype(amount): WeiNewtype) -> Self {
        ((amount / GweiNewtype::WEI_PER_GWEI as i128) as i64).into()
    }
}

// When precision doesn't matter that much, or amounts are known to be less than 9M ETH.
#[derive(Clone, Copy, Debug, PartialEq, Serialize)]
pub struct GweiImprecise(pub f64);

impl From<GweiNewtype> for GweiImprecise {
    fn from(GweiNewtype(amount): GweiNewtype) -> Self {
        GweiImprecise(amount as f64)
    }
}

impl From<EthNewtype> for GweiImprecise {
    fn from(EthNewtype(amount): EthNewtype) -> Self {
        GweiImprecise(amount * EthNewtype::GWEI_PER_ETH as f64)
    }
}

struct GweiAmountVisitor;

impl<'de> Visitor<'de> for GweiAmountVisitor {
    type Value = GweiNewtype;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter
            .write_str("a number, or string of number, smaller u64::MAX representing some amount of ETH in Gwei")
    }

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        v.parse::<i64>().map(GweiNewtype).map_err(|error| {
            de::Error::invalid_value(
                de::Unexpected::Str(&format!("unexpected value: {v}, error: {error}")),
                &"a number as string: \"118908973575220938\", which fits within u64",
            )
        })
    }

    fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(GweiNewtype(v))
    }

    fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(GweiNewtype(i64::try_from(v).unwrap()))
    }
}

impl<'de> Deserialize<'de> for GweiNewtype {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_any(GweiAmountVisitor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gwei_add_test() {
        assert_eq!(GweiNewtype(1) + GweiNewtype(1), GweiNewtype(2));
    }

    #[test]
    fn gwei_sub_test() {
        assert_eq!(GweiNewtype(1) - GweiNewtype(1), GweiNewtype(0));
    }
}
