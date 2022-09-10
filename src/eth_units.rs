use std::{
    fmt,
    ops::{Add, Sub},
    str::FromStr,
};

use serde::{de, de::Visitor, Deserialize, Serialize, Serializer};

pub const GWEI_PER_ETH: u64 = 1_000_000_000;

pub const GWEI_PER_ETH_F64: f64 = 1_000_000_000_f64;

#[allow(dead_code)]
pub type Gwei = u64;

pub type GweiF64 = f64;

// TODO: Decide if using a NewType is worth it.
// Can handle at most 1.84e19 Gwei, or 9.22e18 when we need to convert to i64 sometimes. That is
// ~9_000_000_000 ETH, which is more than the entire supply.
// When converting to f64 however, max safe is 2^53, so anything more than ~9M ETH will lose
// accuracy. i.e. don't put this into JSON for amounts >9M ETH.
#[derive(Clone, Copy, Debug, PartialEq, Serialize)]
#[serde(transparent)]
pub struct GweiNewtype(pub u64);

impl fmt::Display for GweiNewtype {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} gwei", self.0)
    }
}

impl GweiNewtype {
    pub fn new(gwei: u64) -> Self {
        Self(gwei)
    }

    pub fn from_eth(eth: u64) -> Self {
        Self(eth * GWEI_PER_ETH)
    }

    pub fn from_eth_f64(eth: f64) -> Self {
        Self((eth * GWEI_PER_ETH_F64) as u64)
    }
}

impl From<GweiNewtype> for i64 {
    fn from(GweiNewtype(amount): GweiNewtype) -> Self {
        i64::try_from(amount).unwrap()
    }
}

impl From<i64> for GweiNewtype {
    fn from(gwei_i64: i64) -> Self {
        GweiNewtype(u64::try_from(gwei_i64).expect("failed to convert i64 into GweiAmount {}"))
    }
}

impl From<String> for GweiNewtype {
    fn from(gwei_str: String) -> Self {
        GweiNewtype(
            gwei_str
                .parse::<u64>()
                .expect("amount to be a string of a gwei amount that fits into u64"),
        )
    }
}

impl Add<GweiNewtype> for GweiNewtype {
    type Output = Self;

    fn add(self, GweiNewtype(rhs): Self) -> Self::Output {
        let GweiNewtype(lhs) = self;
        GweiNewtype(lhs + rhs)
    }
}

impl Sub<GweiNewtype> for GweiNewtype {
    type Output = Self;

    fn sub(self, GweiNewtype(rhs): GweiNewtype) -> Self::Output {
        let GweiNewtype(lhs) = self;
        GweiNewtype(lhs - rhs)
    }
}

impl From<WeiString> for GweiNewtype {
    fn from(WeiString(amount_str): WeiString) -> Self {
        let gwei_u128 = u128::from_str(&amount_str).unwrap() / u128::from(GWEI_PER_ETH);
        let gwei_u64 = u64::try_from(gwei_u128).unwrap();
        Self(gwei_u64)
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
        v.parse::<u64>()
            .map(|gwei_u64| GweiNewtype(gwei_u64))
            .map_err(|error| {
                de::Error::invalid_value(
                    de::Unexpected::Str(&format!("unexpected value: {}, error: {}", v, error)),
                    &"a number as string: \"118908973575220938\", which fits within u64",
                )
            })
    }

    fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(GweiNewtype(u64::try_from(v).unwrap()))
    }

    fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(GweiNewtype(v))
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

pub fn to_gwei_string<S>(gwei: &GweiNewtype, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let gwei_str = gwei.0.to_string();
    serializer.serialize_str(&gwei_str)
}

pub type Wei = i128;

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(transparent)]
pub struct WeiString(pub String);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gwei_from_wei_string_test() {
        let wei_string = WeiString("118068179561500000000000000".to_string());
        let gwei = GweiNewtype::from(wei_string);
        assert_eq!(gwei, GweiNewtype(118068179561500000));
    }

    #[test]
    fn gwei_from_string_test() {
        let gwei = GweiNewtype::from("1234567890".to_string());
        assert_eq!(gwei, GweiNewtype(1234567890));
    }

    #[test]
    fn gwei_add_test() {
        assert_eq!(GweiNewtype(1) + GweiNewtype(1), GweiNewtype(2));
    }

    #[test]
    fn gwei_sub_test() {
        assert_eq!(GweiNewtype(1) - GweiNewtype(1), GweiNewtype(0));
    }

    #[test]
    fn gwei_from_eth() {
        assert_eq!(GweiNewtype::from_eth(1), GweiNewtype(GWEI_PER_ETH))
    }
}
