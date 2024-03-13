use chrono::{DateTime, TimeZone, Utc};
use serde::{Deserialize, Deserializer};

pub fn from_i32_opt_hex_str<'de, D>(deserializer: D) -> Result<Option<i32>, D::Error>
where
    D: Deserializer<'de>,
{
    let opt: Option<String> = Deserialize::deserialize(deserializer)?;
    if let Some(s) = opt {
        Ok(Some(i32::from_str_radix(&s[2..], 16).unwrap()))
    } else {
        Ok(None)
    }
}

pub fn from_i32_hex_str<'de, D>(deserializer: D) -> Result<i32, D::Error>
where
    D: Deserializer<'de>,
{
    let s: String = Deserialize::deserialize(deserializer)?;
    Ok(i32::from_str_radix(&s[2..], 16).unwrap())
}

pub fn from_u32_hex_str<'de, D>(deserializer: D) -> Result<u32, D::Error>
where
    D: Deserializer<'de>,
{
    let s: String = Deserialize::deserialize(deserializer)?;
    Ok(u32::from_str_radix(&s[2..], 16).unwrap())
}

pub fn from_u64_hex_str<'de, D>(deserializer: D) -> Result<u64, D::Error>
where
    D: Deserializer<'de>,
{
    let s: String = Deserialize::deserialize(deserializer)?;
    Ok(u64::from_str_radix(&s[2..], 16).unwrap())
}

pub fn from_u128_hex_str<'de, D>(deserializer: D) -> Result<u128, D::Error>
where
    D: Deserializer<'de>,
{
    let s: String = Deserialize::deserialize(deserializer)?;
    Ok(u128::from_str_radix(&s[2..], 16).unwrap())
}

pub fn from_unix_timestamp_hex_str<'de, D>(deserializer: D) -> Result<DateTime<Utc>, D::Error>
where
    D: Deserializer<'de>,
{
    let timestamp_u32 = from_u32_hex_str(deserializer)?;
    let date_time = Utc.timestamp_opt(timestamp_u32.into(), 0).unwrap();
    Ok(date_time)
}
