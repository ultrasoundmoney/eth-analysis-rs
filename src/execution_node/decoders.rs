use serde::{Deserialize, Deserializer};

pub fn from_u32_hex_str<'de, D>(deserializer: D) -> Result<u32, D::Error>
where
    D: Deserializer<'de>,
{
    let s: String = Deserialize::deserialize(deserializer)?;
    Ok(u32::from_str_radix(&s[2..], 16).unwrap())
}
