use serde::{Deserialize, Deserializer};

pub fn from_u32_string<'de, D>(deserializer: D) -> Result<u32, D::Error>
where
    D: Deserializer<'de>,
{
    let s: String = Deserialize::deserialize(deserializer)?;
    Ok(s.parse::<u32>().unwrap())
}
