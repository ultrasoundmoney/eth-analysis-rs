use serde::{de, Deserialize, Deserializer, Serializer};

pub fn from_u32_string<'de, D>(deserializer: D) -> Result<u32, D::Error>
where
    D: Deserializer<'de>,
{
    let s: String = Deserialize::deserialize(deserializer)?;
    Ok(s.parse::<u32>().unwrap())
}

#[allow(dead_code)]
pub fn from_i128_string<'de, D>(deserializer: D) -> Result<i128, D::Error>
where
    D: Deserializer<'de>,
{
    let s: &str = Deserialize::deserialize(deserializer)?;
    s.parse::<i128>().map_err(|error| {
        de::Error::invalid_value(
            de::Unexpected::Str(&format!("unexpected value: {}, error: {}", s, error)),
            &"a number as string: \"118908973575220938641041929\", which fits within i128",
        )
    })
}

pub fn to_i128_string<S>(num_i128: &i128, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_str(&num_i128.to_string())
}

#[allow(dead_code)]
pub fn from_u128_string<'de, D>(deserializer: D) -> Result<u128, D::Error>
where
    D: Deserializer<'de>,
{
    let s: &str = Deserialize::deserialize(deserializer)?;
    s.parse::<u128>().map_err(|error| {
        de::Error::invalid_value(
            de::Unexpected::Str(&format!("unexpected value: {}, error: {}", s, error)),
            &"a number as string e.g. \"118908973575220938641041929\", which fits within u128",
        )
    })
}

pub fn to_u128_string<S>(num_u128: &u128, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_str(&num_u128.to_string())
}

#[allow(dead_code)]
pub fn from_u64_string<'de, D>(deserializer: D) -> Result<u64, D::Error>
where
    D: Deserializer<'de>,
{
    let s: &str = Deserialize::deserialize(deserializer)?;
    s.parse::<u64>().map_err(|error| {
        de::Error::invalid_value(
            de::Unexpected::Str(&format!("unexpected value: {}, error: {}", s, error)),
            &"a number as string e.g. \"11750378872376585\", which fits within u64",
        )
    })
}

pub fn to_u64_string<S>(num_u64: &u64, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_str(&num_u64.to_string())
}

#[cfg(test)]
mod tests {
    use serde::Serialize;

    use super::*;

    #[derive(Debug, Deserialize, PartialEq, Serialize)]
    struct PersonI128 {
        name: String,
        #[serde(
            deserialize_with = "from_i128_string",
            serialize_with = "to_i128_string"
        )]
        big_num: i128,
    }

    #[test]
    fn deserialize_i128_str_test() {
        let src = r#"{ "name": "alex", "big_num": "-118908973575220938641041929" }"#;
        let actual = serde_json::from_str::<PersonI128>(src).unwrap();
        let expected = PersonI128 {
            name: "alex".to_string(),
            big_num: -118908973575220938641041929,
        };
        assert_eq!(actual, expected);
    }

    #[test]
    fn serialize_i128_str_test() {
        let expected = r#"{"name":"alex","big_num":"-118908973575220938641041929"}"#;
        let actual = serde_json::to_string(&PersonI128 {
            name: "alex".to_string(),
            big_num: -118908973575220938641041929,
        })
        .unwrap();
        assert_eq!(actual, expected);
    }

    #[derive(Debug, Deserialize, PartialEq, Serialize)]
    struct PersonU128 {
        name: String,
        #[serde(
            deserialize_with = "from_u128_string",
            serialize_with = "to_u128_string"
        )]
        big_num: u128,
    }

    #[test]
    fn deserialize_u128_str_test() {
        let src = r#"{ "name": "alex", "big_num": "118908973575220938641041929" }"#;
        let actual = serde_json::from_str::<PersonU128>(src).unwrap();
        let expected = PersonU128 {
            name: "alex".to_string(),
            big_num: 118908973575220938641041929,
        };
        assert_eq!(actual, expected);
    }

    #[test]
    fn string_from_u128_test() {
        let expected = r#"{"name":"alex","big_num":"118908973575220938641041929"}"#;
        let actual = serde_json::to_string(&PersonU128 {
            name: "alex".to_string(),
            big_num: 118908973575220938641041929,
        })
        .unwrap();
        assert_eq!(actual, expected);
    }

    #[derive(Debug, Deserialize, PartialEq, Serialize)]
    struct PersonU64 {
        name: String,
        #[serde(deserialize_with = "from_u64_string", serialize_with = "to_u64_string")]
        big_num: u64,
    }

    #[test]
    fn deserialize_u64_str_test() {
        let src = r#"{ "name": "alex", "big_num": "11750378872376585" }"#;
        let actual = serde_json::from_str::<PersonU64>(src).unwrap();
        let expected = PersonU64 {
            name: "alex".to_string(),
            big_num: 11750378872376585,
        };
        assert_eq!(actual, expected);
    }

    #[test]
    fn string_from_u64_test() {
        let expected = r#"{"name":"alex","big_num":"11750378872376585"}"#;
        let actual = serde_json::to_string(&PersonU64 {
            name: "alex".to_string(),
            big_num: 11750378872376585,
        })
        .unwrap();
        assert_eq!(actual, expected);
    }
}
