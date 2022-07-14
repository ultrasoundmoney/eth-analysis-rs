use serde::{de, Deserialize, Deserializer, Serializer};

pub fn from_u32_string<'de, D>(deserializer: D) -> Result<u32, D::Error>
where
    D: Deserializer<'de>,
{
    let s: String = Deserialize::deserialize(deserializer)?;
    Ok(s.parse::<u32>().unwrap())
}

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

#[cfg(test)]
mod tests {
    use serde::Serialize;

    use super::*;

    #[derive(Debug, Deserialize, PartialEq, Serialize)]
    struct Person {
        name: String,
        #[serde(
            deserialize_with = "from_i128_string",
            serialize_with = "to_i128_string"
        )]
        big_num: i128,
    }

    #[test]
    fn deserialize_i128_str_test() {
        let src = r#"{ "name": "alex", "big_num": "118908973575220938641041929" }"#;
        let actual = serde_json::from_str::<Person>(src).unwrap();
        let expected = Person {
            name: "alex".to_string(),
            big_num: 118908973575220938641041929,
        };
        assert_eq!(actual, expected);
    }

    #[test]
    fn serialize_i128_str_test() {
        let expected = r#"{"name":"alex","big_num":"118908973575220938641041929"}"#;
        let actual = serde_json::to_string(&Person {
            name: "alex".to_string(),
            big_num: 118908973575220938641041929,
        })
        .unwrap();
        assert_eq!(actual, expected);
    }
}
