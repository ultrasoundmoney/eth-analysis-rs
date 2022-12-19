use serde::{Deserialize, Deserializer};

pub fn i32_from_string<'de, D>(deserializer: D) -> Result<i32, D::Error>
where
    D: Deserializer<'de>,
{
    let s: String = Deserialize::deserialize(deserializer)?;
    let num_i32 = s.parse::<i32>().map_err(serde::de::Error::custom)?;
    Ok(num_i32)
}

#[cfg(test)]
mod tests {
    use serde::{Serialize, Serializer};

    use super::*;

    fn string_from_i32<S>(num_i32: &i32, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&num_i32.to_string())
    }

    #[derive(Debug, Deserialize, PartialEq, Serialize)]
    struct PersonI32 {
        name: String,
        #[serde(
            deserialize_with = "i32_from_string",
            serialize_with = "string_from_i32"
        )]
        age: i32,
    }

    #[test]
    fn deserialize_i32_str_test() {
        let src = r#"{ "name": "alex", "age": "29" }"#;
        let actual = serde_json::from_str::<PersonI32>(src).unwrap();
        let expected = PersonI32 {
            name: "alex".to_string(),
            age: 29,
        };
        assert_eq!(actual, expected);
    }

    #[test]
    fn serialize_i32_str_test() {
        let expected = r#"{"name":"alex","age":"29"}"#;
        let actual = serde_json::to_string(&PersonI32 {
            name: "alex".to_string(),
            age: 29,
        })
        .unwrap();
        assert_eq!(actual, expected);
    }
}
