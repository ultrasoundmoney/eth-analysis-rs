use serde::Deserialize;
use std::collections::HashMap;

#[derive(Debug, Deserialize, PartialEq)]
pub struct ProtocolSummary {
    pub slug: String,
}

#[derive(Debug, Deserialize, PartialEq)]
pub struct TokensAtDate {
    date: u32,
    pub tokens: HashMap<String, f64>,
}

#[derive(Debug, Deserialize, PartialEq)]
pub struct ChainTvl {
    /// sorted oldest first
    pub tokens: Vec<TokensAtDate>,
}

#[derive(Debug, Deserialize, PartialEq)]
pub struct ChainTvls {
    #[serde(rename = "Ethereum")]
    pub ethereum: Option<ChainTvl>,
}

#[derive(Debug, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Protocol {
    pub chain_tvls: ChainTvls,
    pub id: String,
    pub name: String,
}

pub fn get_protocols() -> Vec<ProtocolSummary> {
    reqwest::blocking::get("https://api.llama.fi/protocols")
        .unwrap()
        .json::<Vec<ProtocolSummary>>()
        .unwrap()
}

pub fn get_protocol(id: &str) -> Protocol {
    reqwest::blocking::get(&format!("https://api.llama.fi/protocol/{}", id))
        .unwrap()
        .json::<Protocol>()
        .unwrap()
}

#[cfg(test)]
mod tests {
    use maplit::hashmap;

    use super::*;

    #[test]
    fn deserializes_protocol_summaries() {
        let json = r#"[{
            "slug": "aave"
        }]"#;

        assert_eq!(
            serde_json::from_str::<Vec<ProtocolSummary>>(json).unwrap(),
            vec![ProtocolSummary {
                slug: String::from("aave")
            }]
        )
    }

    #[test]
    fn deserializes_protocol_with_eth() {
        let json = r#"{
            "id": "111",
            "name": "AAVE",
            "chainTvls": {
                "Ethereum": {
                    "tokens": [{
                        "date": 1578528000,
                        "tokens": {
                            "WETH": 105.78250578567727
                        }
                    }]
                }
            }
        }"#;

        let expected = Protocol {
            id: String::from("111"),
            name: String::from("AAVE"),
            chain_tvls: ChainTvls {
                ethereum: Some(ChainTvl {
                    tokens: vec![TokensAtDate {
                        date: 1578528000,
                        tokens: hashmap! {
                            String::from("WETH") => 105.78250578567727,
                        },
                    }],
                }),
            },
        };
        assert_eq!(serde_json::from_str::<Protocol>(json).unwrap(), expected)
    }

    #[test]
    fn deserializes_protocol_without_eth() {
        let json = r#"{
            "id": "111",
            "name": "AAVE",
            "chainTvls": {
                "Ethereum": {
                    "tokens": [{
                        "date": 1578528000,
                        "tokens": {
                            "ZRX": 50.456
                        }
                    }]
                }
            }
        }"#;

        let expected = Protocol {
            id: String::from("111"),
            name: String::from("AAVE"),
            chain_tvls: ChainTvls {
                ethereum: Some(ChainTvl {
                    tokens: vec![TokensAtDate {
                        date: 1578528000,
                        tokens: hashmap! {
                            String::from("ZRX") => 50.456
                        },
                    }],
                }),
            },
        };

        assert_eq!(serde_json::from_str::<Protocol>(json).unwrap(), expected)
    }

    #[test]
    fn deserializes_protocol_outside_ethereum() {
        let json = r#"{
            "id": "111",
            "name": "AAVE",
            "chainTvls": {
                "Solana": {
                    "tokens": [{
                        "date": 1578528000,
                        "tokens": {
                            "MANGO": 34.421
                        }
                    }]
                }
            }
        }"#;

        let expected = Protocol {
            id: String::from("111"),
            name: String::from("AAVE"),
            chain_tvls: ChainTvls { ethereum: None },
        };

        assert_eq!(serde_json::from_str::<Protocol>(json).unwrap(), expected)
    }
}
