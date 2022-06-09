use std::env;

pub fn get_beacon_url() -> String {
    env::var("BEACON_URL").expect("BEACON_URL in env")
}

pub fn get_db_url() -> String {
    env::var("DATABASE_URL").expect("DATABASE_URL in env")
}

pub fn get_glassnode_api_key() -> String {
    env::var("GLASSNODE_API_KEY").expect("GLASSNODE_API_KEY in env")
}

pub fn get_etherscan_api_key() -> String {
    env::var("ETHERSCAN_API_KEY").expect("ETHERSCAN_API_KEY in env")
}
