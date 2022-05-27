use std::env;

pub fn get_beacon_url() -> String {
    env::var("BEACON_URL").expect("BEACON_URL is in env")
}

pub fn get_db_url() -> String {
    env::var("DATABASE_URL").expect("DATABASE_URL is in env")
}
