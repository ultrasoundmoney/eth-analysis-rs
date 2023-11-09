//! Fns to read variables from the environment more conveniently and help other functions figure
//! out what environment they're running in.

use std::env;

use lazy_static::lazy_static;
use tracing::debug;

const SECRET_LOG_BLACKLIST: [&str; 3] = ["DATABASE_URL", "OPSGENIE_API_KEY", "ETHERSCAN_API_KEY"];

lazy_static! {
    pub static ref ENV_CONFIG: EnvConfig = get_env_config();
}

fn obfuscate_if_secret(blacklist: &[&str], key: &str, value: &str) -> String {
    if blacklist.contains(&key) {
        let mut last_four = value.to_string();
        last_four.drain(0..value.len().saturating_sub(4));
        format!("****{last_four}")
    } else {
        value.to_string()
    }
}

/// Get an environment variable, encoding found or missing as Option, and panic otherwise.
pub fn get_env_var(key: &str) -> Option<String> {
    let var = match env::var(key) {
        Err(env::VarError::NotPresent) => None,
        Err(e) => panic!("{e}"),
        Ok(var) => Some(var),
    };

    if let Some(ref existing_var) = var {
        let output = obfuscate_if_secret(&SECRET_LOG_BLACKLIST, key, existing_var);
        debug!("env var {key}: {output}");
    } else {
        debug!("env var {key} requested but not found")
    };

    var
}

pub fn get_env_bool(key: &str) -> Option<bool> {
    get_env_var(key).map(|var| match var.to_lowercase().as_str() {
        "true" => true,
        "false" => false,
        "t" => true,
        "f" => false,
        "1" => true,
        "0" => false,
        str => panic!("invalid bool value {str} for {key}"),
    })
}

pub struct EnvConfig {
    pub beacon_url: String,
    pub bind_public_interface: bool,
    pub db_url: String,
    pub etherscan_api_key: String,
    // Separate out geth deltas fork URL.
    pub geth_url: String,
    pub log_json: bool,
    pub log_perf: bool,
}

pub fn get_env_config() -> EnvConfig {
    EnvConfig {
        beacon_url: get_env_var("BEACON_URL").expect("BEACON_URL is required"),
        bind_public_interface: get_env_bool("BIND_PUBLIC_INTERFACE").unwrap_or(true),
        db_url: get_env_var("DATABASE_URL").expect("DATABASE_URL is required"),
        etherscan_api_key: get_env_var("ETHERSCAN_API_KEY").expect("ETHERSCAN_API_KEY is required"),
        geth_url: get_env_var("GETH_URL").expect("GETH_URL is required"),
        log_json: get_env_bool("LOG_JSON").unwrap_or(false),
        log_perf: get_env_bool("LOG_PERF").unwrap_or(false),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_env_var_safe_some() {
        let test_key = "TEST_KEY_SAFE_SOME";
        let test_value = "my-env-value";
        std::env::set_var(test_key, test_value);
        assert_eq!(get_env_var(test_key), Some(test_value.to_string()));
    }

    #[test]
    fn test_get_env_var_safe_none() {
        let key = get_env_var("DOESNT_EXIST");
        assert!(key.is_none());
    }

    #[test]
    fn test_get_env_bool_not_there() {
        let flag = get_env_bool("DOESNT_EXIST");
        assert_eq!(flag, None);
    }

    #[test]
    fn test_get_env_bool_true() {
        let test_key = "TEST_KEY_BOOL_TRUE";
        let test_value = "true";
        std::env::set_var(test_key, test_value);
        assert_eq!(get_env_bool(test_key), Some(true));
    }

    #[test]
    fn test_get_env_bool_true_upper() {
        let test_key = "TEST_KEY_BOOL_TRUE2";
        let test_value = "TRUE";
        std::env::set_var(test_key, test_value);
        assert_eq!(get_env_bool(test_key), Some(true));
    }

    #[test]
    fn test_get_env_bool_false() {
        let test_key = "TEST_KEY_BOOL_FALSE";
        let test_value = "false";
        std::env::set_var(test_key, test_value);
        assert_eq!(get_env_bool(test_key), Some(false));
    }

    #[test]
    fn test_obfuscate_if_secret() {
        let secret_key = "SECRET_KEY";
        let blacklist = vec![secret_key];
        assert_eq!(
            obfuscate_if_secret(&blacklist, secret_key, "my_secret_value"),
            "****alue"
        );

        let normal_key = "NORMAL_KEY";
        assert_eq!(
            obfuscate_if_secret(&blacklist, normal_key, "my_normal_value"),
            "my_normal_value"
        );
    }
}
