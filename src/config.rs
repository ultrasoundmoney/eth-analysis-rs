use std::env;

pub fn get_env_key_unsafe(key: &str) -> String {
    env::var(key).expect(&format!("{key} in env"))
}

pub fn get_env_key(key: &str) -> Result<String, env::VarError> {
    env::var(key)
}

pub fn get_beacon_url() -> String {
    get_env_key_unsafe("BEACON_URL")
}

pub fn get_db_url() -> String {
    get_env_key_unsafe("DATABASE_URL")
}

pub fn get_glassnode_api_key() -> String {
    get_env_key_unsafe("GLASSNODE_API_KEY")
}

pub fn get_etherscan_api_key() -> String {
    get_env_key_unsafe("ETHERSCAN_API_KEY")
}

pub fn get_execution_url() -> String {
    get_env_key_unsafe("GETH_URL")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[should_panic]
    fn test_get_key_unsafe_panics() {
        get_env_key_unsafe("DOESNT_EXIST");
    }

    #[test]
    fn test_get_key_unsafe_returns() {
        let test_key = "TEST_KEY";
        let test_value = "my-env-value";
        std::env::set_var(test_key, test_value);
        assert_eq!(get_env_key_unsafe(test_key), test_value);
    }

    #[test]
    fn test_get_key_safe_some() {
        let test_key = "TEST_KEY";
        let test_value = "my-env-value";
        std::env::set_var(test_key, test_value);
        assert_eq!(get_env_key(test_key), Ok(test_value.to_string()));
    }

    #[test]
    fn test_get_key_safe_none() {
        let key = get_env_key("DOESNT_EXIST");
        assert!(key.is_err());
    }
}
