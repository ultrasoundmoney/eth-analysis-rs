use std::env;

use cached::proc_macro::cached;

pub fn get_env_var(key: &str) -> Option<String> {
    let var = match env::var(key) {
        Err(env::VarError::NotPresent) => None,
        Err(err) => panic!("{}", err),
        Ok(var) => Some(var),
    };
    tracing::debug!("env var {key}: {var:?}");
    var
}

pub fn get_env_var_unsafe(key: &str) -> String {
    get_env_var(key).expect(&format!("{key} in env"))
}

#[cached]
pub fn get_beacon_url() -> String {
    get_env_var_unsafe("BEACON_URL")
}

#[cached]
pub fn get_db_url() -> String {
    get_env_var_unsafe("DATABASE_URL")
}

#[cached]
pub fn get_glassnode_api_key() -> String {
    get_env_var_unsafe("GLASSNODE_API_KEY")
}

#[cached]
pub fn get_etherscan_api_key() -> String {
    get_env_var_unsafe("ETHERSCAN_API_KEY")
}

#[cached]
pub fn get_execution_url() -> String {
    get_env_var_unsafe("GETH_URL")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[should_panic]
    fn get_env_var_unsafe_panics_test() {
        get_env_var_unsafe("DOESNT_EXIST");
    }

    #[test]
    fn get_env_var_unsafe_test() {
        let test_key = "TEST_KEY_UNSAFE";
        let test_value = "my-env-value";
        std::env::set_var(test_key, test_value);
        assert_eq!(get_env_var_unsafe(test_key), test_value);
    }

    #[test]
    fn get_env_var_safe_some_test() {
        let test_key = "TEST_KEY_SAFE_SOME";
        let test_value = "my-env-value";
        std::env::set_var(test_key, test_value);
        assert_eq!(get_env_var(test_key), Some(test_value.to_string()));
    }

    #[test]
    fn get_env_var_safe_none_test() {
        let key = get_env_var("DOESNT_EXIST");
        assert!(key.is_none());
    }
    }
}
