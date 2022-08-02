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

pub fn get_db_url_with_name<'a>(name: &str) -> String {
    format!("{}?application-name={name}", get_db_url())
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

fn get_env_bool(key: &str) -> bool {
    let flag = get_env_var(key).map_or(false, |var| var.to_lowercase() == "true");
    tracing::debug!("env flag {key}: {flag}");
    flag
}

#[cached]
pub fn get_log_perf() -> bool {
    get_env_bool("LOG_PERF")
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

    #[test]
    fn get_env_bool_not_there_test() {
        let flag = get_env_bool("DOESNT_EXIST");
        assert_eq!(flag, false);
    }

    #[test]
    fn get_env_bool_true_test() {
        let test_key = "TEST_KEY_BOOL_TRUE";
        let test_value = "true";
        std::env::set_var(test_key, test_value);
        assert_eq!(get_env_bool(test_key), true);
    }

    #[test]
    fn get_env_bool_true_upper_test() {
        let test_key = "TEST_KEY_BOOL_TRUE2";
        let test_value = "TRUE";
        std::env::set_var(test_key, test_value);
        assert_eq!(get_env_bool(test_key), true);
    }

    #[test]
    fn get_env_bool_false_test() {
        let test_key = "TEST_KEY_BOOL_FALSE";
        let test_value = "false";
        std::env::set_var(test_key, test_value);
        assert_eq!(get_env_bool(test_key), false);
    }
}
