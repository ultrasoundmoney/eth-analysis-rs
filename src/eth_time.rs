use chrono::{DateTime, Utc};
use lazy_static::lazy_static;

use crate::execution_chain;

lazy_static! {
    pub static ref MERGE_HARD_FORK_TIMESTAMP: DateTime<Utc> =
        *execution_chain::PARIS_HARD_FORK_TIMESTAMP;
}
