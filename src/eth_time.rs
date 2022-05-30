use chrono::{DateTime, TimeZone, Utc};
use lazy_static::lazy_static;

lazy_static! {
    pub static ref LONDON_HARDFORK_TIMESTAMP: DateTime<Utc> = Utc.timestamp(1628166822, 0);
}
