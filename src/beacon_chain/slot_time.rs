use chrono::Datelike;
use chrono::TimeZone;
use chrono::Utc;
use lazy_static::lazy_static;

lazy_static! {
    static ref GENESIS_TIMESTAMP: chrono::DateTime<Utc> = chrono::Utc.timestamp(1606824023, 0);
}

pub fn get_timestamp(slot: &u32) -> chrono::DateTime<Utc> {
    *GENESIS_TIMESTAMP + chrono::Duration::seconds((slot * 12).into())
}

pub fn get_is_first_of_day(slot: &u32) -> bool {
    match slot {
        slot if *slot == 0u32 => true,
        slot if *slot > 0u32 => {
            let day_of_month_previous_slot = get_timestamp(&(slot - 1)).day();
            let day_of_month = get_timestamp(&slot).day();

            return day_of_month_previous_slot != day_of_month;
        }
        _ => panic!("slot must be larger than zero, got {}", slot),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn first_of_day_slot() {
        assert!(get_is_first_of_day(&3599))
    }

    #[test]
    fn not_first_of_day_slot() {
        assert!(!get_is_first_of_day(&3598));
        assert!(!get_is_first_of_day(&3600));
    }
}
