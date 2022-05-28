use chrono::{DateTime, Datelike, Duration, DurationRound, TimeZone, Utc};
use lazy_static::lazy_static;

lazy_static! {
    static ref GENESIS_TIMESTAMP: DateTime<Utc> = Utc.timestamp(1606824023, 0);
}

pub fn get_timestamp(slot: &u32) -> DateTime<Utc> {
    *GENESIS_TIMESTAMP + Duration::seconds((slot * 12).into())
}

pub fn get_is_first_of_day(slot: &u32) -> bool {
    match slot {
        slot if *slot == 0 => true,
        slot if *slot > 0 => {
            let day_of_month_previous_slot = get_timestamp(&(slot - 1)).day();
            let day_of_month = get_timestamp(&slot).day();

            return day_of_month_previous_slot != day_of_month;
        }
        _ => panic!("slot must be larger than zero, got {}", slot),
    }
}

#[derive(Debug)]
pub struct FirstOfDaySlot(pub u32);

impl FirstOfDaySlot {
    pub fn new(slot: &u32) -> Option<Self> {
        if get_is_first_of_day(slot) {
            Some(FirstOfDaySlot(*slot))
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_first_of_day() {
        assert!(get_is_first_of_day(&3599))
    }

    #[test]
    fn test_not_first_of_day() {
        assert!(!get_is_first_of_day(&3598));
        assert!(!get_is_first_of_day(&3600));
    }

    #[test]
    fn test_get_timestamp() {
        let timestamp = get_timestamp(&0);
        assert_eq!(
            timestamp,
            "2020-12-01T12:00:23Z".parse::<DateTime<Utc>>().unwrap()
        )
    }

    #[test]
    fn test_start_of_day() {
        assert!(FirstOfDaySlot::new(&0).is_some())
    }

    #[test]
    fn test_not_start_of_day() {
        assert!(FirstOfDaySlot::new(&1).is_none())
    }
}
