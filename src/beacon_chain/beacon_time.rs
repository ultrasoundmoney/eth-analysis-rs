use std::cmp::Ordering;

use chrono::{DateTime, Datelike, Duration, TimeZone, Timelike, Utc};
use lazy_static::lazy_static;

use super::Slot;

// pub const SECONDS_PER_SLOT: u8 = 12;
// pub const SLOTS_PER_EPOCH: u8 = 32;

lazy_static! {
    pub static ref GENESIS_TIMESTAMP: DateTime<Utc> = Utc.timestamp(1606824023, 0);
    pub static ref SLOT_DURATION: Duration = Duration::seconds(12);
}

pub fn get_date_time_from_slot(slot: &Slot) -> DateTime<Utc> {
    *GENESIS_TIMESTAMP + Duration::seconds((slot * 12).into())
}

#[cfg(test)]
pub fn get_slot_from_date_time(date_time: &DateTime<Utc>) -> Slot {
    let diff_seconds = *date_time - *GENESIS_TIMESTAMP;
    (diff_seconds.num_seconds() / SLOT_DURATION.num_seconds())
        .try_into()
        .unwrap()
}

pub fn get_is_first_of_day(slot: &Slot) -> bool {
    match slot.cmp(&0) {
        Ordering::Equal => true,
        Ordering::Greater => {
            let day_previous_slot = get_date_time_from_slot(&(slot - 1)).day();
            let day = get_date_time_from_slot(&slot).day();

            return day_previous_slot != day;
        }
        Ordering::Less => panic!("slot must be larger than zero, got {}", slot),
    }
}

#[derive(Debug)]
pub struct FirstOfDaySlot(pub Slot);

impl FirstOfDaySlot {
    pub fn new(slot: &u32) -> Option<Self> {
        if get_is_first_of_day(slot) {
            Some(FirstOfDaySlot(*slot))
        } else {
            None
        }
    }
}

#[allow(dead_code)]
fn get_is_first_of_minute(slot: &Slot) -> bool {
    match slot.cmp(&0) {
        Ordering::Equal => true,
        Ordering::Greater => {
            let minute_of_previous_slot = get_date_time_from_slot(&(slot - 1)).minute();
            let minute_of_slot = get_date_time_from_slot(&slot).minute();

            return minute_of_previous_slot != minute_of_slot;
        }
        Ordering::Less => panic!("slot must be larger than zero, got {}", slot),
    }
}

#[derive(Debug)]
pub struct FirstOfMinuteSlot(pub Slot);

impl FirstOfMinuteSlot {
    #[allow(dead_code)]
    pub fn new(slot: &u32) -> Option<Self> {
        dbg!(get_date_time_from_slot(slot));
        if get_is_first_of_minute(slot) {
            Some(FirstOfMinuteSlot(*slot))
        } else {
            None
        }
    }
}

#[derive(Debug)]
pub struct FirstOfDaySlotWithBlock(pub Slot);

impl FirstOfDaySlotWithBlock {
    // pub async fn new(beacon_node: BeaconNode, slot: &Slot) -> Result<Option<Self>> {
    // We use a tiny algo here to figure out if we're the first of all slots in the day with a
    // block.
    // If we don't have a block, we know we're not.
    // If we do have a block and we're also the first slot of the day, we are.
    // If we do have a block and we're not the first slot of the day, we still may be the first
    // in the day with a slot. Search backwards for a slot that is the first of the day without
    // a block.
    // If we hit a slot with the block, we can stop, we're not.
    // If we hit a slot without a block and its the first of the day, we are!
    // If we hit a slot without a block, we keep searching.
    // unimplemented!()
    // }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn first_of_day_genesis_test() {
        assert!(FirstOfDaySlot::new(&0).is_some());
    }

    #[test]
    fn first_of_day_test() {
        assert!(FirstOfDaySlot::new(&3599).is_some());
    }

    #[test]
    fn not_first_of_day_test() {
        assert!(!FirstOfDaySlot::new(&1).is_some());
        assert!(!FirstOfDaySlot::new(&3598).is_some());
        assert!(!FirstOfDaySlot::new(&3600).is_some());
    }

    #[test]
    fn get_timestamp_test() {
        assert_eq!(
            get_date_time_from_slot(&0),
            "2020-12-01T12:00:23Z".parse::<DateTime<Utc>>().unwrap()
        );
        assert_eq!(
            get_date_time_from_slot(&3599),
            "2020-12-02T00:00:11Z".parse::<DateTime<Utc>>().unwrap()
        );
    }

    #[test]
    fn start_of_day_test() {
        assert!(FirstOfDaySlot::new(&0).is_some());
        assert!(FirstOfDaySlot::new(&3599).is_some());
    }

    #[test]
    fn not_start_of_day_test() {
        assert!(FirstOfDaySlot::new(&1).is_none());
        assert!(FirstOfDaySlot::new(&3598).is_none());
        assert!(FirstOfDaySlot::new(&3600).is_none());
    }

    #[test]
    fn first_of_minute_genesis_test() {
        assert!(FirstOfMinuteSlot::new(&0).is_some());
    }

    #[test]
    fn first_of_minute_test() {
        assert!(FirstOfMinuteSlot::new(&4).is_some());
    }

    #[test]
    fn not_first_of_minute_test() {
        assert!(FirstOfMinuteSlot::new(&3).is_none());
        assert!(FirstOfMinuteSlot::new(&5).is_none());
    }
}
