use chrono::{DateTime, Utc};
use lazy_static::lazy_static;
use serde_json::Value;

const BLOB_SCHEDULE_JSON: &str = include_str!("../../data/blobs/blobschedule.json");
const MIN_BLOB_BASE_FEE: u128 = 1;

lazy_static! {
    static ref BLOB_SCHEDULE: Vec<(i64, u128)> = parse_blob_schedule(BLOB_SCHEDULE_JSON);
}

pub fn calc_blob_base_fee(excess_blob_gas: Option<i32>, timestamp: DateTime<Utc>) -> Option<u128> {
    excess_blob_gas
        .and_then(|v| v.try_into().ok())
        .zip(blob_update_fraction_from_timestamp(timestamp.timestamp()))
        .map(|(gas, fraction)| fake_exponential(MIN_BLOB_BASE_FEE, gas, fraction))
}

fn blob_update_fraction_from_timestamp(timestamp: i64) -> Option<u128> {
    BLOB_SCHEDULE
        .iter()
        .find(|(ts, _)| timestamp >= *ts)
        .map(|(_, fraction)| *fraction)
}

fn fake_exponential(factor: u128, numerator: u128, denominator: u128) -> u128 {
    let mut i: u128 = 1;
    let mut output: u128 = 0;
    let mut numerator_accum: u128 = factor * denominator;
    while numerator_accum > 0 {
        output += numerator_accum;
        numerator_accum = (numerator_accum * numerator) / (denominator * i);
        i += 1;
    }
    output / denominator
}

// Parses the blob schedule into a vec so the code doesn't need to change for any future blob
// schedule additions
fn parse_blob_schedule(json: &str) -> Vec<(i64, u128)> {
    let json: Value = serde_json::from_str(json).expect("failed to parse blob schedule JSON");

    let blob_schedule = json["blobSchedule"]
        .as_object()
        .expect("blobSchedule should be an object");
    // Any additions that don't follow the current blob schedule format can only cause a panic at
    // startup.
    let mut entries: Vec<(i64, u128)> = blob_schedule
        .keys()
        .map(|fork_name| {
            let time_key = format!("{}Time", fork_name);
            let time = json[&time_key]
                .as_i64()
                .unwrap_or_else(|| panic!("missing or invalid {}", time_key));
            let fraction = blob_schedule[fork_name]["baseFeeUpdateFraction"]
                .as_u64()
                .unwrap_or_else(|| panic!("missing baseFeeUpdateFraction for {}", fork_name))
                as u128;
            (time, fraction)
        })
        .collect();
    // Sort schedule by timestamp so we don't rely on the order of the blob schedule.
    entries.sort_by(|a, b| b.cmp(a));
    entries
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[should_panic(expected = "missing or invalid")]
    fn test_panics_on_invalid_schedule() {
        let json = include_str!("../../data/test/breakingblobscheduletest.json");
        parse_blob_schedule(json);
    }

    #[test]
    fn test_parses_and_sorts_test_schedule() {
        let json = include_str!("../../data/test/blobscheduletest.json");
        let entries = parse_blob_schedule(json);

        // Should have 7 entries
        assert_eq!(entries.len(), 7);

        // Should be sorted descending by time
        for window in entries.windows(2) {
            assert!(
                window[0].0 > window[1].0,
                "entries should be sorted descending by time"
            );
        }

        // badTest (1746612611) should be sorted between prague (1746612311) and osaka (1747387400)
        let bad_test_idx = entries.iter().position(|(t, _)| *t == 1746612611).unwrap();
        let prague_idx = entries.iter().position(|(t, _)| *t == 1746612311).unwrap();
        let osaka_idx = entries.iter().position(|(t, _)| *t == 1747387400).unwrap();

        assert!(
            bad_test_idx < prague_idx,
            "badTest should come before prague (descending order)"
        );
        assert!(
            bad_test_idx > osaka_idx,
            "badTest should come after osaka (descending order)"
        );
    }

    #[test]
    fn test_returns_correct_fractions() {
        // cancun (before prague)
        assert_eq!(blob_update_fraction_from_timestamp(0), Some(3_338_477));
        assert_eq!(
            blob_update_fraction_from_timestamp(1746612310),
            Some(3_338_477)
        );

        // prague (at and after 1746612311)
        assert_eq!(
            blob_update_fraction_from_timestamp(1746612311),
            Some(5_007_716)
        );
        assert_eq!(
            blob_update_fraction_from_timestamp(1747000000),
            Some(5_007_716)
        );

        // osaka (at and after 1747387400)
        assert_eq!(
            blob_update_fraction_from_timestamp(1747387400),
            Some(5_007_716)
        );
        assert_eq!(
            blob_update_fraction_from_timestamp(1750000000),
            Some(5_007_716)
        );

        // bpo1 (at and after 1757387400)
        assert_eq!(
            blob_update_fraction_from_timestamp(1757387400),
            Some(8_346_193)
        );
        assert_eq!(
            blob_update_fraction_from_timestamp(1760000000),
            Some(8_346_193)
        );

        // bpo2 (at and after 1767387784)
        assert_eq!(
            blob_update_fraction_from_timestamp(1767387784),
            Some(11_684_671)
        );
        assert_eq!(
            blob_update_fraction_from_timestamp(2000000000),
            Some(11_684_671)
        );
    }
}
