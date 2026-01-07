//! Blob fee calculation based on EIP-4844.
//!
//! To update the blob schedule for future forks, replace the blob schedule JSON with the new
//! version. Blob schedule can be found in the chainspec:
//! <https://github.com/eth-clients/mainnet/blob/main/metadata/chainspec.json>

use lazy_static::lazy_static;
use serde::Deserialize;

use super::node::decoders::{from_u128_hex_str, from_u32_hex_str};

const BLOB_SCHEDULE_JSON: &str = include_str!("../../data/blobs/blobschedule.json");
const MIN_BLOB_BASE_FEE: u128 = 1;

#[derive(Debug, Deserialize)]
struct BlobScheduleJson {
    #[serde(rename = "blobSchedule")]
    blob_schedule: Vec<BlobScheduleEntry>,
}

#[derive(Debug, Deserialize)]
struct BlobScheduleEntry {
    #[serde(deserialize_with = "from_u32_hex_str")]
    timestamp: u32,
    #[serde(
        rename = "baseFeeUpdateFraction",
        deserialize_with = "from_u128_hex_str"
    )]
    base_fee_update_fraction: u128,
}

lazy_static! {
    static ref BLOB_SCHEDULE: Vec<(i64, u128)> = {
        let json: BlobScheduleJson = serde_json::from_str(BLOB_SCHEDULE_JSON)
            .expect("failed to parse blob schedule JSON");
        let mut entries: Vec<(i64, u128)> = json
            .blob_schedule
            .into_iter()
            .map(|e| (e.timestamp as i64, e.base_fee_update_fraction))
            .collect();
        // Sort descending by timestamp
        entries.sort_by(|a, b| b.cmp(a));
        entries
    };
}

pub fn calc_blob_base_fee(excess_blob_gas: u128, timestamp: i64) -> Option<u128> {
    blob_update_fraction_from_timestamp(timestamp)
        .map(|fraction| fake_exponential(MIN_BLOB_BASE_FEE, excess_blob_gas, fraction))
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_blob_schedule_is_sorted_descending() {
        for window in BLOB_SCHEDULE.windows(2) {
            assert!(
                window[0].0 > window[1].0,
                "entries should be sorted descending by time"
            );
        }
    }

    #[test]
    fn test_returns_correct_fractions() {
        // before cancun (1710338135) - no blobs existed
        assert_eq!(blob_update_fraction_from_timestamp(0), None);
        assert_eq!(blob_update_fraction_from_timestamp(1710338134), None);

        // cancun (at and after 1710338135, before prague)
        assert_eq!(
            blob_update_fraction_from_timestamp(1710338135),
            Some(3_338_477)
        );
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
            blob_update_fraction_from_timestamp(1750000000),
            Some(5_007_716)
        );

        // bpo1 (at and after 1765290071)
        assert_eq!(
            blob_update_fraction_from_timestamp(1765290071),
            Some(8_346_193)
        );
        assert_eq!(
            blob_update_fraction_from_timestamp(1766000000),
            Some(8_346_193)
        );

        // bpo2 (at and after 1767747671)
        assert_eq!(
            blob_update_fraction_from_timestamp(1767747671),
            Some(11_684_671)
        );
        assert_eq!(
            blob_update_fraction_from_timestamp(2000000000),
            Some(11_684_671)
        );
    }
}
