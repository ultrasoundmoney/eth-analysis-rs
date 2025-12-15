use chrono::{DateTime, Utc};
use lazy_static::lazy_static;
use serde::Deserialize;

const BLOB_SCHEDULE_JSON: &str = include_str!("../../data/blobs/blobschedule.json");

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct BlobScheduleFile {
    blob_schedule: BlobSchedule,
    cancun_time: i64,
    prague_time: i64,
    osaka_time: i64,
    bpo1_time: i64,
    bpo2_time: i64,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct BlobSchedule {
    cancun: ForkParams,
    prague: ForkParams,
    osaka: ForkParams,
    bpo1: ForkParams,
    bpo2: ForkParams,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ForkParams {
    base_fee_update_fraction: u128,
}

lazy_static! {
    static ref BLOB_SCHEDULE: [(i64, u128); 5] = {
        let file: BlobScheduleFile =
            serde_json::from_str(BLOB_SCHEDULE_JSON).expect("failed to parse blob schedule JSON");
        [
            (file.bpo2_time, file.blob_schedule.bpo2.base_fee_update_fraction),
            (file.bpo1_time, file.blob_schedule.bpo1.base_fee_update_fraction),
            (file.osaka_time, file.blob_schedule.osaka.base_fee_update_fraction),
            (file.prague_time, file.blob_schedule.prague.base_fee_update_fraction),
            (file.cancun_time, file.blob_schedule.cancun.base_fee_update_fraction),
        ]
    };
}

pub fn calc_blob_base_fee(excess_blob_gas: Option<i32>, timestamp: DateTime<Utc>) -> Option<i64> {
    const MIN_BLOB_BASE_FEE: u128 = 1;

    excess_blob_gas.map(|gas| {
        let gas: u128 = gas.try_into().expect("excess_blob_gas must be positive");
        let fraction = blob_update_fraction_from_timestamp(timestamp.timestamp());
        fake_exponential(MIN_BLOB_BASE_FEE, gas, fraction)
            .try_into()
            .expect("blob_base_fee overflow")
    })
}

fn blob_update_fraction_from_timestamp(timestamp: i64) -> u128 {
    BLOB_SCHEDULE
        .iter()
        .find(|(ts, _)| timestamp >= *ts)
        .map(|(_, fraction)| *fraction)
        .expect("no matching blob schedule entry")
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
    fn test_returns_correct_fractions() {
        // cancun (before prague)
        assert_eq!(blob_update_fraction_from_timestamp(0), 3_338_477);
        assert_eq!(blob_update_fraction_from_timestamp(1746612310), 3_338_477);

        // prague (at and after 1746612311)
        assert_eq!(blob_update_fraction_from_timestamp(1746612311), 5_007_716);
        assert_eq!(blob_update_fraction_from_timestamp(1747000000), 5_007_716);

        // osaka (at and after 1747387400)
        assert_eq!(blob_update_fraction_from_timestamp(1747387400), 5_007_716);
        assert_eq!(blob_update_fraction_from_timestamp(1750000000), 5_007_716);

        // bpo1 (at and after 1757387400)
        assert_eq!(blob_update_fraction_from_timestamp(1757387400), 8_346_193);
        assert_eq!(blob_update_fraction_from_timestamp(1760000000), 8_346_193);

        // bpo2 (at and after 1767387784)
        assert_eq!(blob_update_fraction_from_timestamp(1767387784), 11_684_671);
        assert_eq!(blob_update_fraction_from_timestamp(2000000000), 11_684_671);
    }
}
