//! Module to help export eth supply data to a csv file

use chrono::{DateTime, Duration, Utc};
use serde::Serialize;

use crate::{
    beacon_chain::{self, Slot},
    db,
    time_frames::{GrowingTimeFrame, TimeFrame},
    units::EthNewtype,
};

use super::over_time::SupplyAtTime;

#[derive(Debug, Serialize)]
struct SupplyAtSlot {
    epoch: i32,
    slot: Slot,
    supply: EthNewtype,
    timestamp: DateTime<Utc>,
}

#[derive(Debug, Serialize)]
struct Row {
    epoch: i32,
    epoch_delta: i32,
    slot: Slot,
    supply: EthNewtype,
    timestamp: DateTime<Utc>,
    timestamp_unix: i64,
}

impl From<SupplyAtTime> for SupplyAtSlot {
    // converting a timestamp to a slot is imprecise.
    fn from(supply_at_time: SupplyAtTime) -> Self {
        let slot = Slot::from_date_time_rounded_down(&supply_at_time.timestamp);
        let epoch = slot.epoch();
        let supply = supply_at_time.supply;
        Self {
            epoch,
            slot,
            supply,
            timestamp: supply_at_time.timestamp,
        }
    }
}

const SLOTS_PER_EPOCH: i32 = 32;

// export every thousandth epoch.
// uses a combination of daily glassnode data, and our own eth_supply table
pub async fn export_thousandth_epoch_supply() {
    let db_pool = db::get_db_pool("export_thousandth_epoch", 3).await;

    let recent_supply: Vec<SupplyAtSlot> = sqlx::query!(
        "
        WITH numbered_rows AS (
            SELECT
                ROW_NUMBER() OVER (ORDER BY timestamp) AS row_number,
                balances_slot AS slot,
                supply::FLOAT8 / 1e18 AS supply,
                timestamp
            FROM eth_supply
        )
        SELECT 
          slot,
          supply AS \"supply!\",
          timestamp
        FROM numbered_rows
        WHERE row_number % 32000 = 1
        ",
    )
    .fetch_all(&db_pool)
    .await
    .unwrap()
    .into_iter()
    .map(|s| SupplyAtSlot {
        epoch: Slot(s.slot).epoch(),
        slot: Slot(s.slot),
        supply: EthNewtype(s.supply),
        timestamp: s.timestamp,
    })
    .collect();

    let first_recent_supply = recent_supply.first().unwrap();

    let early_supply = sqlx::query!(
        "
        SELECT
            timestamp,
            supply
        FROM daily_supply_glassnode
        WHERE timestamp < $1
        AND timestamp >= $2
        ORDER BY timestamp ASC
        ",
        first_recent_supply.timestamp
            - Duration::seconds((1000 * SLOTS_PER_EPOCH * Slot::SECONDS_PER_SLOT).into()),
        *beacon_chain::GENESIS_TIMESTAMP
    )
    .fetch_all(&db_pool)
    .await
    .unwrap()
    .into_iter()
    .map(|row| {
        let slot = Slot::from_date_time_rounded_down(&row.timestamp);
        let epoch = slot.epoch();
        SupplyAtSlot {
            slot,
            epoch,
            timestamp: row.timestamp,
            supply: EthNewtype(row.supply),
        }
    })
    .fold(Vec::<SupplyAtSlot>::new(), |mut f, supply| {
        // If the current row is more than 1000 epochs since the last, add it,
        // otherwise, skip it.
        if let Some(last) = f.last() {
            if supply.epoch - last.epoch > 1000 {
                f.push(supply);
            }
        } else {
            f.push(supply);
        }
        f
    });

    let supply = early_supply.into_iter().chain(recent_supply.into_iter());

    let mut csv_writer = csv::Writer::from_path("eth_supply.csv").unwrap();
    let mut last_epoch: Option<i32> = None;
    for supply in supply {
        let row = Row {
            epoch: supply.epoch,
            epoch_delta: last_epoch.map(|e| supply.epoch - e).unwrap_or(0),
            slot: supply.slot,
            supply: supply.supply,
            timestamp: supply.timestamp,
            timestamp_unix: supply.timestamp.timestamp(),
        };
        last_epoch = Some(supply.epoch);
        csv_writer
            .serialize(row)
            .expect("failed to serialize supply");
    }
    csv_writer.flush().unwrap();
}

#[derive(Debug, Serialize)]
struct SupplyAtTimeRow {
    supply: EthNewtype,
    timestamp: DateTime<Utc>,
    timestamp_unix: i64,
}

pub async fn export_daily_supply_since_merge() {
    let db_pool = db::get_db_pool("export_daily_supply_since_merge", 3).await;
    let supply = super::over_time::from_time_frame(
        &db_pool,
        &TimeFrame::Growing(GrowingTimeFrame::SinceMerge),
    )
    .await;

    let timestamp = crate::time::get_timestamp();

    let mut csv_writer =
        csv::Writer::from_path(format!("eth_supply_since_merge_{timestamp}.csv")).unwrap();
    for supply in supply {
        let row = SupplyAtTimeRow {
            supply: supply.supply,
            timestamp: supply.timestamp,
            timestamp_unix: supply.timestamp.timestamp(),
        };
        csv_writer
            .serialize(row)
            .expect("failed to serialize supply");
    }
    csv_writer.flush().unwrap();
}
