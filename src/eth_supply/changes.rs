use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use futures::try_join;
use serde::Serialize;
use sqlx::{PgExecutor, PgPool};

use crate::{
    beacon_chain::{Slot, FIRST_POST_LONDON_SLOT, FIRST_POST_MERGE_SLOT},
    performance::TimedExt,
    time_frames::{GrowingTimeFrame, LimitedTimeFrame, TimeFrame},
    units::WeiNewtype,
};

use super::SupplyParts;

use GrowingTimeFrame::*;
use LimitedTimeFrame::*;
use TimeFrame::*;

#[derive(Debug, PartialEq, Serialize)]
pub struct SupplyChange {
    from_slot: Slot,
    from_timestamp: DateTime<Utc>,
    from_supply: WeiNewtype,
    to_slot: Slot,
    to_timestamp: DateTime<Utc>,
    to_supply: WeiNewtype,
    change: WeiNewtype,
}

impl SupplyChange {
    fn new(from_slot: Slot, from_supply: WeiNewtype, to_slot: Slot, to_supply: WeiNewtype) -> Self {
        let change = to_supply - from_supply;
        let from_timestamp = from_slot.date_time();
        let to_timestamp = to_slot.date_time();

        Self {
            from_slot,
            from_timestamp,
            from_supply,
            to_slot,
            to_timestamp,
            to_supply,
            change,
        }
    }
}

#[derive(Debug, Serialize)]
pub struct SupplyChanges {
    d1: Option<SupplyChange>,
    d30: Option<SupplyChange>,
    d7: Option<SupplyChange>,
    h1: Option<SupplyChange>,
    m5: Option<SupplyChange>,
    since_burn: Option<SupplyChange>,
    since_merge: Option<SupplyChange>,
    slot: Slot,
    timestamp: DateTime<Utc>,
}

// This number was recorded before we has a rigorous definition of how to combine the execution and
// beacon chains to come up with a precise supply. After a rigorous supply is established for every
// block and slot it would be good to update this number.
const MERGE_SLOT_SUPPLY: WeiNewtype = WeiNewtype(120_521_140_924_621_298_474_538_089);

// Until we have an eth supply calculated by adding together per-block supply deltas, we're using
// an estimate based on glassnode data.
const LONDON_SLOT_SUPPLY_ESTIMATE: WeiNewtype = WeiNewtype(117_397_725_113_869_100_000_000_000);

async fn from_time_frame(
    executor: impl PgExecutor<'_>,
    time_frame: &TimeFrame,
    slot: &Slot,
    to_supply: WeiNewtype,
) -> Result<Option<SupplyChange>> {
    match time_frame {
        TimeFrame::Limited(tf) => {
            let from_slot = Slot::from_date_time_rounded_down(&(slot.date_time() - tf.duration()));

            sqlx::query!(
                r#"
                SELECT
                    balances_slot,
                    supply::TEXT AS "supply!"
                FROM
                    eth_supply
                WHERE
                    balances_slot = $1
                "#,
                from_slot.0
            )
            .fetch_optional(executor)
            .timed(&format!("get-supply-change-from-{}", tf))
            .await
            .map(|row| {
                row.map(|row| {
                    let from_supply: WeiNewtype =
                        row.supply.parse().expect("expect supply to be i128");
                    SupplyChange::new(from_slot, from_supply, *slot, to_supply)
                })
            })
            .context("query supply from balance")
        }
        TimeFrame::Growing(SinceBurn) => {
            let supply_change = SupplyChange::new(
                FIRST_POST_LONDON_SLOT,
                LONDON_SLOT_SUPPLY_ESTIMATE,
                *slot,
                to_supply,
            );
            Ok(Some(supply_change))
        }
        TimeFrame::Growing(SinceMerge) => {
            let supply_change =
                SupplyChange::new(FIRST_POST_MERGE_SLOT, MERGE_SLOT_SUPPLY, *slot, to_supply);
            Ok(Some(supply_change))
        }
    }
}

pub struct SupplyChangesStore<'a> {
    db_pool: &'a PgPool,
}

impl<'a> SupplyChangesStore<'a> {
    pub fn new(db_pool: &'a PgPool) -> Self {
        Self { db_pool }
    }

    pub async fn get(&self, slot: &Slot, supply_parts: &SupplyParts) -> Result<SupplyChanges> {
        let (m5, h1, d1, d7, d30, since_burn, since_merge) = try_join!(
            from_time_frame(self.db_pool, &Limited(Minute5), slot, supply_parts.supply()),
            from_time_frame(self.db_pool, &Limited(Hour1), slot, supply_parts.supply()),
            from_time_frame(self.db_pool, &Limited(Day1), slot, supply_parts.supply()),
            from_time_frame(self.db_pool, &Limited(Day7), slot, supply_parts.supply()),
            from_time_frame(self.db_pool, &Limited(Day30), slot, supply_parts.supply()),
            from_time_frame(
                self.db_pool,
                &Growing(SinceBurn),
                slot,
                supply_parts.supply()
            ),
            from_time_frame(
                self.db_pool,
                &Growing(SinceMerge),
                slot,
                supply_parts.supply()
            ),
        )?;

        let supply_changes = SupplyChanges {
            m5,
            h1,
            d1,
            d7,
            d30,
            since_burn,
            since_merge,
            slot: *slot,
            timestamp: slot.date_time(),
        };

        Ok(supply_changes)
    }
}

#[cfg(test)]
mod tests {
    use sqlx::Acquire;

    use crate::{
        db,
        eth_supply::test::store_test_eth_supply,
        units::{EthNewtype, WeiNewtype},
    };

    use super::*;

    #[tokio::test]
    async fn m5_supply_change_test() {
        let mut connection = db::get_test_db_connection().await;
        let mut transaction = connection.begin().await.unwrap();

        let from_slot = Slot(0);
        let to_slot = Slot(25);
        store_test_eth_supply(&mut transaction, &from_slot, EthNewtype(10.0))
            .await
            .unwrap();
        let to_supply = EthNewtype(20.0).into();
        let supply_change_m5 = from_time_frame(
            &mut transaction,
            &TimeFrame::Limited(LimitedTimeFrame::Minute5),
            &to_slot,
            to_supply,
        )
        .await
        .unwrap()
        .unwrap();

        assert_eq!(
            SupplyChange {
                from_slot,
                from_timestamp: from_slot.date_time(),
                from_supply: WeiNewtype::from_eth(10),
                to_slot,
                to_timestamp: to_slot.date_time(),
                to_supply: WeiNewtype::from_eth(20),
                change: WeiNewtype::from_eth(10),
            },
            supply_change_m5
        );
    }
}
