use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use futures::try_join;
use serde::Serialize;
use sqlx::{PgExecutor, PgPool};

use crate::{
    beacon_chain::Slot,
    caching::{self, CacheKey},
    eth_units::WeiNewtype,
    performance::TimedExt,
    time_frames::{LimitedTimeFrame, TimeFrame},
};

use super::SupplyParts;

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
    m5: Option<SupplyChange>,
    h1: Option<SupplyChange>,
    d1: Option<SupplyChange>,
    d7: Option<SupplyChange>,
    d30: Option<SupplyChange>,
    since_merge: Option<SupplyChange>,
    since_burn: Option<SupplyChange>,
}

impl SupplyChanges {
    fn new(m5: Option<SupplyChange>, h1: Option<SupplyChange>) -> Self {
        SupplyChanges {
            m5,
            h1,
            d1: None,
            d7: None,
            d30: None,
            since_merge: None,
            since_burn: None,
        }
    }
}

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
                        (&row.supply).parse().expect("expect supply to be i128");
                    SupplyChange::new(from_slot, from_supply, *slot, to_supply)
                })
            })
            .context("query supply from balance")
        }
        _ => unimplemented!(),
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
        use LimitedTimeFrame::*;
        use TimeFrame::*;

        let (m5, h1) = try_join!(
            from_time_frame(self.db_pool, &Limited(Minute5), slot, supply_parts.supply()),
            from_time_frame(self.db_pool, &Limited(Hour1), slot, supply_parts.supply())
        )?;

        let supply_changes = SupplyChanges::new(m5, h1);

        Ok(supply_changes)
    }
}

pub async fn update_and_publish(
    db_pool: &PgPool,
    slot: &Slot,
    supply_parts: &SupplyParts,
) -> Result<()> {
    let supply_changes = SupplyChangesStore::new(db_pool)
        .get(slot, supply_parts)
        .await?;
    caching::set_value(db_pool, &CacheKey::SupplyChanges, &supply_changes).await?;
    caching::publish_cache_update(db_pool, &CacheKey::SupplyChanges).await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use sqlx::Acquire;

    use crate::{
        db,
        eth_supply::test::store_test_eth_supply,
        eth_units::{GweiNewtype, WeiNewtype},
    };

    use super::*;

    #[tokio::test]
    async fn m5_supply_change_test() {
        let mut connection = db::get_test_db().await;
        let mut transaction = connection.begin().await.unwrap();

        let from_slot = Slot(0);
        let to_slot = Slot(25);
        store_test_eth_supply(&mut transaction, &from_slot, 10.0)
            .await
            .unwrap();
        let to_supply = GweiNewtype::from_eth(20).wei();
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
                from_slot: from_slot,
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
