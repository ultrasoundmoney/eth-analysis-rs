use chrono::{DateTime, Utc};
use serde::Serialize;

use crate::{beacon_chain::Slot, units::WeiNewtype};

use super::{over_time::SupplyAtTime, SupplyOverTime};

#[derive(Debug, PartialEq, Eq, Serialize)]
pub struct SupplyChange {
    from_slot: Slot,
    from_timestamp: DateTime<Utc>,
    from_supply: WeiNewtype,
    to_slot: Slot,
    to_timestamp: DateTime<Utc>,
    to_supply: WeiNewtype,
    change: WeiNewtype,
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

impl From<&Vec<SupplyAtTime>> for SupplyChange {
    fn from(supply_at_time: &Vec<SupplyAtTime>) -> Self {
        let first = supply_at_time
            .first()
            .expect("expect at least one supply in list");
        let last = supply_at_time
            .last()
            .expect("expect at least one supply in list");
        let change = last.supply - first.supply;

        SupplyChange {
            change: change.into(),
            from_slot: Slot::from_date_time(&first.timestamp)
                .unwrap_or_else(|| Slot::from_date_time_rounded_down(&first.timestamp)),
            from_supply: first.supply.into(),
            from_timestamp: first.timestamp,
            to_slot: Slot::from_date_time(&last.timestamp)
                .unwrap_or_else(|| Slot::from_date_time_rounded_down(&last.timestamp)),
            to_supply: last.supply.into(),
            to_timestamp: last.timestamp,
        }
    }
}

impl From<&SupplyOverTime> for SupplyChanges {
    fn from(supply_over_time: &SupplyOverTime) -> Self {
        Self {
            d1: Some((&supply_over_time.d1).into()),
            d30: Some((&supply_over_time.d30).into()),
            d7: Some((&supply_over_time.d7).into()),
            h1: Some((&supply_over_time.h1).into()),
            m5: Some((&supply_over_time.m5).into()),
            since_burn: Some((&supply_over_time.since_burn).into()),
            since_merge: Some((&supply_over_time.since_merge).into()),
            slot: supply_over_time.slot,
            timestamp: supply_over_time.timestamp,
        }
    }
}
