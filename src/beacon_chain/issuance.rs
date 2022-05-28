use sqlx::PgPool;

use super::slot_time;
use super::{gwei_amounts::GweiAmount, slot_time::FirstOfDaySlot};

pub async fn store_issuance_for_day(
    pool: &PgPool,
    state_root: &str,
    FirstOfDaySlot(slot): FirstOfDaySlot,
    gwei: GweiAmount,
) {
    let gwei: i64 = gwei.to_owned().into();

    sqlx::query!(
        "
            INSERT INTO beacon_issuance (timestamp, state_root, gwei) VALUES ($1, $2, $3)
        ",
        slot_time::get_timestamp(&slot),
        state_root,
        gwei,
    )
    .execute(pool)
    .await
    .unwrap();
}
