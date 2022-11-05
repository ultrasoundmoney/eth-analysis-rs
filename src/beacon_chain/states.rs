use chrono::{DateTime, Utc};
use sqlx::{postgres::PgRow, PgExecutor, Row};

use super::beacon_time;

// Beacon chain slots are defined as 12 second periods starting from genesis. With u32 our program
// would overflow when the slot number passes 4_294_967_295. u32::MAX * 12 seconds = ~1633 years.
pub type Slot = u32;

trait SlotExt {
    fn get_date_time(&self) -> DateTime<Utc>;
}

impl SlotExt for Slot {
    fn get_date_time(&self) -> DateTime<Utc> {
        beacon_time::get_date_time_from_slot(self)
    }
}

struct BeaconStateRow {
    block_root: Option<String>,
    slot: i32,
    state_root: String,
}

#[derive(Debug, PartialEq)]
pub struct BeaconState {
    pub block_root: Option<String>,
    pub slot: Slot,
    pub state_root: String,
}

impl From<BeaconStateRow> for BeaconState {
    fn from(row: BeaconStateRow) -> Self {
        Self {
            block_root: row.block_root,
            slot: row.slot as u32,
            state_root: row.state_root,
        }
    }
}

pub async fn get_last_state(executor: impl PgExecutor<'_>) -> Option<BeaconState> {
    sqlx::query_as!(
        BeaconStateRow,
        r#"
            SELECT
                beacon_states.state_root,
                beacon_states.slot,
                beacon_blocks.block_root AS "block_root?"
            FROM beacon_states
            LEFT JOIN beacon_blocks ON beacon_states.state_root = beacon_blocks.state_root
            ORDER BY slot DESC
            LIMIT 1
        "#,
    )
    .fetch_optional(executor)
    .await
    .unwrap()
    .map(|row| row.into())
}

pub async fn store_state<'a>(
    executor: impl PgExecutor<'a>,
    state_root: &str,
    slot: &Slot,
) -> sqlx::Result<()> {
    sqlx::query!(
        "
            INSERT INTO beacon_states (state_root, slot) VALUES ($1, $2)
        ",
        state_root,
        *slot as i32
    )
    .execute(executor)
    .await?;

    Ok(())
}

pub async fn get_state_root_by_slot(
    executor: impl PgExecutor<'_>,
    slot: &Slot,
) -> sqlx::Result<String> {
    sqlx::query(
        r#"
            SELECT
                beacon_states.state_root
            FROM
                beacon_states
            WHERE
                slot = $1
        "#,
    )
    .bind(*slot as i32)
    .map(|row: PgRow| row.get::<String, _>("state_root"))
    .fetch_one(executor)
    .await
}

pub async fn delete_states<'a>(executor: impl PgExecutor<'a>, greater_than_or_equal: &Slot) {
    sqlx::query!(
        "
            DELETE FROM beacon_states
            WHERE slot >= $1
        ",
        *greater_than_or_equal as i32
    )
    .execute(executor)
    .await
    .unwrap();
}

#[cfg(test)]
mod tests {
    use crate::db;

    use super::*;
    use sqlx::Connection;

    #[tokio::test]
    async fn store_state_test() {
        let mut connection = db::get_test_db().await;
        let mut transaction = connection.begin().await.unwrap();

        store_state(&mut transaction, "0xstate_root", &0)
            .await
            .unwrap();

        let state = get_last_state(&mut transaction).await.unwrap();

        assert_eq!(
            state,
            BeaconState {
                slot: 0,
                state_root: "0xstate_root".to_string(),
                block_root: None
            }
        );
    }

    #[tokio::test]
    async fn get_last_state_test() {
        let mut connection = db::get_test_db().await;
        let mut transaction = connection.begin().await.unwrap();

        store_state(&mut transaction, "0xstate_root_1", &0)
            .await
            .unwrap();

        store_state(&mut transaction, "0xstate_root_2", &1)
            .await
            .unwrap();

        let state = get_last_state(&mut transaction).await.unwrap();

        assert_eq!(
            state,
            BeaconState {
                slot: 1,
                state_root: "0xstate_root_2".to_string(),
                block_root: None
            }
        );
    }

    #[tokio::test]
    async fn delete_state_test() {
        let mut connection = db::get_test_db().await;
        let mut transaction = connection.begin().await.unwrap();

        store_state(&mut transaction, "0xstate_root", &0)
            .await
            .unwrap();

        let state = get_last_state(&mut transaction).await;
        assert!(state.is_some());

        delete_states(&mut transaction, &0).await;

        let state_after = get_last_state(&mut transaction).await;
        assert!(state_after.is_none());
    }

    #[tokio::test]
    async fn get_state_root_by_slot_test() {
        let mut connection = db::get_test_db().await;
        let mut transaction = connection.begin().await.unwrap();

        store_state(&mut transaction, "0xtest", &0).await.unwrap();

        let state_root = get_state_root_by_slot(&mut transaction, &0).await.unwrap();

        assert_eq!(state_root, "0xtest");
    }
}
