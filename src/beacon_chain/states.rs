mod heal;

use chrono::{DateTime, Utc};
use sqlx::PgExecutor;

use super::beacon_time;

pub use heal::heal_beacon_states;

// Beacon chain slots are defined as 12 second periods starting from genesis. With u32 our program
// would overflow when the slot number passes 2_147_483_647. i32::MAX * 12 seconds = ~817 years.
pub type Slot = i32;

trait SlotExt {
    fn get_date_time(&self) -> DateTime<Utc>;
}

impl SlotExt for Slot {
    fn get_date_time(&self) -> DateTime<Utc> {
        beacon_time::get_date_time_from_slot(self)
    }
}

#[derive(Debug, PartialEq)]
pub struct BeaconState {
    pub slot: Slot,
    pub state_root: String,
}

pub async fn get_last_state(executor: impl PgExecutor<'_>) -> Option<BeaconState> {
    sqlx::query_as!(
        BeaconState,
        "
            SELECT
                beacon_states.state_root,
                beacon_states.slot
            FROM beacon_states
            ORDER BY slot DESC
            LIMIT 1
        ",
    )
    .fetch_optional(executor)
    .await
    .unwrap()
    .map(|row| row.into())
}

pub async fn store_state(
    executor: impl PgExecutor<'_>,
    state_root: &str,
    slot: &Slot,
) -> sqlx::Result<()> {
    sqlx::query!(
        "
            INSERT INTO
                beacon_states
                (state_root, slot)
            VALUES
                ($1, $2)
        ",
        state_root,
        *slot,
    )
    .execute(executor)
    .await?;

    Ok(())
}

pub async fn get_state_root_by_slot(
    executor: impl PgExecutor<'_>,
    slot: &Slot,
) -> sqlx::Result<Option<String>> {
    let state_root = sqlx::query!(
        "
            SELECT
                state_root
            FROM
                beacon_states
            WHERE
                slot = $1
        ",
        *slot
    )
    .fetch_optional(executor)
    .await?
    .map(|row| row.state_root);

    Ok(state_root)
}

pub async fn delete_states(executor: impl PgExecutor<'_>, greater_than_or_equal: &Slot) {
    sqlx::query!(
        "
            DELETE FROM beacon_states
            WHERE slot >= $1
        ",
        *greater_than_or_equal
    )
    .execute(executor)
    .await
    .unwrap();
}

pub async fn delete_state(executor: impl PgExecutor<'_>, slot: &Slot) {
    sqlx::query!(
        "
            DELETE FROM beacon_states
            WHERE slot = $1
        ",
        *slot
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
            BeaconState {
                slot: 0,
                state_root: "0xstate_root".to_string(),
            },
            state,
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

        let state_root = get_state_root_by_slot(&mut transaction, &0)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(state_root, "0xtest");
    }
}
