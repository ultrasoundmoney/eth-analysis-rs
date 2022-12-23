mod heal;

use sqlx::PgExecutor;

use super::Slot;

pub use heal::heal_beacon_states;

#[derive(Debug, PartialEq, Eq)]
pub struct BeaconState {
    pub slot: Slot,
    pub state_root: String,
}

pub async fn get_last_state(executor: impl PgExecutor<'_>) -> Option<BeaconState> {
    sqlx::query_as!(
        BeaconState,
        r#"
            SELECT
                beacon_states.state_root,
                beacon_states.slot AS "slot: Slot"
            FROM beacon_states
            ORDER BY slot DESC
            LIMIT 1
        "#,
    )
    .fetch_optional(executor)
    .await
    .unwrap()
}

pub async fn store_state(executor: impl PgExecutor<'_>, state_root: &str, slot: &Slot) {
    sqlx::query!(
        "
            INSERT INTO
                beacon_states
                (state_root, slot)
            VALUES
                ($1, $2)
        ",
        state_root,
        slot.0,
    )
    .execute(executor)
    .await
    .unwrap();
}

pub async fn get_state_root_by_slot(executor: impl PgExecutor<'_>, slot: &Slot) -> Option<String> {
    sqlx::query!(
        "
            SELECT
                state_root
            FROM
                beacon_states
            WHERE
                slot = $1
        ",
        slot.0
    )
    .fetch_optional(executor)
    .await
    .unwrap()
    .map(|row| row.state_root)
}

pub async fn delete_states(executor: impl PgExecutor<'_>, greater_than_or_equal: &Slot) {
    sqlx::query!(
        "
            DELETE FROM beacon_states
            WHERE slot >= $1
        ",
        greater_than_or_equal.0
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
        slot.0
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

        store_state(&mut transaction, "0xstate_root", &Slot(0)).await;

        let state = get_last_state(&mut transaction).await.unwrap();

        assert_eq!(
            BeaconState {
                slot: Slot(0),
                state_root: "0xstate_root".to_string(),
            },
            state,
        );
    }

    #[tokio::test]
    async fn get_last_state_test() {
        let mut connection = db::get_test_db().await;
        let mut transaction = connection.begin().await.unwrap();

        store_state(&mut transaction, "0xstate_root_1", &Slot(0)).await;

        store_state(&mut transaction, "0xstate_root_2", &Slot(1)).await;

        let state = get_last_state(&mut transaction).await.unwrap();

        assert_eq!(
            state,
            BeaconState {
                slot: Slot(1),
                state_root: "0xstate_root_2".to_string(),
            }
        );
    }

    #[tokio::test]
    async fn delete_state_test() {
        let mut connection = db::get_test_db().await;
        let mut transaction = connection.begin().await.unwrap();

        store_state(&mut transaction, "0xstate_root", &Slot(0)).await;

        let state = get_last_state(&mut transaction).await;
        assert!(state.is_some());

        delete_states(&mut transaction, &Slot(0)).await;

        let state_after = get_last_state(&mut transaction).await;
        assert!(state_after.is_none());
    }

    #[tokio::test]
    async fn get_state_root_by_slot_test() {
        let mut connection = db::get_test_db().await;
        let mut transaction = connection.begin().await.unwrap();

        store_state(&mut transaction, "0xtest", &Slot(0)).await;

        let state_root = get_state_root_by_slot(&mut transaction, &Slot(0))
            .await
            .unwrap();

        assert_eq!(state_root, "0xtest");
    }
}
