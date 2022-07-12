use sqlx::PgExecutor;

// Beacon chain slots are defined as 12 second periods starting from genesis. With u32 our program
// would overflow when the slot number passes 4_294_967_295. u32::MAX * 12 seconds = ~1633 years.
pub type Slot = u32;

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

pub async fn get_last_state<'a>(executor: impl PgExecutor<'a>) -> Option<BeaconState> {
    sqlx::query_as!(
        BeaconStateRow,
        r#"
            SELECT
                beacon_states.state_root,
                beacon_states.slot,
                beacon_blocks.block_root AS "block_root?"
            FROM beacon_states
            LEFT JOIN beacon_blocks ON beacon_blocks.state_root = beacon_states.state_root
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
    slot: &u32,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config;
    use serial_test::serial;
    use sqlx::PgConnection;

    async fn clean_tables<'a>(pg_exec: impl PgExecutor<'a>) {
        sqlx::query("TRUNCATE beacon_states CASCADE")
            .execute(pg_exec)
            .await
            .unwrap();
    }

    #[tokio::test]
    #[serial]
    async fn store_state_test() {
        let mut connection: PgConnection = sqlx::Connection::connect(&config::get_db_url())
            .await
            .unwrap();

        store_state(&mut connection, "0xstate_root", &0)
            .await
            .unwrap();

        let state = get_last_state(&mut connection).await.unwrap();

        clean_tables(&mut connection).await;

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
    #[serial]
    async fn get_last_state_test() {
        let mut connection: PgConnection = sqlx::Connection::connect(&config::get_db_url())
            .await
            .unwrap();

        store_state(&mut connection, "0xstate_root_1", &0)
            .await
            .unwrap();

        store_state(&mut connection, "0xstate_root_2", &1)
            .await
            .unwrap();

        let state = get_last_state(&mut connection).await.unwrap();

        clean_tables(&mut connection).await;

        assert_eq!(
            state,
            BeaconState {
                slot: 1,
                state_root: "0xstate_root_2".to_string(),
                block_root: None
            }
        );
    }
}
