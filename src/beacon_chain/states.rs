use sqlx::PgExecutor;

struct BeaconStateRow {
    block_root: Option<String>,
    slot: i32,
    state_root: String,
}

pub struct BeaconState {
    pub block_root: Option<String>,
    pub slot: u32,
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
