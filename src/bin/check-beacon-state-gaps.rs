use std::error::Error;

use eth_analysis::config;
use futures::{StreamExt, TryStreamExt};
use sqlx::{PgConnection, Row};

#[tokio::main]
pub async fn main() -> Result<(), Box<dyn Error>> {
    tracing_subscriber::fmt::init();

    tracing::info!("checking for gaps in beacon states");

    let mut connection: PgConnection = sqlx::Connection::connect(&config::get_db_url())
        .await
        .unwrap();

    let mut rows = sqlx::query(
        "
            SELECT slot FROM beacon_states
            ORDER BY slot ASC
        ",
    )
    .fetch(&mut connection)
    .map(|row| {
        row.map(|row| {
            let slot: i32 = row.get("slot");
            slot as u32
        })
    });

    let mut last_slot = None;
    while let Some(slot) = rows.try_next().await? {
        if let Some(last_slot) = last_slot {
            if last_slot != slot - 1 {
                panic!("last slot: {last_slot}, current slot: {slot}")
            }
        }

        last_slot = Some(slot);
    }

    tracing::info!("done checking beacon states for gaps");

    Ok(())
}
