use core::panic;
use std::{collections::HashSet, error::Error};

use futures::{StreamExt, TryStreamExt};
use sqlx::{postgres::PgRow, PgConnection, Row};

use crate::db::DB_URL;

pub async fn check_beacon_state_gaps() -> Result<(), Box<dyn Error>> {
    tracing_subscriber::fmt::init();

    tracing::info!("checking for gaps in beacon states");

    let mut connection: PgConnection = sqlx::Connection::connect(&*DB_URL).await.unwrap();

    {
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

        tracing::info!("done checking beacon state slots for gaps");
    }

    let mut block_rows = sqlx::query(
        "
            SELECT beacon_blocks.block_root, beacon_blocks.parent_root FROM beacon_states
            JOIN beacon_blocks ON beacon_states.state_root = beacon_blocks.state_root
            ORDER BY beacon_states.slot ASC
        ",
    )
    .map(|row: PgRow| {
        let block_root = row.get::<String, _>("block_root");
        let parent_root = row.get::<String, _>("parent_root");
        (block_root, parent_root)
    })
    .fetch(&mut connection);

    let mut hashes = HashSet::new();

    while let Some((block_root, parent_root)) = block_rows.try_next().await? {
        hashes.insert(block_root.clone());

        let is_parent_known = hashes.contains(&parent_root);

        if !is_parent_known {
            panic!(
                "block_root {}, parent_root {:?}, missing",
                &block_root, &parent_root
            );
        }
    }

    tracing::info!("done checking beacon blocks hashes for gaps");

    Ok(())
}
