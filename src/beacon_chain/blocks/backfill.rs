use anyhow::Result;
use sqlx::PgPool;
use tracing::{info, instrument};

#[instrument(skip(db_pool))]
pub async fn backfill_beacon_block_slots(db_pool: &PgPool) -> Result<()> {
    info!("starting to backfill beacon_blocks.slot from beacon_states.slot");

    let mut transaction = db_pool.begin().await?;

    let rows_affected = sqlx::query!(
        r#"
        UPDATE beacon_blocks
        SET slot = beacon_states.slot
        FROM beacon_states
        WHERE beacon_blocks.state_root = beacon_states.state_root
        AND beacon_blocks.slot IS NULL
        "#
    )
    .execute(&mut *transaction)
    .await?
    .rows_affected();

    transaction.commit().await?;

    info!(
        rows_affected,
        "finished backfilling beacon_blocks.slot"
    );

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        beacon_chain::{
            blocks::{self as blocks_store, store_block, GENESIS_PARENT_ROOT},
            node::{BeaconBlock, BeaconBlockBody, BeaconHeader, BeaconHeaderEnvelope},
            states::{self as states_store, store_state},
            BeaconHeaderSignedEnvelopeBuilder, Slot,
        },
        db,
        units::GweiNewtype,
    };

    #[tokio::test]
    async fn test_backfill_beacon_block_slots() -> Result<()> {
        let test_db = db::tests::TestDb::new().await;
        let pool = &test_db.pool;

        let test_id = "backfill_slots";
        let slot_0 = Slot(0);
        let slot_1 = Slot(1);

        let header_0 = BeaconHeaderSignedEnvelopeBuilder::new(&format!("{}_0", test_id))
            .slot(slot_0)
            .build();
        let block_0 = BeaconBlock {
            body: BeaconBlockBody {
                deposits: vec![],
                execution_payload: None,
                execution_requests: None,
            },
            parent_root: GENESIS_PARENT_ROOT.to_string(),
            slot: slot_0,
            state_root: header_0.state_root().to_string(),
        };

        store_state(pool, header_0.state_root(), slot_0).await;
        // Store initially without slot by calling an older interface version or null
        sqlx::query!(
            "
            INSERT INTO beacon_blocks (
                block_root,
                state_root,
                parent_root,
                slot -- insert with NULL slot initially
            )
            VALUES ($1, $2, $3, NULL)
            ",
            header_0.root,
            header_0.state_root(),
            header_0.parent_root()
        )
        .execute(pool)
        .await?;


        let header_1 = BeaconHeaderSignedEnvelopeBuilder::new(&format!("{}_1", test_id))
            .slot(slot_1)
            .parent_header(&header_0)
            .build();
        let block_1 = BeaconBlock {
            body: BeaconBlockBody {
                deposits: vec![],
                execution_payload: None,
                execution_requests: None,
            },
            parent_root: header_0.root.clone(),
            slot: slot_1,
            state_root: header_1.state_root().to_string(),
        };
        store_state(pool, header_1.state_root(), slot_1).await;
        // This one will be stored with the slot directly by the updated store_block
        store_block(
            pool,
            &block_1,
            &GweiNewtype(0),
            &GweiNewtype(0),
            &GweiNewtype(0),
            &GweiNewtype(0),
            &header_1,
            slot_1,
        )
        .await;

        // Check that slot is NULL for block 0 before backfill
        let block_row_before_0: Option<i32> = sqlx::query_scalar!(
            "SELECT slot FROM beacon_blocks WHERE block_root = $1",
            header_0.root
        )
        .fetch_optional(pool)
        .await?
        .flatten();
        assert_eq!(block_row_before_0, None);

        // Check that slot is NOT NULL for block 1
         let block_row_before_1: Option<i32> = sqlx::query_scalar!(
            "SELECT slot FROM beacon_blocks WHERE block_root = $1",
            header_1.root
        )
        .fetch_one(pool)
        .await?;
        assert_eq!(block_row_before_1, Some(slot_1.0));


        backfill_beacon_block_slots(pool).await?;

        let block_row_after_0: Option<i32> = sqlx::query_scalar!(
            "SELECT slot FROM beacon_blocks WHERE block_root = $1",
            header_0.root
        )
        .fetch_one(pool)
        .await?;
        assert_eq!(block_row_after_0, Some(slot_0.0));
        
        let block_row_after_1: Option<i32> = sqlx::query_scalar!(
            "SELECT slot FROM beacon_blocks WHERE block_root = $1",
            header_1.root
        )
        .fetch_one(pool)
        .await?;
        assert_eq!(block_row_after_1, Some(slot_1.0));

        Ok(())
    }
}