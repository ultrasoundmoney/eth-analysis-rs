//! # Burn Totals
//! This module sums total burn. Limited, and growing time frames.
//!
//! ## Growing time frames
//! Get the last valid sum. If one exists, continue, otherwise, start from zero. Continue with
//! addition.
//!
//! ### GetLastValidSum
//! Check the hash of the last sum exists in the blocks table. If it does, return the sum, if it
//! does not, iterate backwards through the sums, dropping as we go, until one is found with a hash in the blocks
//! table. If none is found, report none exists.
//!
//! ### Addition
//! Gather the burn of each block after the last valid sum. Sum them iteratively and remember the
//! last one hundred sums calculated. Add them to the sums table.
//!
//! ## Limited time frames
//! Get the last valid sum. If one exists, continue, otherwise, start from zero. Continue with
//! expiration, then addition.
//!
//! ### GetLastValidSum
//! Check the hash of the last sum exists in the blocks table. If it does, return the sum, if it
//! does not, iterate backwards through the in-frame blocks, dropping as we go, whilst subtracting from the sum, until one is found with a hash in the blocks
//! table. If none is found, report none exists.
//!
//! ### Expiration
//! Iterate forward through the in-frame blocks, dropping as we go, whilst subtracting from the
//! sum, until one is found which is timestamped after NOW - TIME_FRAME_INTERVAL.
//!
//! ### Addition
//! Gather the burn of each block after the last valid sum. Remember each in-frame block, and
//! update the sum.
//!
//! ## Table schema
//! time_frame,block_number,block_hash,timestamp,burn,sum

mod store;

use chrono::{DateTime, Utc};
use futures::join;
use serde::Serialize;
use sqlx::{PgConnection, PgPool};
use tracing::{debug, info};

use crate::{
    burn_rates::BurnRates,
    burn_sums::store::BurnSumStore,
    caching::{self, CacheKey},
    execution_chain::{BlockNumber, BlockRange, ExecutionNodeBlock},
    time_frames::{GrowingTimeFrame, LimitedTimeFrame, TimeFrame},
    units::{EthNewtype, WeiNewtype},
};

#[derive(Debug, PartialEq, Serialize)]
pub struct BurnSums {
    pub since_merge: EthNewtype,
    pub since_burn: EthNewtype,
    pub d1: EthNewtype,
    pub d30: EthNewtype,
    pub d7: EthNewtype,
    pub h1: EthNewtype,
    pub m5: EthNewtype,
}

#[derive(Debug)]
pub struct BurnSumRecord {
    time_frame: TimeFrame,
    block_number: BlockNumber,
    block_hash: String,
    timestamp: DateTime<Utc>,
    sum: WeiNewtype,
}

pub async fn on_rollback(connection: &mut PgConnection, block_number_gte: &BlockNumber) {
    BurnSumStore::delete_new_sums_tx(connection, block_number_gte).await;
}

async fn expired_burn_for_time_frame(
    burn_sum_store: &BurnSumStore<'_>,
    last_burn_sum: &BurnSumRecord,
    limited_time_frame: &LimitedTimeFrame,
) -> WeiNewtype {
    // The earliest blocks included in the last limited time frame sum.
    let last_sum_earliest_timestamp = last_burn_sum.timestamp - limited_time_frame.duration();

    // The earliest block which should be excluded in the current limited time frame sum. Meaning,
    // every block, before _but not including_ this timestamp should be removed from the sum.
    let current_sum_earliest_timestamp = Utc::now() - limited_time_frame.duration();

    debug!(
        last_sum_earliest_timestamp = last_sum_earliest_timestamp.to_rfc3339(),
        current_sum_earliest_timestamp = current_sum_earliest_timestamp.to_rfc3339(),
        "calculating expired burn for time frame"
    );

    let expired_included_burn = burn_sum_store
        .burn_sum_from_time_range(last_sum_earliest_timestamp, current_sum_earliest_timestamp)
        .await;

    let expired_block_count = burn_sum_store
        .blocks_from_time_range(last_sum_earliest_timestamp, current_sum_earliest_timestamp)
        .await
        .len();

    debug!(
        %limited_time_frame,
        %last_sum_earliest_timestamp,
        %current_sum_earliest_timestamp,
        expired_block_count,
        %expired_included_burn,
        "calculated expired burn"
    );

    expired_included_burn
}

async fn calc_new_burn_sum_record(
    burn_sum_store: &BurnSumStore<'_>,
    block: &ExecutionNodeBlock,
    time_frame: &TimeFrame,
) -> BurnSumRecord {
    match burn_sum_store.last_burn_sum(time_frame).await {
        None => {
            info!("no burn sum record found, calculating from scratch for time frame {time_frame}");

            let burn_sum = burn_sum_store
                .burn_sum_from_time_frame(time_frame, block)
                .await;

            debug!(%burn_sum, %time_frame, "new burn sum from scratch");

            BurnSumRecord {
                time_frame: *time_frame,
                block_number: block.number,
                block_hash: block.hash.clone(),
                timestamp: block.timestamp,
                sum: burn_sum,
            }
        }
        Some(last_burn_sum) => {
            debug!(?last_burn_sum, "found last burn sum record");

            // Add in-time-frame burn.
            let new_range = BlockRange::new(last_burn_sum.block_number + 1, block.number);
            let new_burn = burn_sum_store.burn_sum_from_block_range(&new_range).await;
            debug!(
                block_count = new_range.count(),
                %new_burn, %time_frame, "new in-time-frame-burn"
            );

            match time_frame {
                // If the time frame is unlimited, add the new burn.
                TimeFrame::Growing(_) => {
                    let new_burn_sum = last_burn_sum.sum + new_burn;
                    BurnSumRecord {
                        time_frame: *time_frame,
                        block_number: block.number,
                        block_hash: block.hash.clone(),
                        timestamp: block.timestamp,
                        sum: new_burn_sum,
                    }
                }
                // If the time frame is limited, add the new burn and remove expired-but-included burn.
                TimeFrame::Limited(limited_time_frame) => {
                    let expired_included_burn = expired_burn_for_time_frame(
                        burn_sum_store,
                        &last_burn_sum,
                        limited_time_frame,
                    )
                    .await;

                    let new_burn_sum = last_burn_sum.sum + new_burn - expired_included_burn;
                    BurnSumRecord {
                        time_frame: *time_frame,
                        block_number: block.number,
                        block_hash: block.hash.clone(),
                        timestamp: block.timestamp,
                        sum: new_burn_sum,
                    }
                }
            }
        }
    }
}

pub async fn on_new_block(db_pool: &PgPool, block: &ExecutionNodeBlock) {
    use GrowingTimeFrame::*;
    use LimitedTimeFrame::*;
    use TimeFrame::*;

    let burn_sum_store = BurnSumStore::new(db_pool);

    let (since_burn, since_merge, d30, d7, d1, h1, m5) = join!(
        calc_new_burn_sum_record(&burn_sum_store, block, &Growing(SinceBurn)),
        calc_new_burn_sum_record(&burn_sum_store, block, &Growing(SinceMerge)),
        calc_new_burn_sum_record(&burn_sum_store, block, &Limited(Day30)),
        calc_new_burn_sum_record(&burn_sum_store, block, &Limited(Day7)),
        calc_new_burn_sum_record(&burn_sum_store, block, &Limited(Day1)),
        calc_new_burn_sum_record(&burn_sum_store, block, &Limited(Hour1)),
        calc_new_burn_sum_record(&burn_sum_store, block, &Limited(Minute5)),
    );

    let burn_sums = [&since_burn, &since_merge, &d30, &d7, &d1, &h1, &m5];
    burn_sum_store.store_burn_sums(burn_sums).await;

    // Drop old sums.
    burn_sum_store.delete_old_sums(block.number).await;

    let burn_sums = BurnSums {
        since_burn: since_burn.sum.into(),
        since_merge: since_merge.sum.into(),
        d30: d30.sum.into(),
        d7: d7.sum.into(),
        d1: d1.sum.into(),
        h1: h1.sum.into(),
        m5: m5.sum.into(),
    };

    debug!("calculated new burn sums");

    let burn_rates: BurnRates = (&burn_sums).into();

    debug!("calculated new burn rates");

    caching::update_and_publish(db_pool, &CacheKey::BurnSums, burn_sums)
        .await
        .unwrap();

    caching::update_and_publish(db_pool, &CacheKey::BurnRates, burn_rates)
        .await
        .unwrap();
}
