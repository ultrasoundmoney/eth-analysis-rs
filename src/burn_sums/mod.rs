//! # Burn Totals
//! This module sums total burn. Limited, and growing time frames. Records are mere burn sums with
//! some metadata. Not "highest" or "lowest" out of all.

mod store;

use std::{cmp::Ordering, collections::HashMap, ops::Index};

use chrono::{DateTime, Utc};
use enum_iterator::all;
use futures::future::join_all;
use serde::Serialize;
use sqlx::{PgConnection, PgPool};
use tracing::debug;

use crate::{
    burn_sums::store::BurnSumStore,
    caching::{self, CacheKey},
    execution_chain::{BlockNumber, BlockRange, BlockStore, ExecutionNodeBlock},
    performance::TimedExt,
    time_frames::{LimitedTimeFrame, TimeFrame},
    units::{EthNewtype, UsdNewtype, WeiNewtype},
};

#[derive(Debug, PartialEq)]
struct WeiUsdAmount {
    wei: WeiNewtype,
    usd: UsdNewtype,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
pub struct EthUsdAmount {
    pub eth: EthNewtype,
    pub usd: UsdNewtype,
}

impl EthUsdAmount {
    pub fn yearly_rate_from_time_frame(&self, time_frame: TimeFrame) -> EthUsdAmount {
        let eth = self.eth.0 / time_frame.years_f64();
        let usd = self.usd.0 / time_frame.years_f64();
        EthUsdAmount {
            eth: EthNewtype(eth),
            usd: UsdNewtype(usd),
        }
    }
}

pub type BurnSums = HashMap<TimeFrame, EthUsdAmount>;

fn burn_sums_from_vec(records: &[BurnSumRecord]) -> BurnSums {
    records
        .iter()
        .map(|record| {
            let time_frame = record.time_frame;
            let eth = record.sum_wei.into();
            let usd = record.sum_usd;
            let eth_usd_amount = EthUsdAmount { eth, usd };
            (time_frame, eth_usd_amount)
        })
        .collect()
}

#[derive(Debug, PartialEq, Serialize)]
pub struct BurnSumsEnvelope {
    pub block_number: BlockNumber,
    pub burn_sums: BurnSums,
    pub timestamp: DateTime<Utc>,
}

impl Index<TimeFrame> for BurnSums {
    type Output = EthUsdAmount;

    fn index(&self, time_frame: TimeFrame) -> &Self::Output {
        self.get(&time_frame).expect("time frame not found")
    }
}

#[derive(Debug)]
pub struct BurnSumRecord {
    first_included_block_number: BlockNumber,
    last_included_block_hash: String,
    last_included_block_number: BlockNumber,
    sum_usd: UsdNewtype,
    sum_wei: WeiNewtype,
    time_frame: TimeFrame,
    timestamp: DateTime<Utc>,
}

pub async fn on_rollback(connection: &mut PgConnection, block_number_gte: &BlockNumber) {
    BurnSumStore::delete_new_sums_tx(connection, block_number_gte).await;
}

async fn expired_burn_from(
    block_store: &BlockStore<'_>,
    burn_sum_store: &BurnSumStore<'_>,
    last_burn_sum: &BurnSumRecord,
    block: &ExecutionNodeBlock,
    limited_time_frame: &LimitedTimeFrame,
) -> Option<(BlockNumber, WeiNewtype, UsdNewtype)> {
    // The first included block for the next sum may have jumped forward zero or
    // more blocks. Meaning zero or more blocks are now considered expired but
    // still included for this limited time frame sum.
    let age_limit = block.timestamp - limited_time_frame.duration();
    let first_included_block_number = block_store
        .first_number_after_or_at(&age_limit)
        .await
        .expect(
            "failed to get first block number after or at block.timestamp - limited_time_frame",
        );

    match first_included_block_number.cmp(&last_burn_sum.first_included_block_number) {
        Ordering::Less => {
            // Current block number should be > the last sum's block number. So the
            // first included block should be greater or equal too, yet the first included
            // block number for the current block is smaller. This should be
            // impossible.
            panic!("first included block number for current block is smaller than the last sum's first included block number");
        }
        Ordering::Equal => {
            // The last sum included the same blocks as the current block, so the
            // new sum is the same as the last sum.
            debug!("first included block number is the same for the current block and the last sum, no expired burn");
            None
        }
        Ordering::Greater => {
            let expired_block_range = BlockRange::new(
                last_burn_sum.first_included_block_number,
                first_included_block_number - 1,
            );

            let (expired_included_burn_wei, expired_included_burn_usd) = burn_sum_store
                .burn_sum_from_block_range(&expired_block_range)
                .await;

            debug!(%expired_block_range, %expired_included_burn_wei, %expired_included_burn_usd, %limited_time_frame, "subtracting expired burn");

            Some((
                first_included_block_number,
                expired_included_burn_wei,
                expired_included_burn_usd,
            ))
        }
    }
}

async fn calc_new_burn_sum_record_from_scratch(
    burn_sum_store: &BurnSumStore<'_>,
    block: &ExecutionNodeBlock,
    time_frame: &TimeFrame,
) -> BurnSumRecord {
    debug!(%block.number, %block.hash, %time_frame, "calculating new burn sum record from scratch");
    let range = BlockRange::from_last_plus_time_frame(&block.number, time_frame);
    let (sum_wei, sum_usd) = burn_sum_store.burn_sum_from_block_range(&range).await;
    BurnSumRecord {
        first_included_block_number: range.start,
        last_included_block_hash: block.hash.clone(),
        last_included_block_number: range.end,
        sum_wei,
        sum_usd,
        time_frame: *time_frame,
        timestamp: block.timestamp,
    }
}

async fn calc_new_burn_sum_record_from_last(
    block_store: &BlockStore<'_>,
    burn_sum_store: &BurnSumStore<'_>,
    last_burn_sum: &BurnSumRecord,
    block: &ExecutionNodeBlock,
    time_frame: &TimeFrame,
) -> BurnSumRecord {
    debug!(%block.number, %block.hash, %time_frame, "calculating new burn sum record from last");
    let new_burn_range =
        BlockRange::new(last_burn_sum.last_included_block_number + 1, block.number);
    let (new_burn_wei, new_burn_usd) = burn_sum_store
        .burn_sum_from_block_range(&new_burn_range)
        .await;

    let expired_burn_sum = match time_frame {
        TimeFrame::Limited(limited_time_frame) => {
            expired_burn_from(
                block_store,
                burn_sum_store,
                last_burn_sum,
                block,
                limited_time_frame,
            )
            .await
        }
        TimeFrame::Growing(_) => None,
    };

    let (sum_wei, sum_usd) = match expired_burn_sum {
        Some((_, expired_burn_sum_wei, expired_burn_sum_usd)) => {
            let sum_wei = new_burn_wei - expired_burn_sum_wei + last_burn_sum.sum_wei;
            let sum_usd = new_burn_usd - expired_burn_sum_usd + last_burn_sum.sum_usd;
            (sum_wei, sum_usd)
        }
        None => {
            let sum_wei = new_burn_wei + last_burn_sum.sum_wei;
            let sum_usd = new_burn_usd + last_burn_sum.sum_usd;
            (sum_wei, sum_usd)
        }
    };

    let first_included_block_number = expired_burn_sum
        .map(|(first_included_block_number, _, _)| first_included_block_number)
        // If there is no expired burn, the first included did not change.
        .unwrap_or(last_burn_sum.first_included_block_number);

    BurnSumRecord {
        first_included_block_number,
        last_included_block_hash: block.hash.clone(),
        last_included_block_number: new_burn_range.end,
        sum_wei,
        sum_usd,
        time_frame: *time_frame,
        timestamp: block.timestamp,
    }
}

async fn calc_new_burn_sum_record(
    block_store: &BlockStore<'_>,
    burn_sum_store: &BurnSumStore<'_>,
    block: &ExecutionNodeBlock,
    time_frame: TimeFrame,
) -> BurnSumRecord {
    match burn_sum_store.last_burn_sum(&time_frame).await {
        Some(last_burn_sum) => {
            calc_new_burn_sum_record_from_last(
                block_store,
                burn_sum_store,
                &last_burn_sum,
                block,
                &time_frame,
            )
            .await
        }
        None => calc_new_burn_sum_record_from_scratch(burn_sum_store, block, &time_frame).await,
    }
}

pub async fn on_new_block(db_pool: &PgPool, block: &ExecutionNodeBlock) -> BurnSumsEnvelope {
    let block_store = BlockStore::new(db_pool);
    let burn_sum_store = BurnSumStore::new(db_pool);

    let futures = all::<TimeFrame>().map(|time_frame| {
        calc_new_burn_sum_record(&block_store, &burn_sum_store, block, time_frame)
            .timed(&format!("calc_new_burn_sum_record_{time_frame}"))
    });
    let burn_sum_records = join_all(futures).await;

    burn_sum_store.store_burn_sums(&burn_sum_records).await;

    // Drop old sums.
    burn_sum_store.delete_old_sums(block.number).await;

    let burn_sums_envelope = BurnSumsEnvelope {
        block_number: block.number,
        burn_sums: burn_sums_from_vec(&burn_sum_records),
        timestamp: block.timestamp,
    };

    debug!("calculated new burn sums");

    caching::update_and_publish(db_pool, &CacheKey::BurnSums, &burn_sums_envelope)
        .await
        .unwrap();

    burn_sums_envelope
}
