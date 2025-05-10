use std::collections::HashMap;

use axum::{extract::Query, http::StatusCode, response::IntoResponse};
use chrono::Duration;
use enum_iterator::all;
use lazy_static::lazy_static;
use tracing::warn;

use crate::{
    caching::CacheKey,
    serve::{self, StateExtension},
    time_frames::{GrowingTimeFrame, LimitedTimeFrame, TimeFrame},
};

struct CachePair {
    max_age: Duration,
    stale_while_revalidate: Duration,
}

lazy_static! {
    static ref CACHE_DURATION_MAP: HashMap<TimeFrame, CachePair> = {
        use GrowingTimeFrame::*;
        use LimitedTimeFrame::*;
        use TimeFrame::*;

        HashMap::from_iter(all::<TimeFrame>().map(|time_frame| {
            let (max_age, stale_while_revalidate) = match time_frame {
                Limited(Minute5) => (Duration::seconds(4), Duration::minutes(2)),
                Limited(Hour1) => (Duration::seconds(4), Duration::minutes(15)),
                Limited(Day1) => (Duration::minutes(5), Duration::hours(1)),
                Limited(Day7) => (Duration::minutes(5), Duration::days(1)),
                Limited(Day30) => (Duration::minutes(30), Duration::days(2)),
                Growing(SinceBurn) => (Duration::hours(1), Duration::days(14)),
                Growing(SinceMerge) => (Duration::hours(1), Duration::days(14)),
            };
            (
                time_frame,
                CachePair {
                    max_age,
                    stale_while_revalidate,
                },
            )
        }))
    };
}

pub async fn blob_fee_per_gas_stats(
    state: StateExtension,
    Query(params): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    match params.get("time_frame") {
        Some(time_frame_param) => match time_frame_param.parse::<TimeFrame>() {
            Ok(time_frame) => serve::cached_get_with_custom_duration(
                state,
                &CacheKey::BlobFeePerGasStatsTimeFrame(time_frame),
                &CACHE_DURATION_MAP[&time_frame].max_age,
                &CACHE_DURATION_MAP[&time_frame].stale_while_revalidate,
            )
            .await
            .into_response(),
            Err(_) => {
                warn!("Invalid time_frame parameter: {}", time_frame_param);
                StatusCode::BAD_REQUEST.into_response()
            }
        },
        None => serve::cached_get(state, &CacheKey::BlobFeePerGasStats)
            .await
            .into_response(),
    }
}

pub async fn base_fee_per_gas_stats(
    state: StateExtension,
    Query(params): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    match params.get("time_frame") {
        Some(time_frame_param) => match time_frame_param.parse::<TimeFrame>() {
            Ok(time_frame) => serve::cached_get_with_custom_duration(
                state,
                &CacheKey::BaseFeePerGasStatsTimeFrame(time_frame),
                &CACHE_DURATION_MAP[&time_frame].max_age,
                &CACHE_DURATION_MAP[&time_frame].stale_while_revalidate,
            )
            .await
            .into_response(),
            Err(_) => {
                warn!("Invalid time_frame parameter: {}", time_frame_param);
                StatusCode::BAD_REQUEST.into_response()
            }
        },
        None => serve::cached_get(state, &CacheKey::BaseFeePerGasStats)
            .await
            .into_response(),
    }
}
