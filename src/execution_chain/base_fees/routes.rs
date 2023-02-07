use std::collections::HashMap;

use axum::{extract::Query, response::IntoResponse};
use reqwest::StatusCode;

use crate::{
    caching::CacheKey,
    serve::{cached_get, StateExtension},
    time_frames::{GrowingTimeFrame, LimitedTimeFrame, TimeFrame},
};

pub async fn base_fee_per_gas_stats(
    state: StateExtension,
    Query(params): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    use GrowingTimeFrame::*;
    use LimitedTimeFrame::*;
    use TimeFrame::*;

    match params.get("time_frame") {
        Some(time_frame) => match time_frame.as_str() {
            "m5" => cached_get(
                state,
                &CacheKey::BaseFeePerGasStatsTimeFrame(Limited(Minute5)),
            )
            .await
            .into_response(),
            "h1" => cached_get(
                state,
                &CacheKey::BaseFeePerGasStatsTimeFrame(Limited(Hour1)),
            )
            .await
            .into_response(),
            "d1" => cached_get(state, &CacheKey::BaseFeePerGasStatsTimeFrame(Limited(Day1)))
                .await
                .into_response(),
            "d7" => cached_get(state, &CacheKey::BaseFeePerGasStatsTimeFrame(Limited(Day7)))
                .await
                .into_response(),
            "d30" => cached_get(
                state,
                &CacheKey::BaseFeePerGasStatsTimeFrame(Limited(Day30)),
            )
            .await
            .into_response(),
            "since_burn" => cached_get(
                state,
                &CacheKey::BaseFeePerGasStatsTimeFrame(Growing(SinceBurn)),
            )
            .await
            .into_response(),
            "since_merge" => cached_get(
                state,
                &CacheKey::BaseFeePerGasStatsTimeFrame(Growing(SinceMerge)),
            )
            .await
            .into_response(),
            _ => StatusCode::BAD_REQUEST.into_response(),
        },
        None => cached_get(state, &CacheKey::BaseFeePerGasStats)
            .await
            .into_response(),
    }
}
