use std::collections::HashMap;

use axum::{extract::Query, response::IntoResponse};
use chrono::Duration;
use lazy_static::lazy_static;
use reqwest::StatusCode;

use crate::{
    caching::CacheKey,
    serve::{self, StateExtension},
    time_frames::{GrowingTimeFrame, LimitedTimeFrame, TimeFrame},
};

lazy_static! {
    static ref FOUR_SECONDS: Duration = Duration::seconds(4);
    static ref TWO_MINUTES: Duration = Duration::minutes(2);
    static ref FIVE_MINUTES: Duration = Duration::minutes(5);
    static ref FIFTEEN_MINUTES: Duration = Duration::minutes(15);
    static ref THIRTY_MINUTES: Duration = Duration::minutes(30);
    static ref ONE_HOUR: Duration = Duration::hours(1);
    static ref ONE_DAY: Duration = Duration::days(1);
    static ref TWO_DAYS: Duration = Duration::days(2);
    static ref FOURTEEN_DAYS: Duration = Duration::days(14);
}

pub async fn base_fee_per_gas_stats(
    state: StateExtension,
    Query(params): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    use GrowingTimeFrame::*;
    use LimitedTimeFrame::*;
    use TimeFrame::*;

    match params.get("time_frame") {
        Some(time_frame) => match time_frame.as_str() {
            "m5" => serve::cached_get_with_custom_duration(
                state,
                &CacheKey::BaseFeePerGasStatsTimeFrame(Limited(Minute5)),
                &FOUR_SECONDS,
                &TWO_MINUTES,
            )
            .await
            .into_response(),
            "h1" => serve::cached_get_with_custom_duration(
                state,
                &CacheKey::BaseFeePerGasStatsTimeFrame(Limited(Hour1)),
                &FOUR_SECONDS,
                &FIFTEEN_MINUTES,
            )
            .await
            .into_response(),
            "d1" => serve::cached_get_with_custom_duration(
                state,
                &CacheKey::BaseFeePerGasStatsTimeFrame(Limited(Day1)),
                &FIVE_MINUTES,
                &ONE_HOUR,
            )
            .await
            .into_response(),
            "d7" => serve::cached_get_with_custom_duration(
                state,
                &CacheKey::BaseFeePerGasStatsTimeFrame(Limited(Day7)),
                &FIVE_MINUTES,
                &ONE_DAY,
            )
            .await
            .into_response(),
            "d30" => serve::cached_get_with_custom_duration(
                state,
                &CacheKey::BaseFeePerGasStatsTimeFrame(Limited(Day30)),
                &THIRTY_MINUTES,
                &TWO_DAYS,
            )
            .await
            .into_response(),
            "since_burn" => serve::cached_get_with_custom_duration(
                state,
                &CacheKey::BaseFeePerGasStatsTimeFrame(Growing(SinceBurn)),
                &ONE_HOUR,
                &FOURTEEN_DAYS,
            )
            .await
            .into_response(),
            "since_merge" => serve::cached_get_with_custom_duration(
                state,
                &CacheKey::BaseFeePerGasStatsTimeFrame(Growing(SinceMerge)),
                &ONE_HOUR,
                &FOURTEEN_DAYS,
            )
            .await
            .into_response(),
            _ => StatusCode::BAD_REQUEST.into_response(),
        },
        None => serve::cached_get(state, &CacheKey::BaseFeePerGasStats)
            .await
            .into_response(),
    }
}
