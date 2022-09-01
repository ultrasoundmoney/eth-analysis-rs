use std::cmp::Ordering;

use chrono::Utc;
use chrono::{DateTime, Duration};
use format_url::FormatUrl;
use serde::Deserialize;

use super::EthPrice;

#[derive(Debug, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
struct FtxCandle {
    open: f64,
    start_time: DateTime<Utc>,
}

#[derive(Debug, Deserialize)]
struct PriceResponse {
    result: Vec<FtxCandle>,
}

const FTX_API: &str = "https://ftx.com";

#[allow(dead_code)]
pub async fn get_price_by_minute(target_minute_rounded: DateTime<Utc>) -> Option<f64> {
    let url = FormatUrl::new(FTX_API)
        .with_path_template("/api/indexes/ETH/candles")
        .with_query_params(vec![
            ("resolution", &60.to_string()),
            (
                "start_time",
                &(target_minute_rounded.timestamp()).to_string(),
            ),
            (
                "end_time",
                &(target_minute_rounded + Duration::minutes(1))
                    .timestamp()
                    .to_string(),
            ),
        ])
        .format_url();

    let body = reqwest::get(url)
        .await
        .unwrap()
        .json::<PriceResponse>()
        .await
        .unwrap();

    if body.result.len() == 0 {
        None
    } else {
        Some(body.result[0].open)
    }
}

fn find_closest_price<'a>(
    prices: &'a [FtxCandle],
    target_minute_rounded: DateTime<Utc>,
) -> &'a FtxCandle {
    let mut best_distance = None;
    let mut best_candidate = None;

    for price in prices {
        let distance = (target_minute_rounded - price.start_time)
            .num_seconds()
            .abs();
        match best_distance {
            None => {
                best_distance = Some(distance);
                best_candidate = Some(price);
            }
            Some(current_best) => match distance.cmp(&current_best) {
                Ordering::Less => {
                    best_distance = Some(distance);
                    best_candidate = Some(price);
                }
                Ordering::Greater => {
                    // Prices are ordered oldest to youngest. As soon as the next price in the
                    // list is further from our target than the last, they'll only get further
                    // away, and we can stop searching.
                    break;
                }
                // We found a minute before and after our target at the exact same distance.
                // We do nothing and simply keep the first one we found (the older price).
                Ordering::Equal => (),
            },
        }
    }

    best_candidate.expect("one to be closest for non-empty prices")
}

pub async fn get_closest_price_by_minute(
    target_minute_rounded: DateTime<Utc>,
    max_distance: Duration,
) -> Option<f64> {
    // Create a period of width max_distance centered on start_of_minute.
    let start = target_minute_rounded - max_distance;
    let end = target_minute_rounded + max_distance;

    let url = FormatUrl::new(FTX_API)
        .with_path_template("/api/indexes/ETH/candles")
        .with_query_params(vec![
            ("resolution", &60.to_string()),
            ("start_time", &start.timestamp().to_string()),
            ("end_time", &end.timestamp().to_string()),
        ])
        .format_url();

    let body = reqwest::get(url)
        .await
        .unwrap()
        .json::<PriceResponse>()
        .await
        .unwrap();

    if body.result.len() == 0 {
        None
    } else {
        let closest_price = find_closest_price(&body.result, target_minute_rounded);
        Some(closest_price.open)
    }
}

enum SupportedResolution {
    Minute,
}

impl ToString for SupportedResolution {
    fn to_string(&self) -> String {
        match self {
            Self::Minute => "60".to_string(),
        }
    }
}

pub async fn get_most_recent_price() -> Option<EthPrice> {
    let now = Utc::now();

    // We return a price up to five minutes old.
    let start_time = now - Duration::minutes(5);

    let url = FormatUrl::new(FTX_API)
        .with_path_template("/api/indexes/ETH/candles")
        .with_query_params(vec![
            ("resolution", &SupportedResolution::Minute.to_string()),
            ("start_time", &start_time.timestamp().to_string()),
            ("end_time", &now.timestamp().to_string()),
        ])
        .format_url();

    let res = reqwest::get(url).await.unwrap();

    let body = res.json::<PriceResponse>().await.unwrap();

    body.result.last().map(|candle| EthPrice {
        timestamp: candle.start_time,
        price_usd: candle.open,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn utc_from_str_unsafe(rfc3339_str: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(rfc3339_str)
            .unwrap()
            .with_timezone(&Utc)
    }

    #[tokio::test]
    async fn get_missing_price_by_minute_test() {
        let missing_minute = utc_from_str_unsafe("2021-10-22T07:40:00Z");
        let price_usd = get_price_by_minute(missing_minute).await;
        assert_eq!(price_usd, None);
    }

    #[tokio::test]
    async fn get_price_by_minute_test() {
        let existing_minute = utc_from_str_unsafe("2021-10-22T07:35:00Z");
        let price_usd = get_price_by_minute(existing_minute).await;
        assert_eq!(price_usd, Some(4135.924229398));
    }

    #[tokio::test]
    async fn get_missing_closest_price_by_minute_test() {
        let missing_plus_one = utc_from_str_unsafe("2021-10-22T07:37:00Z");
        let price_usd = get_closest_price_by_minute(missing_plus_one, Duration::minutes(1)).await;
        assert_eq!(price_usd, None);
    }

    #[tokio::test]
    async fn get_closest_price_by_minute_test() {
        let existing_plus_two = utc_from_str_unsafe("2021-10-22T07:37:00Z");
        let price_usd = get_closest_price_by_minute(existing_plus_two, Duration::minutes(2)).await;
        assert_eq!(price_usd, Some(4135.924229398));
    }

    #[test]
    fn find_closest_before_test() {
        let price_1 = FtxCandle {
            start_time: utc_from_str_unsafe("2021-01-01T00:01:00Z"),
            open: 0.0,
        };
        let price_2 = FtxCandle {
            start_time: utc_from_str_unsafe("2021-01-01T00:04:00Z"),
            open: 1.0,
        };

        let prices = vec![price_1, price_2];

        let closest = find_closest_price(&prices, utc_from_str_unsafe("2021-01-01T00:02:00Z"));
        assert_eq!(*closest, prices[0]);
    }

    #[test]
    fn find_closest_after_test() {
        let price_1 = FtxCandle {
            start_time: utc_from_str_unsafe("2021-01-01T00:01:00Z"),
            open: 0.0,
        };
        let price_2 = FtxCandle {
            start_time: utc_from_str_unsafe("2021-01-01T00:04:00Z"),
            open: 1.0,
        };

        let prices = vec![price_1, price_2];

        let closest = find_closest_price(&prices, utc_from_str_unsafe("2021-01-01T00:03:00Z"));
        assert_eq!(*closest, prices[1]);
    }

    #[test]
    fn find_with_equal_distance_test() {
        let price_1 = FtxCandle {
            start_time: utc_from_str_unsafe("2021-01-01T00:01:00Z"),
            open: 0.0,
        };
        let price_2 = FtxCandle {
            start_time: utc_from_str_unsafe("2021-01-01T00:05:00Z"),
            open: 1.0,
        };

        let prices = vec![price_1, price_2];

        let closest = find_closest_price(&prices, utc_from_str_unsafe("2021-01-01T00:03:00Z"));
        assert_eq!(*closest, prices[0]);
    }

    #[tokio::test]
    async fn get_most_recent_price_test() {
        get_most_recent_price().await;
    }
}
