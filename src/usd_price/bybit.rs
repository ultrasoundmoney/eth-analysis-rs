use std::{cmp::Ordering, ops::Sub};

use anyhow::{Context, Result};
use backoff::{self, Error, ExponentialBackoff};
use chrono::{DateTime, Duration, TimeZone, Utc};
use format_url::FormatUrl;
use serde::Deserialize;
use tracing::{debug, info, warn};

use super::EthPrice;

#[derive(Debug, Deserialize)]
struct BybitCandle {
    timestamp: String,
    usd: String,
    #[allow(unused)]
    high: String,
    #[allow(unused)]
    low: String,
    #[allow(unused)]
    close: String,
}

#[derive(Debug, Deserialize)]
struct BybitPriceResult {
    #[allow(unused)]
    symbol: String,
    #[allow(unused)]
    category: String,
    list: Vec<BybitCandle>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BybitPriceResponse {
    #[allow(unused)]
    ret_code: i64,
    #[allow(unused)]
    ret_msg: String,
    result: BybitPriceResult,
}

const BYBIT_API: &str = "https://api.bybit.com";

// 1min candles of index price made up of of Kraken, Coinbase, Bitstamp & Bitfinex spot price
async fn get_eth_candles(start: DateTime<Utc>, end: DateTime<Utc>) -> Result<Vec<EthPrice>> {
    let url = FormatUrl::new(BYBIT_API)
        .with_path_template("/derivatives/v3/public/index-price-kline")
        .with_query_params(vec![
            ("category", "inverse"),
            ("symbol", "ETHUSD"),
            ("interval", "1"),
            ("start", &start.timestamp_millis().to_string()),
            ("end", &end.timestamp_millis().to_string()),
        ])
        .format_url();

    backoff::future::retry(ExponentialBackoff::default(), || async {
        let candles = send_eth_price_request(&url).await.map_err(|err| {
            info!(%err, "error sending request to bybit, retrying");
            Error::transient(err)
        })?;
        if candles.is_empty() {
            warn!(%start, %end, "bybit returned no candles for the requested period");
        }

        Ok(candles)
    })
    .await
}

pub async fn send_eth_price_request(url: &str) -> Result<Vec<EthPrice>> {
    debug!("sending request to {}", url);

    let body = reqwest::get(url)
        .await?
        .json::<BybitPriceResponse>()
        .await?;

    let candles: Vec<EthPrice> = body
        .result
        .list
        .iter()
        .map(|c| {
            let timestamp_millis = c
                .timestamp
                .parse::<i64>()
                .expect("expect bybit candles to contain integer timestamps");
            let timestamp = Utc
                .timestamp_millis_opt(timestamp_millis)
                .earliest()
                .expect("expect bybit candles to contain millisecond timestamps");
            let usd = c
                .usd
                .parse::<f64>()
                .expect("expect bybit candles to contain float usd prices");
            EthPrice { timestamp, usd }
        })
        .rev() // Reverse so we get timestamps in ascending order
        .collect();

    Ok(candles)
}

// Return current 1min candle open price
pub async fn get_eth_price() -> Result<EthPrice> {
    let end = Utc::now();
    let start = end.sub(Duration::minutes(1));
    get_eth_candles(start, end).await.and_then(|cs| {
        cs.into_iter()
            .last()
            .context("tried to retrieve last element in empty array")
    })
}

fn find_closest_price(prices: &[EthPrice], target_minute_rounded: DateTime<Utc>) -> &'_ EthPrice {
    let mut best_distance = None;
    let mut best_candidate = None;

    for price in prices {
        let distance = (target_minute_rounded - price.timestamp)
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

    let candles = get_eth_candles(start, end).await.unwrap_or(Vec::new());

    if candles.is_empty() {
        None
    } else {
        let closest_price = find_closest_price(&candles, target_minute_rounded);
        Some(closest_price.usd)
    }
}

#[cfg(test)]
mod tests {
    use chrono::DurationRound;

    use super::*;

    #[tokio::test]
    async fn get_closest_price_by_minute_test() {
        let existing_plus_two = "2021-10-22T07:37:00Z".parse::<DateTime<Utc>>().unwrap();
        let usd = get_closest_price_by_minute(existing_plus_two, Duration::minutes(2)).await;
        assert_eq!(usd, Some(4134.16));
    }

    #[tokio::test]
    async fn includes_end_timestamp_test() {
        let start = "2022-10-01T00:00:00Z".parse::<DateTime<Utc>>().unwrap();
        let end = "2022-10-01T00:00:00Z".parse::<DateTime<Utc>>().unwrap();
        let result = get_eth_candles(start, end).await.unwrap();
        assert_eq!(result[0].timestamp, start);
    }

    #[test]
    fn find_closest_before_test() {
        let price_1 = EthPrice {
            timestamp: "2021-01-01T00:01:00Z".parse::<DateTime<Utc>>().unwrap(),
            usd: 0.0,
        };
        let price_2 = EthPrice {
            timestamp: "2021-01-01T00:04:00Z".parse::<DateTime<Utc>>().unwrap(),
            usd: 1.0,
        };

        let prices = vec![price_1, price_2];

        let closest = find_closest_price(
            &prices,
            "2021-01-01T00:02:00Z".parse::<DateTime<Utc>>().unwrap(),
        );
        assert_eq!(*closest, prices[0]);
    }

    #[test]
    fn find_closest_after_test() {
        let price_1 = EthPrice {
            timestamp: "2021-01-01T00:01:00Z".parse::<DateTime<Utc>>().unwrap(),
            usd: 0.0,
        };
        let price_2 = EthPrice {
            timestamp: "2021-01-01T00:04:00Z".parse::<DateTime<Utc>>().unwrap(),
            usd: 1.0,
        };

        let prices = vec![price_1, price_2];

        let closest = find_closest_price(
            &prices,
            "2021-01-01T00:03:00Z".parse::<DateTime<Utc>>().unwrap(),
        );
        assert_eq!(*closest, prices[1]);
    }

    #[test]
    fn find_with_equal_distance_test() {
        let price_1 = EthPrice {
            timestamp: "2021-01-01T00:01:00Z".parse::<DateTime<Utc>>().unwrap(),
            usd: 0.0,
        };
        let price_2 = EthPrice {
            timestamp: "2021-01-01T00:05:00Z".parse::<DateTime<Utc>>().unwrap(),
            usd: 1.0,
        };

        let prices = vec![price_1, price_2];

        let closest = find_closest_price(
            &prices,
            "2021-01-01T00:03:00Z".parse::<DateTime<Utc>>().unwrap(),
        );
        assert_eq!(*closest, prices[0]);
    }

    #[tokio::test]
    async fn returns_in_progress_candle_test() {
        let now = Utc::now();
        let rounded_down = now.duration_trunc(Duration::minutes(1)).unwrap();
        let result = get_eth_price().await.unwrap();
        assert_eq!(result.timestamp, rounded_down);
    }
}
