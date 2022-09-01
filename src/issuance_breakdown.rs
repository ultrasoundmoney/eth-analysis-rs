use crate::caching::CacheKey;
use crate::eth_units::{GweiAmount, GWEI_PER_ETH};
use crate::key_value_store::KeyValue;
use crate::{beacon_chain, caching, config, etherscan, key_value_store};
use serde::Serialize;
use sqlx::{PgConnection, PgExecutor};

#[derive(Debug, Serialize)]
struct IssuanceBreakdown {
    crowd_sale: GweiAmount,
    early_contributors: GweiAmount,
    ethereum_foundation: GweiAmount,
    proof_of_stake: GweiAmount,
    proof_of_work: GweiAmount,
}

async fn store_issuance_breakdown<'a>(
    pg_executor: impl PgExecutor<'a>,
    issuance_breakdown: &IssuanceBreakdown,
) {
    key_value_store::set_value(
        pg_executor,
        KeyValue {
            key: &CacheKey::IssuanceBreakdown.to_db_key(),
            value: &serde_json::to_value(issuance_breakdown).unwrap(),
        },
    )
    .await
}

pub async fn update_issuance_breakdown() {
    tracing_subscriber::fmt::init();

    tracing::info!("updating issuance breakdown");

    let mut connection: PgConnection = sqlx::Connection::connect(&config::get_db_url())
        .await
        .unwrap();

    sqlx::migrate!().run(&mut connection).await.unwrap();

    let crowd_sale = GweiAmount::from_eth_f64(60_108_506.26);
    tracing::debug!("crowd sale: {} ETH", crowd_sale.0 / GWEI_PER_ETH);

    let early_contributors_without_vitalik = GweiAmount::from_eth_f64(8_418_324.49);
    let vitalik = GweiAmount::from_eth_f64(696_940.59);
    let early_contributors = early_contributors_without_vitalik + vitalik;
    tracing::debug!(
        "early contributors: {} ETH",
        early_contributors.0 / GWEI_PER_ETH
    );

    let ethereum_foundation = GweiAmount::from_eth_f64(3_483_159.75);
    tracing::debug!(
        "ethereum foundation: {} ETH",
        ethereum_foundation.0 / GWEI_PER_ETH
    );

    let proof_of_stake = beacon_chain::get_current_issuance(&mut connection)
        .await
        .unwrap();
    tracing::debug!(
        "proof of stake issuance: {} ETH",
        proof_of_stake.0 / GWEI_PER_ETH
    );

    let eth_supply_2 = etherscan::get_eth_supply_2().await.unwrap();

    tracing::debug!(
        "eth supply without beacon issuance, with burnt fees: {} ETH",
        GweiAmount::from(eth_supply_2.eth_supply.clone()).0 / GWEI_PER_ETH
    );

    let proof_of_work = GweiAmount::from(eth_supply_2.eth_supply)
        - crowd_sale
        - ethereum_foundation
        - early_contributors;
    tracing::debug!(
        "proof of work issuance: {} ETH",
        proof_of_work.0 / GWEI_PER_ETH
    );

    let issuance_breakdown = IssuanceBreakdown {
        crowd_sale,
        early_contributors,
        ethereum_foundation,
        proof_of_stake,
        proof_of_work,
    };

    store_issuance_breakdown(&mut connection, &issuance_breakdown).await;

    caching::publish_cache_update(&mut connection, CacheKey::IssuanceBreakdown).await;

    tracing::info!("done updating issuance breakdown")
}
