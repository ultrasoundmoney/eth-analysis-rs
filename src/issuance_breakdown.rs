use crate::eth_units::{GweiAmount, GWEI_PER_ETH};
use crate::key_value_store::KeyValue;
use crate::{beacon_chain, config, etherscan, key_value_store};
use serde::Serialize;
use sqlx::{PgConnection, PgExecutor};

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct IssuanceBreakdown {
    crowd_sale: GweiAmount,
    ethereum_foundation: GweiAmount,
    early_contributors: GweiAmount,
    proof_of_stake_issuance: GweiAmount,
    proof_of_work_issuance: GweiAmount,
}

const ISSUANCE_BREAKDOWN_CACHE_KEY: &str = "issuance-breakdown";

async fn store_issuance_breakdown<'a>(
    pg_executor: impl PgExecutor<'a>,
    issuance_breakdown: &IssuanceBreakdown,
) {
    key_value_store::set_value(
        pg_executor,
        KeyValue {
            key: ISSUANCE_BREAKDOWN_CACHE_KEY,
            value: serde_json::to_value(issuance_breakdown).unwrap(),
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

    let crowd_sale = GweiAmount::from_eth(60_000_000);
    tracing::debug!("crowd sale: {} ETH", crowd_sale.0 / GWEI_PER_ETH);

    let ethereum_foundation = GweiAmount::from_eth(6_000_000);
    tracing::debug!(
        "ethereum foundation: {} ETH",
        ethereum_foundation.0 / GWEI_PER_ETH
    );

    let early_contributors = GweiAmount::from_eth(6_000_000);
    tracing::debug!(
        "early contributors: {} ETH",
        early_contributors.0 / GWEI_PER_ETH
    );

    let proof_of_stake_issuance = beacon_chain::get_current_issuance(&mut connection)
        .await
        .unwrap();
    tracing::debug!(
        "proof of stake issuance: {} ETH",
        proof_of_stake_issuance.0 / GWEI_PER_ETH
    );

    let eth_supply_2 = etherscan::get_eth_supply_2().await.unwrap();

    tracing::debug!(
        "circulating supply: {} ETH",
        GweiAmount::from(eth_supply_2.eth_supply.clone()).0 / GWEI_PER_ETH
    );

    let proof_of_work_issuance = GweiAmount::from(eth_supply_2.eth_supply)
        - GweiAmount::from(eth_supply_2.burnt_fees)
        - GweiAmount::from(eth_supply_2.eth2_staking)
        - crowd_sale
        - ethereum_foundation
        - early_contributors;
    tracing::debug!(
        "proof of work issuance: {} ETH",
        proof_of_work_issuance.0 / GWEI_PER_ETH
    );

    let issuance_breakdown = IssuanceBreakdown {
        crowd_sale,
        ethereum_foundation,
        early_contributors,
        proof_of_work_issuance,
        proof_of_stake_issuance,
    };

    store_issuance_breakdown(&mut connection, &issuance_breakdown).await;

    tracing::info!("done updating issuance breakdown")
}
