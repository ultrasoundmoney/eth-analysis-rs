use anyhow::Result;
use serde::Serialize;
use sqlx::{Connection, PgConnection};
use tracing::{debug, info};

use crate::{
    beacon_chain,
    caching::{self, CacheKey},
    db, etherscan, key_value_store, log,
    units::{EthNewtype, GweiNewtype},
};

#[derive(Debug, Serialize)]
struct IssuanceBreakdown {
    crowd_sale: GweiNewtype,
    early_contributors: GweiNewtype,
    ethereum_foundation: GweiNewtype,
    proof_of_stake: GweiNewtype,
    proof_of_work: GweiNewtype,
}

pub async fn update_issuance_breakdown() -> Result<()> {
    log::init_with_env();

    info!("updating issuance breakdown");

    let mut connection: PgConnection =
        PgConnection::connect(&db::get_db_url_with_name("update-issuance-breakdown"))
            .await
            .unwrap();

    sqlx::migrate!().run(&mut connection).await.unwrap();

    let crowd_sale: GweiNewtype = EthNewtype(60_108_506.26).into();
    debug!(
        "crowd sale: {} ETH",
        crowd_sale.0 / EthNewtype::GWEI_PER_ETH
    );

    let early_contributors_without_vitalik = EthNewtype(8_418_324.49);
    let vitalik = EthNewtype(696_940.59);
    let early_contributors = early_contributors_without_vitalik + vitalik;
    debug!("early contributors: {} ETH", early_contributors.0);

    let ethereum_foundation = EthNewtype(3_483_159.75);
    debug!("ethereum foundation: {} ETH", ethereum_foundation.0);

    let proof_of_stake = beacon_chain::get_current_issuance(&mut connection).await;
    debug!(
        "proof of stake issuance: {} ETH",
        Into::<EthNewtype>::into(proof_of_stake)
    );

    let eth_supply_2 = etherscan::get_eth_supply_2().await.unwrap();

    debug!(
        "eth supply without beacon issuance, with burnt fees: {} ETH",
        Into::<EthNewtype>::into(eth_supply_2.eth_supply_min_beacon_issuance_plus_burn)
    );

    let eth_supply_min_beacon_issuance_plus_burn_gwei: GweiNewtype =
        eth_supply_2.eth_supply_min_beacon_issuance_plus_burn.into();
    let proof_of_work = eth_supply_min_beacon_issuance_plus_burn_gwei
        - crowd_sale
        - ethereum_foundation.into()
        - early_contributors.into();
    debug!(
        "proof of work issuance: {} ETH",
        Into::<EthNewtype>::into(proof_of_work)
    );

    let issuance_breakdown = IssuanceBreakdown {
        crowd_sale,
        early_contributors: early_contributors.into(),
        ethereum_foundation: ethereum_foundation.into(),
        proof_of_stake,
        proof_of_work,
    };

    key_value_store::set_value(
        &mut connection,
        &CacheKey::IssuanceBreakdown.to_db_key(),
        &serde_json::to_value(issuance_breakdown).unwrap(),
    )
    .await?;

    caching::publish_cache_update(&mut connection, &CacheKey::IssuanceBreakdown).await?;

    info!("done updating issuance breakdown");

    Ok(())
}
