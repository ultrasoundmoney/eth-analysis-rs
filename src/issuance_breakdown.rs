use anyhow::Result;
use serde::Serialize;
use tracing::{debug, info};

use crate::{
    beacon_chain::{IssuanceStore, IssuanceStorePostgres},
    caching::{self, CacheKey},
    db, etherscan, log,
    units::{EthNewtype, GweiImprecise, GweiNewtype},
};

#[derive(Debug, Serialize)]
struct IssuanceBreakdown {
    crowd_sale: GweiImprecise,
    early_contributors: GweiImprecise,
    ethereum_foundation: GweiImprecise,
    proof_of_stake: GweiImprecise,
    proof_of_work: GweiImprecise,
}

pub async fn update_issuance_breakdown() -> Result<()> {
    log::init_with_env();

    info!("updating issuance breakdown");

    let db_pool = db::get_db_pool("update-issuance-breakdown").await;

    sqlx::migrate!().run(&db_pool).await.unwrap();

    let issuance_store = IssuanceStorePostgres::new(&db_pool);

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

    let proof_of_stake = (&issuance_store).current_issuance().await;
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
        crowd_sale: crowd_sale.into(),
        early_contributors: early_contributors.into(),
        ethereum_foundation: ethereum_foundation.into(),
        proof_of_stake: proof_of_stake.into(),
        proof_of_work: proof_of_work.into(),
    };

    caching::update_and_publish(&db_pool, &CacheKey::IssuanceBreakdown, issuance_breakdown).await?;

    info!("done updating issuance breakdown");

    Ok(())
}
