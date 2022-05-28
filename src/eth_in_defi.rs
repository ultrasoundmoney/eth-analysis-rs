use crate::defi_llama::{self, Protocol};
use pit_wall::Progress;
use sqlx::PgPool;

#[allow(dead_code)]
#[derive(Debug)]
struct EthInDefiAtDate {
    timestamp: chrono::DateTime<chrono::Utc>,
    eth: f64,
}

async fn store_eth_in_defi(
    pool: &PgPool,
    eth: &f64,
) -> sqlx::Result<sqlx::postgres::PgQueryResult> {
    sqlx::query_as!(
        EthInDefiAtDate,
        "INSERT INTO eth_in_defi (timestamp, eth) VALUES ($1, $2)",
        chrono::Utc::now(),
        eth
    )
    .execute(pool)
    .await
}

fn print_eth_like_in_protocol(protocol: &Protocol) -> Option<()> {
    let last_tokens_at_date = protocol
        .chain_tvls
        .ethereum
        .as_ref()
        .and_then(|chain_tvl| chain_tvl.tokens.last())?;

    for key in last_tokens_at_date.tokens.keys() {
        if key.contains("ETH") {
            log::info!(
                "protocol {} - {}, contains a token that may be ETH, named: {}",
                protocol.name,
                protocol.id,
                key
            );
        };
    }

    Some(())
}

pub async fn update(pool: &PgPool) {
    let protocols = defi_llama::get_protocols();

    log::info!("{} protocols to crawl", protocols.len());

    let mut progress = Progress::new("crawl protocols", protocols.len().try_into().unwrap());

    let eth_in_protocols = protocols
        .iter()
        .map(|protocol| defi_llama::get_protocol(&protocol.slug))
        .fold(0f64, |sum, protocol| {
            let eth_in_protocol = get_eth_in_protocol(&protocol);

            print_eth_like_in_protocol(&protocol);

            match eth_in_protocol {
                None => log::debug!("no wETH in {} - {}", protocol.name, protocol.id),
                Some(weth) => log::debug!("{} wETH in {} - {}", weth, protocol.name, protocol.id),
            };

            progress.inc_work_done();
            if progress.work_done != 0 && progress.work_done % 10 == 0 {
                log::debug!("{}", progress.get_progress_string());
            }

            sum + eth_in_protocol.unwrap_or(&0f64)
        });

    store_eth_in_defi(pool, &eth_in_protocols).await.unwrap();

    log::info!(
        "counted {} wETH total in all protocols on Defi Llama",
        eth_in_protocols
    );
}

fn get_eth_in_protocol(protocol: &defi_llama::Protocol) -> Option<&f64> {
    protocol
        .chain_tvls
        .ethereum
        .as_ref()?
        .tokens
        .last()?
        .tokens
        .get("WETH")
}
