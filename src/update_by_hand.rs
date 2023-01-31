use anyhow::Result;
use console::Term;
use dialoguer::{Input, MultiSelect, Select};
use serde::Serialize;
use sqlx::{types::Json, PgConnection};
use std::env;

#[derive(Serialize)]
struct EthInDefiOld {
    #[serde(rename = "ethLocked")]
    eth: f64,
    timestamp: i64,
}

#[derive(Serialize)]
struct EthInDefi {
    eth: f64,
    timestamp: i64,
}

async fn set_eth_in_defi_for_env(env: &str, eth: f64) -> Result<()> {
    let db_url = env::var(format!("DATABASE_URL_{}", env.to_uppercase())).expect(
        "three database url env vars DATABASE_URL_DEV, DATABASE_URL_STAG, and DATABASE_URL_PROD",
    );
    let mut conn: PgConnection = sqlx::Connection::connect(&db_url).await?;

    let eth_in_defi_at_date = EthInDefi {
        eth,
        timestamp: chrono::Utc::now().timestamp(),
    };

    // TODO: after services have switched to the eth-in-defi key, drop the eth-locked key.
    let eth_in_defi_at_date_old = EthInDefiOld {
        eth,
        timestamp: chrono::Utc::now().timestamp(),
    };

    sqlx::query(
        "
            INSERT INTO key_value_store (key, value)
            VALUES ($1, $2)
            ON CONFLICT (key) DO UPDATE SET
                value = $2
        ",
    )
    .bind("eth-in-defi")
    .bind(Json(eth_in_defi_at_date))
    .execute(&mut conn)
    .await?;

    sqlx::query(
        "
            INSERT INTO key_value_store (key, value)
            VALUES ($1, $2)
            ON CONFLICT (key) DO UPDATE SET
                value = $2
        ",
    )
    .bind("eth-locked")
    .bind(Json(eth_in_defi_at_date_old))
    .execute(&mut conn)
    .await?;

    Ok(())
}

async fn set_eth_in_defi() -> Result<()> {
    let env_options = vec!["dev", "stag", "prod"];
    let selected_envs: Vec<String> = MultiSelect::new()
        .items(&env_options)
        .interact()?
        .into_iter()
        .map(|i| env_options[i].to_string())
        .collect();

    let eth_in_defi = Input::<f64>::new()
        .with_prompt("how much eth is currently in defi?")
        .interact()?;

    let mut handles = Vec::new();

    for env in selected_envs.iter().cloned() {
        let handle = tokio::spawn(async move {
            set_eth_in_defi_for_env(&env, eth_in_defi).await.unwrap();
        });

        handles.push(handle);
    }

    for env in selected_envs.iter().cloned() {
        set_eth_in_defi_for_env(&env, eth_in_defi).await.unwrap()
    }

    println!(
        "stored {} eth in defi for envs {}",
        eth_in_defi,
        selected_envs.join(",")
    );

    Ok(())
}

pub async fn run_cli() -> Result<()> {
    let term = Term::stdout();
    term.clear_screen().unwrap();
    term.write_line("= update by hand tool =").unwrap();
    term.write_line("\n").unwrap();

    let target_options = vec!["eth-in-defi", "merge-stats"];
    let target = Select::new()
        .with_prompt("which datapoint would you like to update?")
        .items(&target_options)
        .default(0)
        .interact()?;

    if target == 0 {
        set_eth_in_defi().await?
    }

    term.write_line("goodbye!")?;

    Ok(())
}
