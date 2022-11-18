# Ethereum analysis services 🦇🔊🔍

The backend services to [ultrasound.money](https://ultrasound.money/) which analyze Ethereum.

These services are written with [ultrasound.money](https://ultrasound.money/) in mind. That is to say, this is not code we expect anyone to build on top of. At the same time, we do encourage peeking under the hood, making suggestions, raising issues, and re-using whatever is useful to you.

To give a rough overview of the code: there are many binaries, all invoking top-level functions from `lib.rs`. Some are intended to run as cronjobs, others continually listen and react to Ethereum node events, and yet others serve API requests.

## Dependencies
* Postgres
* Execution client (tested with Geth)
* Consensus client (tested with Lighthouse)
* Etherscan API key (required for issuance breakdown)
* Glassnode API key (required for supply projections)
* OpsGenie API key (required for phoenix, alert service)

## Environment Variables
```sh
BEACON_URL=http://****:5052
DATABASE_URL=postgresql://****
ETHERSCAN_API_KEY=****
GETH_URL=ws://****:8546/
GLASSNODE_API_KEY=****
OPSGENIE_API_KEY=****
SQLX_OFFLINE=true
```

## Usage
For runnable binaries see [the bin folder in this repo](https://github.com/ultrasoundmoney/eth-analysis-rs/tree/main/src/bin). After making any required env vars available one executes with cargo, e.g.
```sh
RUST_LOG=eth_analysis=info cargo run --bin sync-beacon-states
```

## Logs
Pass the env var `RUST_LOG` e.g. `RUST_LOG=eth_analysis=debug cargo run --bin serve`. For more examples see [the `env_logger` docs](https://docs.rs/env_logger/latest/env_logger/).
