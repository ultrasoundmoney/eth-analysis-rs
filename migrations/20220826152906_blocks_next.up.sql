CREATE TABLE blocks_next (
  base_fee_per_gas INT8 NOT NULL,
  difficulty INT8 NOT NULL,
  eth_price FLOAT8 NOT NULL,
  gas_used INT4 NOT NULL,
  hash TEXT PRIMARY KEY,
  number INT4 UNIQUE NOT NULL,
  parent_hash TEXT UNIQUE NOT NULL,
  timestamp TIMESTAMPTZ NOT NULL,
  total_difficulty NUMERIC NOT NULL
);

CREATE INDEX blocks_next_timestamp_idx ON blocks_next (timestamp);
