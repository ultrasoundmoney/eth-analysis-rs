CREATE TABLE
  daily_supply_glassnode (
    timestamp TIMESTAMPTZ NOT NULL,
    supply FLOAT8 NOT NULL,
    PRIMARY KEY (timestamp)
  );
