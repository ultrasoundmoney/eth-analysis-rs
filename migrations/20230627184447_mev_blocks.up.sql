CREATE TABLE IF NOT EXISTS mev_blocks (
    bid_wei NUMERIC NOT NULL,
    block_hash TEXT NOT NULL PRIMARY KEY,
    block_number INTEGER NOT NULL,
    slot INTEGER NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS mev_slot_idx ON mev_blocks (slot);
CREATE INDEX IF NOT EXISTS mev_block_number_idx ON mev_blocks (block_number);
CREATE INDEX IF NOT EXISTS mev_timestamp_idx ON mev_blocks (timestamp);
