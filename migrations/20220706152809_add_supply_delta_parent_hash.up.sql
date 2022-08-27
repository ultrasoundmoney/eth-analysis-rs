ALTER TABLE execution_supply_deltas ADD COLUMN parent_hash TEXT NOT NULL;
CREATE UNIQUE INDEX execution_supply_deltas_parent_hash_idx ON execution_supply_deltas (parent_hash);
