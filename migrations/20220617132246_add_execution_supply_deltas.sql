CREATE TABLE IF NOT EXISTS execution_supply_deltas (
	block_number int4 NOT NULL,
	fee_burn float8 NOT NULL,
	fixed_reward float8 NOT NULL,
	hash text NOT NULL,
	supply_delta float8 NOT NULL,
	self_destruct float8 NOT NULL,
	uncles_reward float8 NOT NULL,
	CONSTRAINT execution_supply_deltas_pkey PRIMARY KEY (hash)
);
CREATE UNIQUE INDEX IF NOT EXISTS execution_supply_deltas_block_number_idx ON execution_supply_deltas USING btree (block_number);
