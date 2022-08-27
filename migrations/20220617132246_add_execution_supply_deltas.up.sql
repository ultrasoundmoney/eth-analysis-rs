CREATE TABLE IF NOT EXISTS execution_supply_deltas (
	block_hash text NOT NULL,
	block_number int4 NOT NULL UNIQUE,
	fee_burn int8 NOT NULL,
	fixed_reward int8 NOT NULL,
	supply_delta int8 NOT NULL,
	self_destruct int8 NOT NULL,
	uncles_reward int8 NOT NULL,
	CONSTRAINT execution_supply_deltas_pkey PRIMARY KEY (block_hash)
);
