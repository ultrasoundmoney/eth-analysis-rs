CREATE TABLE IF NOT EXISTS public.blocks (
	hash text NOT NULL,
	"number" int4 NOT NULL,
	mined_at timestamptz NOT NULL,
	tips float8 NULL,
	base_fee_sum float8 NULL,
	contract_creation_sum float8 NULL,
	eth_transfer_sum float8 NULL,
	base_fee_per_gas int8 NULL,
	gas_used int8 NULL,
	eth_price float8 NULL,
	base_fee_sum_256 numeric(78) NULL,
	CONSTRAINT blocks_number_key UNIQUE (number),
	CONSTRAINT blocks_pkey PRIMARY KEY (hash)
);
CREATE INDEX IF NOT EXISTS blocks_mined_at_idx ON public.blocks USING btree (mined_at);
CREATE INDEX IF NOT EXISTS blocks_number_idx ON public.blocks USING btree (number);
CREATE UNIQUE INDEX IF NOT EXISTS blocks_number_mined_at_idx ON public.blocks USING btree (number, mined_at);
