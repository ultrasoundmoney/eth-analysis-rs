CREATE TABLE IF NOT EXISTS execution_supply (
	block_hash text NOT NULL,
	block_number int4 UNIQUE NOT NULL,
	total_supply int8 NOT NULL,
	CONSTRAINT execution_supply_pkey PRIMARY KEY (block_hash),
	CONSTRAINT blocks_hash_fk FOREIGN KEY (block_hash) REFERENCES public.execution_supply_deltas(block_hash),
	CONSTRAINT blocks_number_fk FOREIGN KEY (block_number) REFERENCES public.execution_supply_deltas(block_number)
);

COMMENT ON COLUMN execution_supply.total_supply IS 'the total execution chain supply at a block, in gwei';
