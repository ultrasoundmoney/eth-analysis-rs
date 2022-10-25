CREATE INDEX blocks_base_fee_per_gas_idx ON blocks(base_fee_per_gas);

CREATE INDEX blocks_next_base_fee_per_gas_idx ON blocks_next(base_fee_per_gas);
