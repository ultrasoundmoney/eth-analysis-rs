CREATE TABLE eth_supply (
	timestamp timestamptz PRIMARY KEY,
	block_number integer NOT NULL REFERENCES blocks_next(number),
	deposits_slot integer NOT NULL REFERENCES beacon_states(slot),
	balances_slot integer NOT NULL REFERENCES beacon_states(slot),
	supply NUMERIC NOT NULL
);
