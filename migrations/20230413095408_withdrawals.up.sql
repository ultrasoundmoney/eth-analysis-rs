-- Without these indices the constraint check is too slow.
CREATE INDEX IF NOT EXISTS idx_beacon_blocks_state_root ON beacon_blocks (
    state_root
);
CREATE INDEX IF NOT EXISTS idx_beacon_issuance_state_root ON beacon_issuance (
    state_root
);
CREATE INDEX IF NOT EXISTS idx_beacon_validators_balance_state_root ON beacon_validators_balance (
    state_root
);
CREATE INDEX IF NOT EXISTS eth_supply_deposits_slot ON eth_supply (
    deposits_slot
);
CREATE INDEX IF NOT EXISTS eth_supply_balances_slot ON eth_supply (
    balances_slot
);

DELETE FROM beacon_issuance
USING beacon_states
WHERE
    beacon_issuance.state_root = beacon_states.state_root
    AND beacon_states.slot >= 6209536;

DELETE FROM beacon_validators_balance
USING beacon_states
WHERE
    beacon_validators_balance.state_root = beacon_states.state_root
    AND beacon_states.slot >= 6209536;

DELETE FROM eth_supply
WHERE
    eth_supply.deposits_slot >= 6209536 OR eth_supply.balances_slot >= 6209536;

DELETE FROM beacon_blocks
USING beacon_states
WHERE
    beacon_blocks.state_root = beacon_states.state_root
    AND beacon_states.slot >= 6209536;

DELETE FROM beacon_states WHERE slot >= 6209536;

ALTER TABLE beacon_blocks ADD COLUMN withdrawal_sum bigint;
ALTER TABLE beacon_blocks ADD COLUMN withdrawal_sum_aggregated bigint;
