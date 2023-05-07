ALTER TABLE beacon_blocks DROP COLUMN withdrawal_sum;
ALTER TABLE beacon_blocks DROP COLUMN withdrawal_sum_aggregated;

DROP INDEX IF EXISTS idx_beacon_blocks_state_root;
DROP INDEX IF EXISTS idx_beacon_issuance_state_root;
DROP INDEX IF EXISTS idx_beacon_validators_balance_state_root;
DROP INDEX IF EXISTS eth_supply_deposits_slot;
DROP INDEX IF EXISTS eth_supply_balances_slot;
