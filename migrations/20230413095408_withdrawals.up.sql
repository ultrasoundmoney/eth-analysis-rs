DELETE FROM beacon_issuance
WHERE
    state_root IN (SELECT state_root FROM beacon_states WHERE slot >= 6209536);
DELETE FROM beacon_validators_balance
WHERE
    state_root IN (SELECT state_root FROM beacon_states WHERE slot >= 6209536);
DELETE FROM eth_supply
WHERE deposits_slot >= 6209536 OR balances_slot >= 6209536;
DELETE FROM beacon_blocks
WHERE
    state_root IN (SELECT state_root FROM beacon_states WHERE slot >= 6209536);
DELETE FROM beacon_states WHERE slot >= 6209536;
ALTER TABLE beacon_blocks ADD COLUMN withdrawal_sum bigint;
ALTER TABLE beacon_blocks ADD COLUMN withdrawal_sum_aggregated bigint;
