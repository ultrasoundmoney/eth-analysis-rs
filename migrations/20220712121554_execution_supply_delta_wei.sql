TRUNCATE execution_supply_deltas CASCADE;

ALTER TABLE execution_supply_deltas ALTER COLUMN fee_burn TYPE NUMERIC;
ALTER TABLE execution_supply_deltas ALTER COLUMN fixed_reward TYPE NUMERIC;
ALTER TABLE execution_supply_deltas ALTER COLUMN supply_delta TYPE NUMERIC;
ALTER TABLE execution_supply_deltas ALTER COLUMN self_destruct TYPE NUMERIC;
ALTER TABLE execution_supply_deltas ALTER COLUMN uncles_reward TYPE NUMERIC;
