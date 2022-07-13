ALTER TABLE execution_supply ADD COLUMN balances_sum NUMERIC;

UPDATE execution_supply SET
	balances_sum = total_supply::NUMERIC;

ALTER TABLE execution_supply DROP COLUMN total_supply;
