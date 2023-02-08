ALTER TABLE burn_sums
RENAME COLUMN sum_wei TO sum;

ALTER TABLE burn_sums
DROP COLUMN sum_usd;
