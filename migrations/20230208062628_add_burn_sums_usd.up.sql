TRUNCATE TABLE burn_sums;

ALTER TABLE burn_sums
RENAME COLUMN sum TO sum_wei;

ALTER TABLE burn_sums
ADD COLUMN sum_usd float8 NOT NULL;
