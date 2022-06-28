ALTER TABLE key_value_store
  ALTER COLUMN value
  SET DATA TYPE jsonb
  USING value::jsonb;
