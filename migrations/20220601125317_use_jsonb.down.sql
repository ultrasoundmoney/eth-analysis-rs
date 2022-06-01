ALTER TABLE key_value_store
  ALTER COLUMN value
  SET DATA TYPE json
  USING value::json;
