CREATE TABLE IF NOT EXISTS key_value_store (
	key text NOT NULL,
	value jsonb NULL,
	CONSTRAINT key_value_store_pk PRIMARY KEY (key)
);
