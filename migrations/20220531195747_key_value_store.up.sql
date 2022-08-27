CREATE TABLE IF NOT EXISTS key_value_store (
	key text NOT NULL,
	value json NULL,
	CONSTRAINT key_value_store_pk PRIMARY KEY (key)
);
