CREATE TABLE
  burn_sums (
    time_frame text NOT NULL,
    block_number integer NOT NULL,
    block_hash text NOT NULL,
    timestamp timestamptz NOT NULL,
    sum numeric NOT NULL,
    CONSTRAINT burn_sums_pkey PRIMARY KEY (time_frame, block_number),
    CONSTRAINT burn_sums_block_number_fkey FOREIGN KEY (block_number) REFERENCES blocks_next (number),
    CONSTRAINT burn_sums_block_hash_fkey FOREIGN KEY (block_hash) REFERENCES blocks_next (hash),
    CONSTRAINT burn_sums_time_frame_hash UNIQUE (time_frame, block_hash)
  );

CREATE INDEX burn_sums_block_number_idx ON burn_sums (block_number);
