CREATE TABLE
  burn_sums (
    time_frame text NOT NULL,
    first_included_block_number integer NOT NULL,
    last_included_block_number integer NOT NULL,
    last_included_block_hash text NOT NULL,
    timestamp timestamptz NOT NULL,
    sum numeric NOT NULL,
    CONSTRAINT burn_sums_pkey PRIMARY KEY (time_frame, last_included_block_number),
    CONSTRAINT burn_sums_first_included_block_number_fkey FOREIGN KEY (first_included_block_number) REFERENCES blocks_next (number),
    CONSTRAINT burn_sums_last_included_block_number_fkey FOREIGN KEY (last_included_block_number) REFERENCES blocks_next (number),
    CONSTRAINT burn_sums_last_included_block_hash_fkey FOREIGN KEY (last_included_block_hash) REFERENCES blocks_next (hash),
    CONSTRAINT burn_sums_time_frame_hash UNIQUE (time_frame, last_included_block_hash)
  );

CREATE INDEX burn_sums_last_included_block_number_idx ON burn_sums (last_included_block_number);
