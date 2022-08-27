-- For performance reasons, rather than updating 4M rows, we create a tmp table and recreate indexes.
-- Make sure no writes happen to beacon_blocks and not to beacon_blocks_tmp while we update.
LOCK TABLE beacon_blocks IN EXCLUSIVE MODE;

-- Set deposit sum for slot 0.
UPDATE beacon_blocks
SET deposit_sum = 674144000000000
WHERE state_root = '0x7e76880eb67bbdc86250aa578958e9d0675e64e714337855204fb5abaaf82c2b';

-- Set genesis parent root for slot 0.
UPDATE beacon_blocks
SET parent_root = '0x0000000000000000000000000000000000000000000000000000000000000000'
WHERE state_root = '0x7e76880eb67bbdc86250aa578958e9d0675e64e714337855204fb5abaaf82c2b';

CREATE TABLE beacon_blocks_tmp AS (
  SELECT
    block_root,
    state_root,
    parent_root,
    deposit_sum,
    (deposit_sum_aggregated + 674144000000000) AS deposit_sum_aggregated
  FROM
    beacon_blocks
);

-- Swap the tables around.
DROP TABLE beacon_blocks;
ALTER TABLE beacon_blocks_tmp RENAME TO beacon_blocks;

-- Recreate the indices.
ALTER TABLE public.beacon_blocks ADD CONSTRAINT beacon_blocks_pk PRIMARY KEY (block_root);
ALTER TABLE public.beacon_blocks ADD CONSTRAINT beacon_blocks_state_root_fk FOREIGN KEY (state_root) REFERENCES beacon_states(state_root);
CREATE UNIQUE INDEX beacon_blocks_state_root_idx ON public.beacon_blocks USING btree (state_root);
ALTER TABLE beacon_blocks ALTER COLUMN state_root SET NOT NULL;
ALTER TABLE beacon_blocks ALTER COLUMN parent_root SET NOT NULL;
ALTER TABLE beacon_blocks ALTER COLUMN deposit_sum SET NOT NULL;
ALTER TABLE beacon_blocks ALTER COLUMN deposit_sum_aggregated SET NOT NULL;
