ALTER TABLE beacon_blocks
ADD COLUMN slot INTEGER;

CREATE INDEX IF NOT EXISTS beacon_blocks_slot_idx ON beacon_blocks (slot);