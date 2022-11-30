ALTER TABLE
  eth_supply ADD CONSTRAINT eth_supply_block_number_fkey FOREIGN KEY (block_number) REFERENCES blocks_next (number)
