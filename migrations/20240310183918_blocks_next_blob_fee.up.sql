-- Add up migration script here
ALTER TABLE blocks_next
    ADD COLUMN blob_base_fee INT8,
    ADD COLUMN blob_gas_used INT4,
    ADD COLUMN excess_blob_gas INT4;
