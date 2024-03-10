-- Add up migration script here
ALTER TABLE blocks_next
    ADD COLUMN blob_gas_used bigint,
    ADD COLUMN blob_base_fee bigint,
    ADD COLUMN blob_fee_sum bigint,
    ADD COLUMN excess_blob_gas bigint;
