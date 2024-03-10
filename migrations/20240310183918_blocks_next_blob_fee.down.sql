-- Add down migration script here
ALTER TABLE blocks_next
    DROP COLUMN blob_gas_used,
    DROP COLUMN blob_base_fee,
    DROP COLUMN blob_fee_sum,
    DROP COLUMN excess_blob_gas;
