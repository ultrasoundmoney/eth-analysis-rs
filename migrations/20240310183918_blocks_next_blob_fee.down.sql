-- Add down migration script here
ALTER TABLE blocks_next
    DROP COLUMN blob_gas_used,
    DROP COLUMN excess_blob_gas;
