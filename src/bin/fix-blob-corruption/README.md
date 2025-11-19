# Blob Data Corruption Fix Tool

## Possible Cause

During the Cloudflare outage, some corrupted blocks might have been stored in the database. When the corrupted blob fees are multiplied by `blob_gas_used` in burn calculations, it could result in impossible burn amounts (e.g., 1.35 million ETH in 7 days instead of the real ~3,000 ETH).

## What You Need

1. **Just the Rust binary** - No external dependencies
2. **DATABASE_URL environment variable** - To connect to your PostgreSQL database

### Step 1: Diagnose

Run the diagnostic tool to see the extent of corruption:

```bash
cargo run --bin fix-blob-corruption -- --mode=diagnose
```

Review the output carefully to understand:
- How many blocks are affected
- The time range of corruption (likely during/after Cloudflare outage)
- Total bad burn amount

### Step 2: Fix

Delete corrupted blocks:

```bash
cargo run --bin fix-blob-corruption -- --mode=fix --confirm
```

This will:
- Delete all blocks with `blob_base_fee > 10,000 gwei` or `excess_blob_gas > 100M`
- Cascade delete related `burn_sums` records (due to foreign key constraints)
- Show deletion summary

**Important**: This is a destructive operation. The `--confirm` flag is required to prevent accidental deletion.

### Step 3: Restart Application

After deletion, restart your eth-analysis-rs application:

```bash
# However you normally run it, e.g.:
systemctl restart eth-analysis
# or
docker restart eth-analysis
# or
./your-start-script.sh
```

The application will:
1. Call `rollback_to_common_ancestor()`
2. Find the last valid block in the database (e.g., 20,999,999)
3. Start syncing from the next block (21,000,000)
4. Fetch correct block data from the execution node
5. Calculate correct `blob_base_fee` from correct `excess_blob_gas`
6. Recalculate `burn_sums` automatically

### Step 4: Verify

After re-sync completes (may take a few minutes depending on how many blocks), verify the fix:

```bash
cargo run --bin fix-blob-corruption -- --mode=diagnose
```

Should show:
```
✅ No corrupted blocks found!
```

Also check burn sums are now reasonable (typically 2,000-5,000 ETH for 7 days, not millions).

## Example Output

### If you have corruption:
```
=== BLOB DATA CORRUPTION DIAGNOSTIC ===

❌ Found 1,234 corrupted blocks

Block range:
  First: 21000000 at 2024-11-15 10:30:00
  Last:  21050000 at 2024-11-16 08:15:00

Total bogus blob burn: 1,350,570.68 ETH

Top 10 most corrupted blocks:
Block      Timestamp                 Blob Fee (gwei) Excess Gas      Burn (ETH)
------------------------------------------------------------------------------------------
21023456   2024-11-15 14:22:33       45234.56        999999999       35623.45
...

Current 7-day burn sum: 1350570.68 ETH ($4051712.04)

To fix this corruption, run:
  cargo run --bin fix-blob-corruption -- --mode=fix --confirm
```

### If you have no corruption:
```
=== BLOB DATA CORRUPTION DIAGNOSTIC ===

✅ No corrupted blocks found!
   All blob_base_fee values are < 10000 gwei
   All excess_blob_gas values are < 100000000
```

