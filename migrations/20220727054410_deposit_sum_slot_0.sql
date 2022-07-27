UPDATE beacon_blocks
SET deposit_sum = 674144000000000
WHERE state_root = '0x7e76880eb67bbdc86250aa578958e9d0675e64e714337855204fb5abaaf82c2b';

UPDATE beacon_blocks
SET deposit_sum_aggregated = deposit_sum_aggregated + 674144000000000;
