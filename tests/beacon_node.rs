use eth_analysis::beacon_chain::node;

const SLOT_1229: u32 = 1229;
const BLOCK_ROOT_1229: &str = "0x35376f52006e12b7e9247b457277fb34f6bd32d83a651e24c2669467607e0778";
const STATE_ROOT_1229: &str = "0x36cb7e3d4585fb90a4ed17a0139de34a08b8354d1a7a054dbe3e4d8a0b93e625";

#[tokio::test]
async fn test_last_finalized_block() {
    let client = reqwest::Client::new();
    node::get_last_finalized_block(&client).await.unwrap();
}

#[tokio::test]
async fn test_block_by_root() {
    let client = reqwest::Client::new();
    node::get_block_by_root(&client, BLOCK_ROOT_1229)
        .await
        .unwrap();
}

#[tokio::test]
async fn test_header_by_slot() {
    let client = reqwest::Client::new();
    node::get_header_by_slot(&client, &SLOT_1229).await.unwrap();
}

#[tokio::test]
async fn test_state_root_by_slot() {
    let client = reqwest::Client::new();
    node::get_state_root_by_slot(&client, &SLOT_1229)
        .await
        .unwrap();
}

#[tokio::test]
async fn test_validator_balances() {
    let client = reqwest::Client::new();
    node::get_validator_balances(&client, STATE_ROOT_1229)
        .await
        .unwrap();
}
