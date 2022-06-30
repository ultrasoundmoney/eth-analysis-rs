use eth_analysis::execution_node::*;

#[tokio::test]
async fn test_get_latest_block() {
    let mut node = ExecutionNode::connect().await;
    let block = node.get_latest_block().await;
    node.close().await;
}

#[tokio::test]
async fn test_get_block() {
    let mut node = ExecutionNode::connect().await;
    let block = node.get_block_by_number(&0).await;
    assert_eq!(0, block.number);
    node.close().await;
}
