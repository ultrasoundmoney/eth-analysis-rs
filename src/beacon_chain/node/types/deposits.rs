// src/beacon_chain/node/types/deposits.rs
use crate::json_codecs::i32_from_string;
use crate::units::GweiNewtype;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct DepositData {
    pub amount: GweiNewtype,
}

#[derive(Debug, Deserialize)]
pub struct Deposit {
    pub data: DepositData,
}

#[derive(Debug, Deserialize)]
pub struct ExecutionDeposit {
    pub pubkey: String,
    pub withdrawal_credentials: String,
    pub amount: GweiNewtype,
    pub signature: String,
    #[serde(deserialize_with = "i32_from_string")]
    pub index: i32,
}

#[derive(Deserialize)]
pub struct PendingDeposit {
    pub amount: GweiNewtype,
}

#[derive(Deserialize)]
pub struct PendingDepositsEnvelope {
    pub data: Vec<PendingDeposit>,
}

#[cfg(test)]
mod tests {
    use crate::beacon_chain::BeaconBlockBuilder;
    use crate::units::GweiNewtype;

    #[test]
    fn get_deposit_sum_from_block_no_deposits_test() {
        let block = BeaconBlockBuilder::default().build();
        assert_eq!(block.total_deposits_amount(), GweiNewtype(0));
    }

    #[test]
    fn get_deposit_sum_from_block_only_consensus_deposits_test() {
        let block = BeaconBlockBuilder::default()
            .deposits(vec![GweiNewtype(10), GweiNewtype(20)])
            .build();
        assert_eq!(block.total_deposits_amount(), GweiNewtype(30));
    }

    #[test]
    fn get_deposit_sum_from_block_only_execution_request_deposits_test() {
        let block = BeaconBlockBuilder::default()
            .execution_request_deposits(vec![GweiNewtype(5), GweiNewtype(15)])
            .build();
        assert_eq!(block.total_deposits_amount(), GweiNewtype(20));
    }

    #[test]
    fn get_deposit_sum_from_block_both_deposit_types_test() {
        let block = BeaconBlockBuilder::default()
            .deposits(vec![GweiNewtype(10), GweiNewtype(20)]) // 30
            .execution_request_deposits(vec![GweiNewtype(5), GweiNewtype(15)]) // 20
            .build();
        assert_eq!(block.total_deposits_amount(), GweiNewtype(50));
    }

    #[test]
    fn get_deposit_sum_from_block_empty_consensus_deposits_with_execution_test() {
        let block = BeaconBlockBuilder::default()
            .deposits(vec![])
            .execution_request_deposits(vec![GweiNewtype(5), GweiNewtype(15)])
            .build();
        assert_eq!(block.total_deposits_amount(), GweiNewtype(20));
    }

    #[test]
    fn get_deposit_sum_from_block_empty_execution_deposits_with_consensus_test() {
        let block = BeaconBlockBuilder::default()
            .deposits(vec![GweiNewtype(10), GweiNewtype(20)])
            .execution_request_deposits(vec![])
            .build();
        assert_eq!(block.total_deposits_amount(), GweiNewtype(30));
    }
}
