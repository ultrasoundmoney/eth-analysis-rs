// src/beacon_chain/node/types/balances.rs
use crate::units::GweiNewtype;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct ValidatorBalance {
    pub balance: GweiNewtype,
}

#[derive(Deserialize)]
pub struct ValidatorBalancesEnvelope {
    pub data: Vec<ValidatorBalance>,
}

#[derive(Debug, Deserialize)]
pub struct Validator {
    pub effective_balance: GweiNewtype,
}

#[derive(Debug, Deserialize)]
pub struct ValidatorEnvelope {
    pub status: String,
    pub validator: Validator,
}

impl ValidatorEnvelope {
    pub fn is_active(&self) -> bool {
        self.status == "active_ongoing"
            || self.status == "active_exiting"
            || self.status == "active_slashed"
    }

    pub fn effective_balance(&self) -> GweiNewtype {
        self.validator.effective_balance
    }
}

#[derive(Deserialize)]
pub struct ValidatorsEnvelope {
    pub data: Vec<ValidatorEnvelope>,
}
