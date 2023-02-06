use crate::units::GweiNewtype;

const APPROXIMATE_GAS_USED_PER_BLOCK: u32 = 15_000_000u32;
const APPROXIMATE_NUMBER_OF_BLOCKS_PER_WEEK: i32 = 50400;

pub fn barrier_from_issuance(issuance: GweiNewtype) -> f64 {
    issuance.0 as f64
        / APPROXIMATE_NUMBER_OF_BLOCKS_PER_WEEK as f64
        / APPROXIMATE_GAS_USED_PER_BLOCK as f64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn barrier_from_issuance_test() {
        let issuance = GweiNewtype(11241590000000);
        let barrier = barrier_from_issuance(issuance);
        assert_eq!(barrier, 14.869828042328042);
    }
}
