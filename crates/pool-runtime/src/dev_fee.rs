use std::time::Duration;

/// Seine's hardcoded dev-fee address. Pool-side handling is kept here so
/// intermittent dev-fee sessions can be treated safely without requiring miner
/// upgrades first.
pub(crate) const SEINE_DEV_FEE_ADDRESS: &str =
    "SEiNEceuDyfEY3GKDQAYVuK6382K2Ln2gQSv83ySAFkKhraBxhHnTf2P6PsF8CXtreqywg4T1qwBAjQo3L2VYDYWtsbb9";

const MIN_DEV_FEE_SUPERSEDE_GRACE: Duration = Duration::from_secs(15);
pub(crate) const DEV_VARDIFF_BOOTSTRAP_HINT_LIMIT: usize = 64;

pub(crate) fn is_seine_dev_fee_address(address: &str) -> bool {
    address.trim() == SEINE_DEV_FEE_ADDRESS
}

pub(crate) fn login_difficulty_floor(address: &str, initial_difficulty: u64) -> u64 {
    if is_seine_dev_fee_address(address) {
        initial_difficulty.max(1)
    } else {
        1
    }
}

pub(crate) fn superseded_assignment_grace(address: &str, configured: Duration) -> Duration {
    if is_seine_dev_fee_address(address) {
        configured.max(MIN_DEV_FEE_SUPERSEDE_GRACE)
    } else {
        configured
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dev_fee_login_floor_uses_initial_difficulty() {
        assert_eq!(login_difficulty_floor(SEINE_DEV_FEE_ADDRESS, 60), 60);
        assert_eq!(login_difficulty_floor("miner-address", 60), 1);
    }

    #[test]
    fn dev_fee_supersede_grace_has_minimum() {
        assert_eq!(
            superseded_assignment_grace(SEINE_DEV_FEE_ADDRESS, Duration::from_secs(5)),
            Duration::from_secs(15)
        );
        assert_eq!(
            superseded_assignment_grace(SEINE_DEV_FEE_ADDRESS, Duration::from_secs(20)),
            Duration::from_secs(20)
        );
        assert_eq!(
            superseded_assignment_grace("miner-address", Duration::from_secs(5)),
            Duration::from_secs(5)
        );
    }
}
