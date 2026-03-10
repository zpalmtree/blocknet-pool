use std::time::Duration;

/// Seine's hardcoded dev-fee address. Pool-side handling is kept here so
/// intermittent dev-fee sessions can be treated safely without requiring miner
/// upgrades first.
pub const SEINE_DEV_FEE_ADDRESS: &str =
    "SEiNEceuDyfEY3GKDQAYVuK6382K2Ln2gQSv83ySAFkKhraBxhHnTf2P6PsF8CXtreqywg4T1qwBAjQo3L2VYDYWtsbb9";
pub const SEINE_DEV_FEE_REFERENCE_TARGET_PCT: f64 = 1.0;

const MIN_DEV_FEE_SUPERSEDE_GRACE: Duration = Duration::from_secs(15);
pub const DEV_VARDIFF_BOOTSTRAP_HINT_LIMIT: usize = 64;

pub fn is_seine_dev_fee_address(address: &str) -> bool {
    address.trim() == SEINE_DEV_FEE_ADDRESS
}

pub fn login_difficulty_floor(address: &str, initial_difficulty: u64) -> u64 {
    if is_seine_dev_fee_address(address) {
        initial_difficulty.max(1)
    } else {
        1
    }
}

pub fn superseded_assignment_grace(address: &str, configured: Duration) -> Duration {
    if is_seine_dev_fee_address(address) {
        configured.max(MIN_DEV_FEE_SUPERSEDE_GRACE)
    } else {
        configured
    }
}

pub fn should_bootstrap_login_from_address_hints(address: &str) -> bool {
    is_seine_dev_fee_address(address)
}

pub fn should_persist_login_hint_immediately(address: &str) -> bool {
    is_seine_dev_fee_address(address)
}

pub fn should_allow_login_difficulty_hint_raise(address: &str) -> bool {
    is_seine_dev_fee_address(address)
}

pub fn should_defer_submit_ack_difficulty(address: &str) -> bool {
    is_seine_dev_fee_address(address)
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

    #[test]
    fn dev_fee_address_enables_bootstrap_and_deferred_ack_handling() {
        assert!(should_bootstrap_login_from_address_hints(
            SEINE_DEV_FEE_ADDRESS
        ));
        assert!(should_persist_login_hint_immediately(SEINE_DEV_FEE_ADDRESS));
        assert!(should_allow_login_difficulty_hint_raise(
            SEINE_DEV_FEE_ADDRESS
        ));
        assert!(should_defer_submit_ack_difficulty(SEINE_DEV_FEE_ADDRESS));
        assert!(!should_bootstrap_login_from_address_hints("miner-address"));
        assert!(!should_persist_login_hint_immediately("miner-address"));
        assert!(!should_allow_login_difficulty_hint_raise("miner-address"));
        assert!(!should_defer_submit_ack_difficulty("miner-address"));
    }
}
