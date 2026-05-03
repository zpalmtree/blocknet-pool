const INITIAL_REWARD: u64 = 72_325_093_035;
const TAIL_EMISSION: u64 = 200_000_000;
const MONTHS_TO_TAIL: u64 = 48;
const DECAY_RATE: f64 = 0.75;
const BLOCK_INTERVAL_SECS: u64 = 5 * 60;
const BLOCKS_PER_MONTH: u64 = (30 * 24 * 60 * 60) / BLOCK_INTERVAL_SECS;

pub fn estimated_block_reward(height: u64) -> u64 {
    let month = height / BLOCKS_PER_MONTH.max(1);
    if month >= MONTHS_TO_TAIL {
        return TAIL_EMISSION;
    }
    let years = month as f64 / 12.0;
    let decay = (-DECAY_RATE * years).exp();
    let reward =
        (INITIAL_REWARD.saturating_sub(TAIL_EMISSION)) as f64 * decay + TAIL_EMISSION as f64;
    reward.max(TAIL_EMISSION as f64) as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn estimated_block_reward_matches_observed_rewards() {
        assert_eq!(estimated_block_reward(0), INITIAL_REWARD);
        assert_eq!(estimated_block_reward(20_134), 63_850_171_202);
    }

    #[test]
    fn estimated_block_reward_has_tail_floor() {
        assert_eq!(
            estimated_block_reward(BLOCKS_PER_MONTH.saturating_mul(MONTHS_TO_TAIL)),
            TAIL_EMISSION
        );
        assert_eq!(
            estimated_block_reward(BLOCKS_PER_MONTH.saturating_mul(5_000)),
            TAIL_EMISSION
        );
    }
}
