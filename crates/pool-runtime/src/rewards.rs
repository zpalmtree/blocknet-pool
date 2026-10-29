use pool_common::faraday::{is_faraday, FARADAY_HEIGHT};

const INITIAL_REWARD: u64 = 72_325_093_035;
const TAIL_EMISSION: u64 = 200_000_000;
const MONTHS_TO_TAIL: u64 = 48;
const DECAY_RATE: f64 = 0.75;
const BLOCK_INTERVAL_SECS: u64 = 5 * 60;
const BLOCKS_PER_MONTH: u64 = (30 * 24 * 60 * 60) / BLOCK_INTERVAL_SECS;
/// Months keep their length across the fork: half as many 10-minute blocks.
const FARADAY_BLOCKS_PER_MONTH: u64 = BLOCKS_PER_MONTH / 2;

/// Display estimate only; credited rewards always come from the daemon. From
/// the fork a block pays twice the v1 reward of its month (core spec §6).
pub fn estimated_block_reward(height: u64) -> u64 {
    if is_faraday(height) {
        let month = FARADAY_HEIGHT / BLOCKS_PER_MONTH
            + (height - FARADAY_HEIGHT) / FARADAY_BLOCKS_PER_MONTH;
        return v1_reward_for_month(month).saturating_mul(2);
    }
    v1_reward_for_month(height / BLOCKS_PER_MONTH.max(1))
}

fn v1_reward_for_month(month: u64) -> u64 {
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
        assert_eq!(v1_reward_for_month(MONTHS_TO_TAIL), TAIL_EMISSION);
        assert_eq!(v1_reward_for_month(5_000), TAIL_EMISSION);
        // The tail is 4.0 BNT from the fork.
        assert_eq!(estimated_block_reward(u64::MAX), 2 * TAIL_EMISSION);
    }

    #[test]
    fn faraday_doubles_the_reward_and_halves_blocks_per_month() {
        assert_eq!(FARADAY_HEIGHT % BLOCKS_PER_MONTH, 0);
        let last_v1 = estimated_block_reward(FARADAY_HEIGHT - 1);
        assert_eq!(last_v1, v1_reward_for_month(11));
        assert_eq!(
            estimated_block_reward(FARADAY_HEIGHT),
            2 * v1_reward_for_month(12)
        );
        // About 685 BNT at the fork (plan §8).
        assert_eq!(estimated_block_reward(FARADAY_HEIGHT) / 100_000_000, 685);
        let next_month = FARADAY_HEIGHT + FARADAY_BLOCKS_PER_MONTH;
        assert_eq!(
            estimated_block_reward(next_month - 1),
            2 * v1_reward_for_month(12)
        );
        assert_eq!(
            estimated_block_reward(next_month),
            2 * v1_reward_for_month(13)
        );
    }
}
