//! Faraday fork parameters the pool depends on (blocknet-pqc core integration
//! spec §1 and §6, decided 2026-10-26 to 2026-10-28). Mainnet only: the testnet
//! fork height is not published yet.

/// First block mined with BLAKE2b-512, 10-minute blocks and the doubled reward.
pub const FARADAY_HEIGHT: u64 = 103_680;
/// Coinbase maturity and `MaxReorgDepth` from the fork, enforced by consensus.
pub const FARADAY_COINBASE_MATURITY: u64 = 60;
/// Header v2 without the nonce: the v1 fields plus a 64-byte output-tree root.
pub const FARADAY_HEADER_BASE_LEN: usize = 156;
/// BLAKE2b personalization, `blocknet/pow/v1` zero-padded to 16 bytes.
pub const BLAKE2B_POW_PERSONAL_MAINNET: [u8; 16] = *b"blocknet/pow/v1\0";

/// Share difficulty bounds from the fork. BLAKE2b runs at about 8 MH/s per CPU
/// core and 15 GH/s on an RTX 5090, so Argon2-era difficulties (tens to about
/// 10^5) would flood the pool with shares. The start gives a 16-core CPU about
/// one share per 30 s and a 5090 a few per second until vardiff settles; the
/// floor sits far above any Argon2-era difficulty, so an older value is
/// recognised and replaced by the start instead of clamped to the floor. The
/// ceiling stays below 2^53 so API consumers keep exact values.
pub const FARADAY_MIN_SHARE_DIFFICULTY: u64 = 1 << 24;
pub const FARADAY_INITIAL_SHARE_DIFFICULTY: u64 = 1 << 32;
pub const FARADAY_MAX_SHARE_DIFFICULTY: u64 = 1 << 50;

const LEGACY_BLOCK_SECONDS: u64 = 300;
const FARADAY_BLOCK_SECONDS: u64 = 600;

/// Every rule is picked by block height, never by wall-clock time.
pub fn is_faraday(height: u64) -> bool {
    height >= FARADAY_HEIGHT
}

pub fn target_block_seconds(height: u64) -> u64 {
    if is_faraday(height) {
        FARADAY_BLOCK_SECONDS
    } else {
        LEGACY_BLOCK_SECONDS
    }
}

/// Confirmations before a found block's reward is paid out. From the fork,
/// consensus makes a coinbase unspendable for 60 blocks whatever the config says.
pub fn required_coinbase_depth(block_height: u64, configured: u64) -> u64 {
    if is_faraday(block_height) {
        configured.max(FARADAY_COINBASE_MATURITY)
    } else {
        configured
    }
}

/// Earliest height whose work may be mixed with work at `height` in a rate
/// estimate: difficulty resets at the fork, so samples never cross it.
pub fn same_pow_era_floor(height: u64) -> u64 {
    if is_faraday(height) {
        FARADAY_HEIGHT
    } else {
        0
    }
}

/// Height field of a serialized header (`version u32 LE || height u64 LE || ...`).
pub fn header_height(header_base: &[u8]) -> Option<u64> {
    let bytes: [u8; 8] = header_base.get(4..12)?.try_into().ok()?;
    Some(u64::from_le_bytes(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rules_switch_exactly_at_the_fork_height() {
        assert!(!is_faraday(FARADAY_HEIGHT - 1));
        assert!(is_faraday(FARADAY_HEIGHT));
        assert!(is_faraday(u64::MAX));
        assert_eq!(target_block_seconds(FARADAY_HEIGHT - 1), 300);
        assert_eq!(target_block_seconds(FARADAY_HEIGHT), 600);
        assert_eq!(same_pow_era_floor(FARADAY_HEIGHT - 1), 0);
        assert_eq!(same_pow_era_floor(FARADAY_HEIGHT + 5), FARADAY_HEIGHT);
    }

    #[test]
    fn coinbase_depth_never_drops_below_consensus_maturity_after_the_fork() {
        assert_eq!(required_coinbase_depth(FARADAY_HEIGHT - 1, 20), 20);
        assert_eq!(required_coinbase_depth(FARADAY_HEIGHT, 20), 60);
        assert_eq!(required_coinbase_depth(FARADAY_HEIGHT, 120), 120);
    }

    #[test]
    fn header_height_reads_the_little_endian_height_field() {
        let mut header = vec![0u8; FARADAY_HEADER_BASE_LEN];
        header[..4].copy_from_slice(&2u32.to_le_bytes());
        header[4..12].copy_from_slice(&FARADAY_HEIGHT.to_le_bytes());
        assert_eq!(header_height(&header), Some(FARADAY_HEIGHT));
        assert_eq!(header_height(&header[..11]), None);
    }
}
