use curve25519_dalek::constants::RISTRETTO_BASEPOINT_TABLE;
use curve25519_dalek::scalar::Scalar;
use sha3::{Digest, Sha3_256};

const STEALTH_ADDRESS_CHECKSUM_TAG: &[u8] = b"blocknet_stealth_address_checksum";
const NETWORK_ID_MAINNET: &str = "blocknet_mainnet";

pub(crate) fn test_miner_address(seed: u8) -> String {
    let spend_scalar = Scalar::from_bytes_mod_order([seed.max(1); 32]);
    let view_scalar = Scalar::from_bytes_mod_order([seed.wrapping_add(1).max(1); 32]);
    let spend_pub = (&spend_scalar * RISTRETTO_BASEPOINT_TABLE).compress();
    let view_pub = (&view_scalar * RISTRETTO_BASEPOINT_TABLE).compress();

    let mut payload = [0u8; 64];
    payload[..32].copy_from_slice(spend_pub.as_bytes());
    payload[32..].copy_from_slice(view_pub.as_bytes());

    let mut encoded = payload.to_vec();
    encoded.extend_from_slice(&address_checksum(&payload)[..4]);
    bs58::encode(encoded).into_string()
}

fn address_checksum(payload: &[u8]) -> [u8; 32] {
    let mut hasher = Sha3_256::new();
    hasher.update(STEALTH_ADDRESS_CHECKSUM_TAG);
    hasher.update(NETWORK_ID_MAINNET.as_bytes());
    hasher.update(payload);
    hasher.finalize().into()
}
