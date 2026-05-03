use serde::{Deserialize, Serialize};
use sha3::{Digest, Sha3_256};

pub const METHOD_LOGIN: &str = "login";
pub const METHOD_SUBMIT: &str = "submit";
pub const METHOD_NOTIFICATION: &str = "notification";

pub const NOTIFY_POOL_BLOCK_SOLVED: &str = "pool_block_solved";
pub const NOTIFY_MINER_BLOCK_FOUND: &str = "miner_block_found";

pub const STRATUM_PROTOCOL_VERSION_CURRENT: u32 = 2;

pub const CAP_SUBMIT_CLAIMED_HASH: &str = "submit_claimed_hash";
pub const CAP_DIFFICULTY_HINT: &str = "difficulty_hint";
pub const CAP_SAME_TEMPLATE_REBIND_V1: &str = "same_template_rebind_v1";
const STEALTH_ADDRESS_CHECKSUM_TAG: &[u8] = b"blocknet_stealth_address_checksum";
const NETWORK_ID_MAINNET: &str = "blocknet_mainnet";
const NETWORK_ID_TESTNET: &str = "blocknet_testnet";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AddressNetwork {
    Mainnet,
    Testnet,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct StratumRequest {
    pub id: u64,
    pub method: String,
    #[serde(default)]
    pub params: serde_json::Value,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct StratumResponse {
    pub id: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct StratumNotify {
    pub method: String,
    pub params: serde_json::Value,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct LoginParams {
    pub address: String,
    pub worker: String,
    #[serde(default)]
    pub protocol_version: u32,
    #[serde(default)]
    pub capabilities: Vec<String>,
    #[serde(default)]
    pub difficulty_hint: Option<u64>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct LoginResult {
    pub protocol_version: u32,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub capabilities: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub required_capabilities: Vec<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SubmitParams {
    pub job_id: String,
    pub nonce: u64,
    #[serde(default)]
    pub claimed_hash: Option<String>,
}

pub fn normalize_worker_name(worker: &str) -> String {
    let trimmed = worker.trim();
    if trimmed.is_empty() {
        "default".to_string()
    } else {
        trimmed.chars().take(64).collect()
    }
}

pub fn build_login_result() -> LoginResult {
    let capabilities = vec![
        CAP_SUBMIT_CLAIMED_HASH.to_string(),
        CAP_DIFFICULTY_HINT.to_string(),
        CAP_SAME_TEMPLATE_REBIND_V1.to_string(),
    ];

    LoginResult {
        protocol_version: STRATUM_PROTOCOL_VERSION_CURRENT,
        capabilities,
        required_capabilities: vec![CAP_SUBMIT_CLAIMED_HASH.to_string()],
    }
}

pub fn validate_miner_address(address: &str) -> Result<(), String> {
    parse_address_network(address).map(|_| ())
}

pub fn address_network(address: &str) -> Result<Option<AddressNetwork>, String> {
    parse_address_network(address)
}

pub fn validate_miner_address_for_network(
    address: &str,
    expected_network: Option<AddressNetwork>,
) -> Result<(), String> {
    if let (Some(expected), Some(actual)) = (expected_network, parse_address_network(address)?) {
        if expected != actual {
            return Err("invalid address checksum".to_string());
        }
    }
    Ok(())
}

fn parse_address_network(address: &str) -> Result<Option<AddressNetwork>, String> {
    let trimmed = address.trim();
    if trimmed.is_empty() {
        return Err("address is required".to_string());
    }

    let decoded = bs58::decode(trimmed)
        .into_vec()
        .map_err(|_| "invalid base58 address".to_string())?;

    match decoded.len() {
        68 => {
            let payload = &decoded[..64];
            let checksum = &decoded[64..];
            let network = if checksum_matches(payload, checksum, NETWORK_ID_MAINNET) {
                Some(AddressNetwork::Mainnet)
            } else if checksum_matches(payload, checksum, NETWORK_ID_TESTNET) {
                Some(AddressNetwork::Testnet)
            } else {
                None
            };
            if let Some(network) = network {
                Ok(Some(network))
            } else {
                Err("invalid address checksum".to_string())
            }
        }
        len => Err(format!(
            "invalid address length: expected 68 bytes, got {len}"
        )),
    }
}

pub fn parse_hash_hex(v: &str) -> Result<[u8; 32], String> {
    let trimmed = v.trim();
    let raw = hex::decode(trimmed).map_err(|err| match err {
        hex::FromHexError::OddLength => "hex length must be even".to_string(),
        _ => "invalid hex".to_string(),
    })?;
    if raw.len() != 32 {
        return Err(format!("expected 32-byte hash, got {} bytes", raw.len()));
    }
    let mut out = [0u8; 32];
    out.copy_from_slice(&raw);
    Ok(out)
}

fn checksum_matches(payload: &[u8], checksum: &[u8], network_id: &str) -> bool {
    if checksum.len() != 4 || payload.len() != 64 {
        return false;
    }
    let sum = address_checksum(payload, network_id);
    checksum[0] == sum[0] && checksum[1] == sum[1] && checksum[2] == sum[2] && checksum[3] == sum[3]
}

fn address_checksum(payload: &[u8], network_id: &str) -> [u8; 32] {
    let mut hasher = Sha3_256::new();
    hasher.update(STEALTH_ADDRESS_CHECKSUM_TAG);
    hasher.update(network_id.as_bytes());
    hasher.update(payload);
    hasher.finalize().into()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_miner_address(seed: u8) -> String {
        let payload = [seed; 64];
        let mut encoded = payload.to_vec();
        encoded.extend_from_slice(&address_checksum(&payload, NETWORK_ID_MAINNET)[..4]);
        bs58::encode(encoded).into_string()
    }

    #[test]
    fn worker_name_normalizes() {
        assert_eq!(normalize_worker_name("   "), "default");
        assert_eq!(normalize_worker_name(" rig-1 "), "rig-1");
        assert_eq!(normalize_worker_name(&"a".repeat(80)), "a".repeat(64));
    }

    #[test]
    fn login_result_flags_required_caps() {
        let result = build_login_result();
        assert!(result
            .capabilities
            .iter()
            .any(|c| c == CAP_SUBMIT_CLAIMED_HASH));
        assert!(result.capabilities.iter().any(|c| c == CAP_DIFFICULTY_HINT));
        assert_eq!(
            result.required_capabilities,
            vec![CAP_SUBMIT_CLAIMED_HASH.to_string()]
        );
    }

    #[test]
    fn login_params_accept_optional_difficulty_hint() {
        let raw = serde_json::json!({
            "address": "addr",
            "worker": "rig01",
            "protocol_version": 2,
            "capabilities": ["submit_claimed_hash"],
            "difficulty_hint": 321
        });
        let parsed: LoginParams = serde_json::from_value(raw).expect("parse login params");
        assert_eq!(parsed.difficulty_hint, Some(321));
    }

    #[test]
    fn hash_hex_parses() {
        let s = "ab".repeat(32);
        let parsed = parse_hash_hex(&s).expect("parse");
        assert_eq!(parsed, [0xAB; 32]);
    }

    #[test]
    fn miner_address_accepts_current_checksum_format() {
        let current_addr = test_miner_address(0x22);
        assert!(validate_miner_address(&current_addr).is_ok());
        assert_eq!(
            address_network(&current_addr).expect("mainnet network should parse"),
            Some(AddressNetwork::Mainnet)
        );

        let payload = [0x22; 64];
        let mut testnet = payload.to_vec();
        testnet.extend_from_slice(&address_checksum(&payload, NETWORK_ID_TESTNET)[..4]);
        let testnet_addr = bs58::encode(testnet).into_string();
        assert!(validate_miner_address(&testnet_addr).is_ok());
        assert_eq!(
            address_network(&testnet_addr).expect("testnet network should parse"),
            Some(AddressNetwork::Testnet)
        );
    }

    #[test]
    fn network_specific_validation_rejects_cross_network_checksum() {
        let payload = [0x55; 64];
        let mut encoded = payload.to_vec();
        encoded.extend_from_slice(&address_checksum(&payload, NETWORK_ID_TESTNET)[..4]);
        let address = bs58::encode(encoded).into_string();

        let err = validate_miner_address_for_network(&address, Some(AddressNetwork::Mainnet))
            .expect_err("cross-network address must fail");
        assert!(err.contains("checksum"));
    }

    #[test]
    fn miner_address_rejects_invalid_base58() {
        let err = validate_miner_address("bench_addr_e2e").expect_err("must reject");
        assert!(err.contains("base58"));
    }

    #[test]
    fn miner_address_rejects_invalid_length() {
        let address = bs58::encode([0x33; 16]).into_string();
        let err = validate_miner_address(&address).expect_err("must reject");
        assert!(err.contains("length"));
    }

    #[test]
    fn miner_address_rejects_bad_checksum() {
        let payload = [0x44; 64];
        let mut encoded = payload.to_vec();
        encoded.extend_from_slice(&address_checksum(&payload, NETWORK_ID_MAINNET)[..4]);
        let last = encoded
            .last_mut()
            .expect("checksummed payload should have checksum bytes");
        *last ^= 0x01;
        let address = bs58::encode(encoded).into_string();
        let err = validate_miner_address(&address).expect_err("must reject");
        assert!(err.contains("checksum"));
    }
}
