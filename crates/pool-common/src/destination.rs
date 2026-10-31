//! Payout destinations and their stable accounting keys. Existing address-keyed
//! tables retain ordinary addresses; memo destinations use an opaque account key.
use serde::{Deserialize, Serialize};
use sha3::{Digest, Sha3_256};

pub const CAP_PAYOUT_MEMO: &str = "payout_memo_v1";
pub const MEMO_ACCOUNT_PREFIX: &str = "memo:";

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PayoutDestination {
    pub address: String,
    pub memo: Option<String>,
}

// Never include plaintext memos in diagnostic output.
impl std::fmt::Debug for PayoutDestination {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PayoutDestination")
            .field("account", &self.account_id())
            .finish()
    }
}

impl PayoutDestination {
    pub fn new(address: &str, memo: Option<&str>) -> Result<Self, String> {
        let memo = memo.filter(|m| !m.is_empty());
        if memo.is_some_and(|m| m.len() > 124 || m.chars().any(char::is_control)) {
            return Err(
                "memo must contain at most 124 UTF-8 bytes and no control characters".into(),
            );
        }
        Ok(Self {
            address: address.trim().into(),
            memo: memo.map(str::to_owned),
        })
    }

    pub fn account_id(&self) -> String {
        match &self.memo {
            None => self.address.clone(),
            Some(memo) => {
                let mut hash = Sha3_256::new();
                hash.update(b"blocknet_pool_payout_memo_v1\0");
                hash.update((self.address.len() as u64).to_be_bytes());
                hash.update(self.address.as_bytes());
                hash.update(memo.as_bytes());
                format!("{MEMO_ACCOUNT_PREFIX}{}", hex::encode(hash.finalize()))
            }
        }
    }

    pub fn from_wallet(address: &str, memo_hex: &str) -> Result<Self, String> {
        let bytes = hex::decode(memo_hex).map_err(|_| "invalid wallet memo encoding")?;
        let memo = String::from_utf8(bytes).map_err(|_| "invalid wallet memo text")?;
        Self::new(address, Some(&memo))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SharedExchangeAddress {
    pub address: String,
    pub name: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn miner_pool_account_contract() {
        assert_eq!(
            PayoutDestination::new("address", Some("ID001"))
                .unwrap()
                .account_id(),
            "memo:f7b26087161089e8313c0f4bc60341a65cc00f7bacdac6d4e5db48bb80ae2ee6"
        );
    }
    #[test]
    fn identity_preserves_ordinary_accounts_and_exact_memo() {
        let plain = PayoutDestination::new("address", None).unwrap();
        assert_eq!(plain.account_id(), "address");
        assert_eq!(plain, PayoutDestination::new("address", Some("")).unwrap());
        let ids = ["001", "1", "A", "a", " a "].map(|memo| {
            PayoutDestination::new("address", Some(memo))
                .unwrap()
                .account_id()
        });
        assert_eq!(
            ids.iter().collect::<std::collections::HashSet<_>>().len(),
            ids.len()
        );
        assert_ne!(ids[0], plain.account_id());
        assert!(PayoutDestination::new("address", Some(&"é".repeat(63))).is_err());
        assert!(PayoutDestination::new("address", Some("line\nbreak")).is_err());
    }
}
