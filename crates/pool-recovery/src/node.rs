pub use pool_runtime::node::{NodeStatus, WalletAddressResponse, WalletBalance};
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct WalletOutputsSummary {
    pub chain_height: u64,
    pub synced_height: u64,
    pub total: u64,
    pub spent: u64,
    pub unspent: u64,
    pub pending: u64,
}
