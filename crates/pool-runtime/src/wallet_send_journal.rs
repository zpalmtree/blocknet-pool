use std::collections::HashMap;

use anyhow::Context;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::Engine as _;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct WalletSendIdempotencyJournal {
    #[serde(default)]
    pub entries: HashMap<String, WalletSendIdempotencyEntry>,
}

#[derive(Debug, Deserialize)]
pub struct WalletSendIdempotencyEntry {
    #[serde(default)]
    pub status: u16,
    #[serde(default)]
    pub body_base64: String,
    #[serde(default)]
    pub created_at_unix_nano: i64,
}

#[derive(Debug, Deserialize)]
pub struct WalletSendIdempotencyBody {
    #[serde(default)]
    pub txid: String,
    #[serde(default)]
    pub dry_run: bool,
    #[serde(default)]
    pub fee: u64,
    #[serde(default)]
    pub recipients: Vec<WalletSendIdempotencyRecipient>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct WalletSendIdempotencyRecipient {
    pub address: String,
    #[serde(default)]
    pub amount: u64,
}

pub fn aggregate_wallet_send_recipients(
    recipients: &[WalletSendIdempotencyRecipient],
) -> Vec<(String, u64)> {
    let mut by_address = HashMap::<String, u64>::new();
    for recipient in recipients {
        let address = recipient.address.trim();
        if address.is_empty() || recipient.amount == 0 {
            continue;
        }
        by_address
            .entry(address.to_string())
            .and_modify(|amount| *amount = amount.saturating_add(recipient.amount))
            .or_insert(recipient.amount);
    }
    let mut aggregated = by_address.into_iter().collect::<Vec<_>>();
    aggregated.sort_by(|a, b| a.0.cmp(&b.0));
    aggregated
}

pub fn decode_wallet_send_body(
    entry: &WalletSendIdempotencyEntry,
) -> anyhow::Result<Option<WalletSendIdempotencyBody>> {
    let encoded_body = entry.body_base64.trim();
    if encoded_body.is_empty() {
        return Ok(None);
    }
    let decoded_body = BASE64_STANDARD
        .decode(encoded_body)
        .context("decode wallet send body")?;
    serde_json::from_slice(&decoded_body)
        .context("parse wallet send body")
        .map(Some)
}
