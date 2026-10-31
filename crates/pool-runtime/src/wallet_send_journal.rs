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
    #[serde(default)]
    pub memo_hex: String,
    pub address: String,
    #[serde(default)]
    pub amount: u64,
}

pub fn aggregate_wallet_send_recipients(
    recipients: &[WalletSendIdempotencyRecipient],
) -> anyhow::Result<Vec<(String, u64)>> {
    let mut by_address = HashMap::<String, u64>::new();
    for recipient in recipients {
        let address = recipient.address.trim();
        if address.is_empty() || recipient.amount == 0 {
            continue;
        }
        let destination =
            pool_common::destination::PayoutDestination::from_wallet(address, &recipient.memo_hex)
                .map_err(anyhow::Error::msg)?;
        by_address
            .entry(destination.account_id())
            .and_modify(|amount| *amount = amount.saturating_add(recipient.amount))
            .or_insert(recipient.amount);
    }
    let mut aggregated = by_address.into_iter().collect::<Vec<_>>();
    aggregated.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(aggregated)
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

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn payout_memo_journal_never_merges_different_customers() {
        let recipients =
            ["", "4944303031", "4944303032"].map(|memo_hex| WalletSendIdempotencyRecipient {
                address: "shared-address".into(),
                memo_hex: memo_hex.into(),
                amount: 100,
            });
        let totals = aggregate_wallet_send_recipients(&recipients).unwrap();
        assert_eq!(totals.len(), 3);
        assert!(totals.iter().all(|(_, amount)| *amount == 100));
    }
}
