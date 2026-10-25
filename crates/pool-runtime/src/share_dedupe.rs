//! Template-scoped share de-duplication.
//!
//! A share is the pair (template header, nonce): the PoW hash depends only on
//! those two values, so the same pair must be credited at most once no matter
//! which assignment (`job_id`) it is submitted under. Assignment ids are not a
//! safe dedupe key because a vardiff rebind with `same_template_rebind_v1`
//! issues a new assignment id for the same header and the same nonce window.
//!
//! Nonce windows are unique per header across connections, so honest miners
//! never collide on (header, nonce); only resubmission of the same work does.

use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use sha2::{Digest, Sha256};

/// How long a header's nonce set is kept after its last use once the template
/// is no longer resolvable. Covers submits that resolved their binding just
/// before the template was retired and are still queued.
const LEDGER_RETIRED_TEMPLATE_GRACE: Duration = Duration::from_secs(10 * 60);
/// Absolute bound for repositories that cannot report template liveness.
const LEDGER_MAX_IDLE: Duration = Duration::from_secs(6 * 60 * 60);
const LEDGER_PRUNE_INTERVAL: Duration = Duration::from_secs(30);

/// Persisted claim retention for template-scoped share claims. Templates are
/// in-memory only, so a claim only needs to outlive every way a template can
/// still accept submits; the in-memory ledger is authoritative for the life of
/// the process and this is the durable backstop.
pub(crate) const TEMPLATE_SHARE_CLAIM_RETENTION: Duration = Duration::from_secs(6 * 60 * 60);

/// Stable dedupe key for a template header. Prefixed so it can share the
/// `seen_shares` claim table with legacy assignment-scoped claims without
/// colliding with 16-hex-char assignment ids.
pub(crate) fn template_share_key(header_base: &[u8]) -> String {
    let digest = Sha256::digest(header_base);
    format!("tpl:{}", hex::encode(&digest[..16]))
}

#[derive(Debug)]
struct LedgerEntry {
    template_job_ids: Vec<String>,
    nonces: HashSet<u64>,
    last_used: Instant,
}

#[derive(Debug, Default)]
struct LedgerInner {
    by_key: HashMap<String, LedgerEntry>,
    last_prune: Option<Instant>,
}

/// In-memory set of (template header, nonce) pairs that are in flight or
/// already finalized (credited or rejected on proof). Entries are released
/// only for retryable failures so the miner can resubmit.
#[derive(Debug, Default)]
pub(crate) struct NonceLedger {
    inner: Mutex<LedgerInner>,
}

impl NonceLedger {
    /// Reserves (key, nonce). Returns false if the pair was already reserved.
    pub(crate) fn reserve(
        &self,
        key: &str,
        template_job_id: &str,
        nonce: u64,
        now: Instant,
    ) -> bool {
        let mut inner = self.inner.lock();
        let entry = inner
            .by_key
            .entry(key.to_string())
            .or_insert_with(|| LedgerEntry {
                template_job_ids: Vec::new(),
                nonces: HashSet::new(),
                last_used: now,
            });
        entry.last_used = now;
        if !entry
            .template_job_ids
            .iter()
            .any(|id| id == template_job_id)
        {
            entry.template_job_ids.push(template_job_id.to_string());
        }
        entry.nonces.insert(nonce)
    }

    pub(crate) fn release(&self, key: &str, nonce: u64) {
        let mut inner = self.inner.lock();
        if let Some(entry) = inner.by_key.get_mut(key) {
            entry.nonces.remove(&nonce);
        }
    }

    #[cfg(test)]
    pub(crate) fn contains(&self, key: &str, nonce: u64) -> bool {
        self.inner
            .lock()
            .by_key
            .get(key)
            .is_some_and(|entry| entry.nonces.contains(&nonce))
    }

    #[cfg(test)]
    pub(crate) fn tracked_templates(&self) -> usize {
        self.inner.lock().by_key.len()
    }

    /// Drops headers whose templates can no longer accept submits. The
    /// liveness callback runs without the ledger lock held.
    pub(crate) fn prune_if_due(&self, now: Instant, template_retained: impl Fn(&str) -> bool) {
        let candidates = {
            let mut inner = self.inner.lock();
            if inner
                .last_prune
                .is_some_and(|last| now.saturating_duration_since(last) < LEDGER_PRUNE_INTERVAL)
            {
                return;
            }
            inner.last_prune = Some(now);
            inner
                .by_key
                .iter()
                .filter(|(_, entry)| {
                    now.saturating_duration_since(entry.last_used) >= LEDGER_RETIRED_TEMPLATE_GRACE
                })
                .map(|(key, entry)| (key.clone(), entry.template_job_ids.clone()))
                .collect::<Vec<_>>()
        };
        if candidates.is_empty() {
            return;
        }
        let removable = candidates
            .into_iter()
            .filter(|(_, template_ids)| !template_ids.iter().any(|id| template_retained(id)))
            .map(|(key, _)| key)
            .collect::<Vec<_>>();
        let mut inner = self.inner.lock();
        for key in removable {
            if inner.by_key.get(&key).is_some_and(|entry| {
                now.saturating_duration_since(entry.last_used) >= LEDGER_RETIRED_TEMPLATE_GRACE
            }) {
                inner.by_key.remove(&key);
            }
        }
        inner
            .by_key
            .retain(|_, entry| now.saturating_duration_since(entry.last_used) < LEDGER_MAX_IDLE);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn template_key_depends_only_on_header() {
        assert_eq!(
            template_share_key(&[1, 2, 3]),
            template_share_key(&[1, 2, 3])
        );
        assert_ne!(
            template_share_key(&[1, 2, 3]),
            template_share_key(&[1, 2, 4])
        );
        assert!(template_share_key(&[1]).starts_with("tpl:"));
    }

    #[test]
    fn reserve_is_scoped_to_header_not_assignment() {
        let ledger = NonceLedger::default();
        let now = Instant::now();
        let key = template_share_key(&[9; 8]);
        assert!(ledger.reserve(&key, "tmpl-a", 5, now));
        assert!(!ledger.reserve(&key, "tmpl-a", 5, now));
        // A different template job id carrying the same header is still the same work.
        assert!(!ledger.reserve(&key, "tmpl-b", 5, now));
        ledger.release(&key, 5);
        assert!(ledger.reserve(&key, "tmpl-a", 5, now));
    }

    #[test]
    fn prune_keeps_live_templates_and_drops_retired_ones_after_grace() {
        let ledger = NonceLedger::default();
        let start = Instant::now();
        let live = template_share_key(b"live");
        let retired = template_share_key(b"retired");
        ledger.reserve(&live, "live-job", 1, start);
        ledger.reserve(&retired, "retired-job", 1, start);

        // Inside the grace window nothing is dropped even if retired.
        ledger.prune_if_due(start + Duration::from_secs(60), |id| id == "live-job");
        assert_eq!(ledger.tracked_templates(), 2);

        let later = start + LEDGER_RETIRED_TEMPLATE_GRACE + Duration::from_secs(1);
        ledger.prune_if_due(later, |id| id == "live-job");
        assert!(ledger.contains(&live, 1));
        assert!(!ledger.contains(&retired, 1));
    }
}
