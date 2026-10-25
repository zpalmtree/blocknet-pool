//! Per-source share risk tracking.
//!
//! Miner addresses are public and login does not prove ownership, so abuse
//! evidence is tracked against the network source that produced it before it
//! is ever attributed to an address:
//!
//! - a *session* is one Stratum login on one connection;
//! - a *pair* is (address, IP group) and spans that address's sessions from
//!   one host, which is what survives reconnect-cycling;
//! - an *IP group* is an IPv4 address or an IPv6 /64.
//!
//! A proven fake share (the pool's hash differs from the miner's claimed hash)
//! strikes the source: every still-unverified share the pair submitted inside
//! the revocation window is revoked, the session is cut off, and the pair is
//! banned for an escalating period. Repeated strikes from one IP group also
//! revoke and ban the group's other *unestablished* pairs (throwaway
//! addresses). Pairs with clean verified history are never punished for a
//! different source's failures, which is what keeps someone logging in as a
//! victim's public address from griefing the victim's own rigs.
//!
//! State is in-memory only. A Stratum restart forgets bans and which
//! provisional shares came from which source; address-level checks still
//! cover shares from before the restart.

use std::collections::{HashMap, HashSet, VecDeque};
use std::net::IpAddr;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use parking_lot::Mutex;

use crate::config::Config;

/// Verified-good samples a pair needs before it counts as established: exempt
/// from other sources' IP-level penalties and address-level lockouts, and
/// eligible for the priority candidate lane.
pub(crate) const SOURCE_ESTABLISHED_MIN_VERIFIED: u64 = 3;
const SOURCE_PRUNE_INTERVAL: Duration = Duration::from_secs(30);
/// Hard bound on tracked provisional shares (memory guard against floods).
const SOURCE_MAX_TRACKED_SHARES: usize = 400_000;
/// Hard bound on tracked pairs; idle pairs are evicted first.
const SOURCE_MAX_TRACKED_PAIRS: usize = 200_000;

/// Groups an IP for per-source accounting: IPv4 by full address, IPv6 by /64
/// (one end-site/host allocation). IPv4-mapped IPv6 addresses are treated as
/// IPv4.
pub(crate) fn ip_group_key(ip: IpAddr) -> String {
    match ip {
        IpAddr::V4(v4) => v4.to_string(),
        IpAddr::V6(v6) => {
            if let Some(v4) = v6.to_ipv4_mapped() {
                return v4.to_string();
            }
            let seg = v6.segments();
            format!("{:x}:{:x}:{:x}:{:x}::/64", seg[0], seg[1], seg[2], seg[3])
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct SourcePolicy {
    /// Unverified shares inside this window are revoked on a strike. Matches
    /// the PPLNS window: older shares are no longer creditable.
    pub revoke_window: Duration,
    pub ban_base: Duration,
    pub ban_max: Duration,
    pub strike_window: Duration,
    /// Strikes from one IP group (any address) before the group's
    /// unestablished pairs are revoked and banned too.
    pub ip_strike_threshold: u32,
}

impl SourcePolicy {
    pub(crate) fn from_config(cfg: &Config) -> Self {
        Self {
            revoke_window: cfg.pplns_window_duration(),
            ban_base: cfg.source_ban_duration_value(),
            ban_max: cfg.source_max_ban_duration_value(),
            strike_window: cfg.invalid_escalation_window_duration(),
            ip_strike_threshold: cfg.source_ip_strike_threshold.max(1) as u32,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SourceRef {
    pub session_id: u64,
    pub address: Arc<str>,
    pub ip_group: Arc<str>,
}

/// Provisional shares revoked by a strike, grouped by address.
#[derive(Debug, Clone, Default)]
pub(crate) struct SourceStrike {
    pub revoked: Vec<(String, Vec<i64>)>,
    pub pair_strikes: u32,
    pub pair_banned_for: Duration,
    pub ip_banned_for: Option<Duration>,
}

impl SourceStrike {
    pub(crate) fn revoked_share_count(&self) -> usize {
        self.revoked.iter().map(|(_, ids)| ids.len()).sum()
    }
}

#[derive(Debug, Clone, Copy)]
struct TrackedShare {
    share_id: i64,
    created_at: SystemTime,
}

#[derive(Debug)]
struct Ban {
    until: Instant,
    reason: String,
}

#[derive(Debug, Default)]
struct StrikeCounter {
    strikes: u32,
    window_until: Option<Instant>,
}

impl StrikeCounter {
    fn active(&self, now: Instant) -> u32 {
        if self.window_until.is_some_and(|until| until > now) {
            self.strikes
        } else {
            0
        }
    }

    fn add(&mut self, now: Instant, window: Duration) -> u32 {
        self.strikes = self.active(now).saturating_add(1);
        self.window_until = Some(now + window.max(Duration::from_secs(1)));
        self.strikes
    }
}

#[derive(Debug)]
struct PairEntry {
    address: Arc<str>,
    ip_group: Arc<str>,
    verified_ok: u64,
    strikes: StrikeCounter,
    ban: Option<Ban>,
    shares: VecDeque<TrackedShare>,
    open_sessions: u32,
    last_seen: Instant,
}

impl PairEntry {
    fn established(&self, now: Instant) -> bool {
        self.verified_ok >= SOURCE_ESTABLISHED_MIN_VERIFIED && self.strikes.active(now) == 0
    }

    fn banned(&self, now: Instant) -> Option<&Ban> {
        self.ban.as_ref().filter(|ban| ban.until > now)
    }
}

#[derive(Debug, Default)]
struct IpEntry {
    strikes: StrikeCounter,
    ban: Option<Ban>,
    pairs: HashSet<Arc<str>>,
}

#[derive(Debug)]
struct SessionEntry {
    pair_key: Arc<str>,
    revoked: Option<String>,
}

#[derive(Debug, Default)]
struct Inner {
    next_session: u64,
    sessions: HashMap<u64, SessionEntry>,
    pairs: HashMap<Arc<str>, PairEntry>,
    ips: HashMap<Arc<str>, IpEntry>,
    share_index: HashMap<i64, (Arc<str>, u64)>,
    last_prune: Option<Instant>,
}

#[derive(Debug)]
pub(crate) struct SourceRiskTracker {
    policy: SourcePolicy,
    inner: Mutex<Inner>,
}

fn pair_key(address: &str, ip_group: &str) -> String {
    format!("{address}\u{1f}{ip_group}")
}

fn ban_message(ban: &Ban, now: Instant) -> String {
    let remaining = ban.until.saturating_duration_since(now);
    let remaining = Duration::from_secs(remaining.as_secs().max(1));
    format!(
        "source banned: {} (retry in {})",
        ban.reason,
        humantime::format_duration(remaining)
    )
}

fn escalated_ban(base: Duration, max: Duration, step: u32) -> Duration {
    let factor = 1u32 << step.min(16);
    base.saturating_mul(factor).min(max.max(base))
}

impl SourceRiskTracker {
    pub(crate) fn new(policy: SourcePolicy) -> Self {
        Self {
            policy,
            inner: Mutex::new(Inner::default()),
        }
    }

    fn admission_locked(
        inner: &Inner,
        pair_key: &str,
        ip_group: &str,
        now: Instant,
    ) -> Result<(), String> {
        let pair = inner.pairs.get(pair_key);
        if let Some(ban) = pair.and_then(|pair| pair.banned(now)) {
            return Err(ban_message(ban, now));
        }
        let established = pair.is_some_and(|pair| pair.established(now));
        if !established {
            if let Some(ban) = inner
                .ips
                .get(ip_group)
                .and_then(|ip| ip.ban.as_ref())
                .filter(|ban| ban.until > now)
            {
                return Err(ban_message(ban, now));
            }
        }
        Ok(())
    }

    /// Registers a login. Fails with a client-facing message when the pair or
    /// (for unestablished pairs) the IP group is banned.
    pub(crate) fn open_session(&self, address: &str, ip_group: &str) -> Result<SourceRef, String> {
        let now = Instant::now();
        let key = pair_key(address, ip_group);
        let mut inner = self.inner.lock();
        self.prune_if_due_locked(&mut inner, now);
        Self::admission_locked(&inner, &key, ip_group, now)?;

        let ip_arc: Arc<str> = match inner.ips.get_key_value(ip_group) {
            Some((existing, _)) => Arc::clone(existing),
            None => Arc::from(ip_group),
        };
        let pair_arc: Arc<str> = match inner.pairs.get_key_value(key.as_str()) {
            Some((existing, _)) => Arc::clone(existing),
            None => Arc::from(key.as_str()),
        };
        let pair = inner
            .pairs
            .entry(Arc::clone(&pair_arc))
            .or_insert_with(|| PairEntry {
                address: Arc::from(address),
                ip_group: Arc::clone(&ip_arc),
                verified_ok: 0,
                strikes: StrikeCounter::default(),
                ban: None,
                shares: VecDeque::new(),
                open_sessions: 0,
                last_seen: now,
            });
        pair.open_sessions = pair.open_sessions.saturating_add(1);
        pair.last_seen = now;
        let address_arc = Arc::clone(&pair.address);
        inner
            .ips
            .entry(Arc::clone(&ip_arc))
            .or_default()
            .pairs
            .insert(Arc::clone(&pair_arc));

        inner.next_session = inner.next_session.wrapping_add(1).max(1);
        let session_id = inner.next_session;
        inner.sessions.insert(
            session_id,
            SessionEntry {
                pair_key: pair_arc,
                revoked: None,
            },
        );
        Ok(SourceRef {
            session_id,
            address: address_arc,
            ip_group: ip_arc,
        })
    }

    pub(crate) fn close_session(&self, session_id: u64) {
        let mut inner = self.inner.lock();
        let Some(session) = inner.sessions.remove(&session_id) else {
            return;
        };
        if let Some(pair) = inner.pairs.get_mut(&session.pair_key) {
            pair.open_sessions = pair.open_sessions.saturating_sub(1);
            pair.last_seen = Instant::now();
        }
    }

    /// Per-submit admission: a struck session stays cut off, and bans placed
    /// after login (from another session's strike) apply immediately.
    pub(crate) fn check_session(&self, source: &SourceRef) -> Result<(), String> {
        let now = Instant::now();
        let inner = self.inner.lock();
        let Some(session) = inner.sessions.get(&source.session_id) else {
            return Ok(());
        };
        if let Some(reason) = session.revoked.as_deref() {
            return Err(format!("source banned: {reason}"));
        }
        Self::admission_locked(&inner, &session.pair_key, &source.ip_group, now)
    }

    pub(crate) fn session_blocked(&self, source: &SourceRef) -> bool {
        self.check_session(source).is_err()
    }

    pub(crate) fn pair_established(&self, source: &SourceRef) -> bool {
        self.is_established(&source.address, &source.ip_group)
    }

    /// Whether (address, IP group) has clean verified history.
    pub(crate) fn is_established(&self, address: &str, ip_group: &str) -> bool {
        let now = Instant::now();
        let key = pair_key(address, ip_group);
        self.inner
            .lock()
            .pairs
            .get(key.as_str())
            .is_some_and(|pair| pair.established(now))
    }

    pub(crate) fn record_provisional(
        &self,
        source: &SourceRef,
        share_id: i64,
        created_at: SystemTime,
    ) {
        let now = Instant::now();
        let mut inner = self.inner.lock();
        let Some(pair_key) = inner
            .sessions
            .get(&source.session_id)
            .map(|session| Arc::clone(&session.pair_key))
        else {
            return;
        };
        let Some(pair) = inner.pairs.get_mut(&pair_key) else {
            return;
        };
        pair.shares.push_back(TrackedShare {
            share_id,
            created_at,
        });
        pair.last_seen = now;
        inner
            .share_index
            .insert(share_id, (pair_key, source.session_id));
        if inner.share_index.len() > SOURCE_MAX_TRACKED_SHARES {
            inner.last_prune = None;
            self.prune_if_due_locked(&mut inner, now);
        }
    }

    /// A share from this session passed full verification.
    pub(crate) fn record_verified(&self, source: &SourceRef) {
        let mut inner = self.inner.lock();
        let Some(pair_key) = inner
            .sessions
            .get(&source.session_id)
            .map(|session| Arc::clone(&session.pair_key))
        else {
            return;
        };
        if let Some(pair) = inner.pairs.get_mut(&pair_key) {
            pair.verified_ok = pair.verified_ok.saturating_add(1);
        }
    }

    /// Source of a tracked provisional share, if it is still inside the window.
    pub(crate) fn source_for_share(&self, share_id: i64) -> Option<SourceRef> {
        let inner = self.inner.lock();
        let (pair_key, session_id) = inner.share_index.get(&share_id)?;
        let pair = inner.pairs.get(pair_key)?;
        Some(SourceRef {
            session_id: *session_id,
            address: Arc::clone(&pair.address),
            ip_group: Arc::clone(&pair.ip_group),
        })
    }

    /// An audited share passed. Returns false if its source is unknown.
    pub(crate) fn record_audit_ok(&self, share_id: i64) -> bool {
        let mut inner = self.inner.lock();
        let Some((pair_key, _)) = inner.share_index.remove(&share_id) else {
            return false;
        };
        if let Some(pair) = inner.pairs.get_mut(&pair_key) {
            pair.verified_ok = pair.verified_ok.saturating_add(1);
        }
        true
    }

    fn drain_pair_shares_locked(
        inner: &mut Inner,
        pair_key: &Arc<str>,
    ) -> Option<(String, Vec<i64>)> {
        let pair = inner.pairs.get_mut(pair_key)?;
        if pair.shares.is_empty() {
            return None;
        }
        let ids = pair
            .shares
            .drain(..)
            .map(|share| share.share_id)
            .collect::<Vec<_>>();
        let address = pair.address.to_string();
        for id in &ids {
            inner.share_index.remove(id);
        }
        Some((address, ids))
    }

    /// Applies a strike for a proven fake share from `source`. Returns the
    /// provisional shares the caller must revoke in the store.
    pub(crate) fn strike(&self, source: &SourceRef, reason: &str) -> SourceStrike {
        let now = Instant::now();
        let mut inner = self.inner.lock();
        let key: Arc<str> = match inner
            .sessions
            .get(&source.session_id)
            .map(|session| Arc::clone(&session.pair_key))
        {
            Some(key) => key,
            None => {
                let key = pair_key(&source.address, &source.ip_group);
                match inner.pairs.get_key_value(key.as_str()) {
                    Some((existing, _)) => Arc::clone(existing),
                    None => {
                        // The pair aged out; recreate it so the ban sticks.
                        let arc: Arc<str> = Arc::from(key.as_str());
                        inner.pairs.insert(
                            Arc::clone(&arc),
                            PairEntry {
                                address: Arc::clone(&source.address),
                                ip_group: Arc::clone(&source.ip_group),
                                verified_ok: 0,
                                strikes: StrikeCounter::default(),
                                ban: None,
                                shares: VecDeque::new(),
                                open_sessions: 0,
                                last_seen: now,
                            },
                        );
                        inner
                            .ips
                            .entry(Arc::clone(&source.ip_group))
                            .or_default()
                            .pairs
                            .insert(Arc::clone(&arc));
                        arc
                    }
                }
            }
        };
        if let Some(session) = inner.sessions.get_mut(&source.session_id) {
            session.revoked = Some(reason.to_string());
        }

        let mut outcome = SourceStrike::default();
        if let Some(pair) = inner.pairs.get_mut(&key) {
            let strikes = pair.strikes.add(now, self.policy.strike_window);
            let ban_for = escalated_ban(self.policy.ban_base, self.policy.ban_max, strikes - 1);
            pair.ban = Some(Ban {
                until: now + ban_for,
                reason: reason.to_string(),
            });
            pair.verified_ok = 0;
            pair.last_seen = now;
            outcome.pair_strikes = strikes;
            outcome.pair_banned_for = ban_for;
        }
        if let Some(revoked) = Self::drain_pair_shares_locked(&mut inner, &key) {
            outcome.revoked.push(revoked);
        }

        let threshold = self.policy.ip_strike_threshold.max(1);
        let ip_strikes = inner
            .ips
            .entry(Arc::clone(&source.ip_group))
            .or_default()
            .strikes
            .add(now, self.policy.strike_window);
        if ip_strikes >= threshold {
            let ban_for = escalated_ban(
                self.policy.ban_base,
                self.policy.ban_max,
                ip_strikes - threshold,
            );
            let ip_entry = inner
                .ips
                .get_mut(&source.ip_group)
                .expect("ip entry inserted above");
            ip_entry.ban = Some(Ban {
                until: now + ban_for,
                reason: format!("{reason} (repeated from this network)"),
            });
            outcome.ip_banned_for = Some(ban_for);
            let siblings = ip_entry
                .pairs
                .iter()
                .filter(|sibling| **sibling != key)
                .cloned()
                .collect::<Vec<_>>();
            for sibling in siblings {
                let unestablished = inner
                    .pairs
                    .get(&sibling)
                    .is_some_and(|pair| !pair.established(now));
                if !unestablished {
                    continue;
                }
                if let Some(revoked) = Self::drain_pair_shares_locked(&mut inner, &sibling) {
                    outcome.revoked.push(revoked);
                }
            }
        }
        outcome
    }

    fn prune_if_due_locked(&self, inner: &mut Inner, now: Instant) {
        if inner
            .last_prune
            .is_some_and(|last| now.saturating_duration_since(last) < SOURCE_PRUNE_INTERVAL)
        {
            return;
        }
        inner.last_prune = Some(now);
        let wall_now = SystemTime::now();
        let mut window = self.policy.revoke_window;
        loop {
            let cutoff = wall_now
                .checked_sub(window)
                .unwrap_or(SystemTime::UNIX_EPOCH);
            let Inner {
                pairs, share_index, ..
            } = &mut *inner;
            for pair in pairs.values_mut() {
                while pair
                    .shares
                    .front()
                    .is_some_and(|share| share.created_at < cutoff)
                {
                    if let Some(share) = pair.shares.pop_front() {
                        share_index.remove(&share.share_id);
                    }
                }
            }
            if share_index.len() <= SOURCE_MAX_TRACKED_SHARES || window < Duration::from_secs(60) {
                break;
            }
            window /= 2;
        }

        let idle_after = self.policy.strike_window.max(Duration::from_secs(60));
        let mut removed = Vec::new();
        inner.pairs.retain(|key, pair| {
            let keep = pair.open_sessions > 0
                || !pair.shares.is_empty()
                || pair.banned(now).is_some()
                || pair.strikes.active(now) > 0
                || now.saturating_duration_since(pair.last_seen) < idle_after;
            if !keep {
                removed.push((Arc::clone(key), Arc::clone(&pair.ip_group)));
            }
            keep
        });
        if inner.pairs.len() > SOURCE_MAX_TRACKED_PAIRS {
            let mut idle = inner
                .pairs
                .iter()
                .filter(|(_, pair)| {
                    pair.open_sessions == 0
                        && pair.shares.is_empty()
                        && pair.banned(now).is_none()
                        && pair.strikes.active(now) == 0
                })
                .map(|(key, pair)| (Arc::clone(key), Arc::clone(&pair.ip_group), pair.last_seen))
                .collect::<Vec<_>>();
            idle.sort_by_key(|(_, _, seen)| *seen);
            let excess = inner.pairs.len() - SOURCE_MAX_TRACKED_PAIRS;
            for (key, ip, _) in idle.into_iter().take(excess) {
                inner.pairs.remove(&key);
                removed.push((key, ip));
            }
        }
        for (key, ip) in removed {
            if let Some(entry) = inner.ips.get_mut(&ip) {
                entry.pairs.remove(&key);
            }
        }
        inner.ips.retain(|_, ip| {
            !ip.pairs.is_empty()
                || ip.ban.as_ref().is_some_and(|ban| ban.until > now)
                || ip.strikes.active(now) > 0
        });
    }

    #[cfg(test)]
    pub(crate) fn tracked_shares(&self) -> usize {
        self.inner.lock().share_index.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn policy() -> SourcePolicy {
        SourcePolicy {
            revoke_window: Duration::from_secs(6 * 60 * 60),
            ban_base: Duration::from_secs(15 * 60),
            ban_max: Duration::from_secs(24 * 60 * 60),
            strike_window: Duration::from_secs(24 * 60 * 60),
            ip_strike_threshold: 2,
        }
    }

    fn establish(tracker: &SourceRiskTracker, source: &SourceRef) {
        for _ in 0..SOURCE_ESTABLISHED_MIN_VERIFIED {
            tracker.record_verified(source);
        }
    }

    #[test]
    fn ipv6_groups_by_slash_64_and_ipv4_by_address() {
        let a: IpAddr = "2001:db8:1:2:aaaa::1".parse().unwrap();
        let b: IpAddr = "2001:db8:1:2:ffff::9".parse().unwrap();
        let c: IpAddr = "2001:db8:1:3::1".parse().unwrap();
        assert_eq!(ip_group_key(a), ip_group_key(b));
        assert_ne!(ip_group_key(a), ip_group_key(c));
        assert_eq!(ip_group_key("192.0.2.7".parse().unwrap()), "192.0.2.7");
        assert_eq!(
            ip_group_key("::ffff:192.0.2.7".parse().unwrap()),
            "192.0.2.7"
        );
    }

    #[test]
    fn strike_revokes_pair_shares_across_sessions_but_not_other_sources() {
        let tracker = SourceRiskTracker::new(policy());
        let now = SystemTime::now();
        let victim_rig = tracker.open_session("victim", "203.0.113.1").unwrap();
        establish(&tracker, &victim_rig);
        let attacker_a = tracker.open_session("victim", "198.51.100.9").unwrap();
        let attacker_b = tracker.open_session("victim", "198.51.100.9").unwrap();
        tracker.record_provisional(&victim_rig, 1, now);
        tracker.record_provisional(&attacker_a, 2, now);
        tracker.record_provisional(&attacker_b, 3, now);

        let outcome = tracker.strike(&attacker_a, "invalid share proof");
        assert_eq!(outcome.revoked, vec![("victim".to_string(), vec![2, 3])]);
        assert!(tracker.check_session(&attacker_a).is_err());
        assert!(
            tracker.check_session(&attacker_b).is_err(),
            "the pair ban cuts off the attacker's other sessions"
        );
        assert!(tracker.open_session("victim", "198.51.100.9").is_err());
        // The victim's own rig on its own IP is untouched.
        assert!(tracker.check_session(&victim_rig).is_ok());
        assert!(tracker.source_for_share(1).is_some());
        assert!(tracker.open_session("victim", "203.0.113.1").is_ok());
    }

    #[test]
    fn repeated_ip_strikes_revoke_unestablished_siblings_only() {
        let tracker = SourceRiskTracker::new(policy());
        let now = SystemTime::now();
        let honest = tracker.open_session("honest", "198.51.100.9").unwrap();
        establish(&tracker, &honest);
        tracker.record_provisional(&honest, 10, now);
        let throwaway = tracker.open_session("throwaway-1", "198.51.100.9").unwrap();
        tracker.record_provisional(&throwaway, 11, now);
        let fake_1 = tracker.open_session("throwaway-2", "198.51.100.9").unwrap();
        let fake_2 = tracker.open_session("throwaway-3", "198.51.100.9").unwrap();

        let first = tracker.strike(&fake_1, "invalid share proof");
        assert!(
            first.ip_banned_for.is_none(),
            "one strike bans only the pair"
        );
        assert!(tracker.open_session("throwaway-4", "198.51.100.9").is_ok());

        let second = tracker.strike(&fake_2, "invalid share proof");
        assert!(second.ip_banned_for.is_some());
        assert_eq!(second.revoked, vec![("throwaway-1".to_string(), vec![11])]);
        assert!(tracker.open_session("throwaway-5", "198.51.100.9").is_err());
        // The established pair keeps its credit and access from that network.
        assert!(tracker.check_session(&honest).is_ok());
        assert!(tracker.source_for_share(10).is_some());
        assert!(tracker.open_session("honest", "198.51.100.9").is_ok());
    }

    #[test]
    fn bans_escalate_per_pair_and_reset_trust() {
        let tracker = SourceRiskTracker::new(policy());
        let source = tracker.open_session("addr", "192.0.2.1").unwrap();
        establish(&tracker, &source);
        assert!(tracker.pair_established(&source));
        let first = tracker.strike(&source, "invalid share proof");
        assert_eq!(first.pair_banned_for, Duration::from_secs(15 * 60));
        assert!(!tracker.pair_established(&source));
        let second = tracker.strike(&source, "invalid share proof");
        assert_eq!(second.pair_banned_for, Duration::from_secs(30 * 60));
    }

    #[test]
    fn audit_attribution_uses_share_index() {
        let tracker = SourceRiskTracker::new(policy());
        let source = tracker.open_session("addr", "192.0.2.1").unwrap();
        tracker.record_provisional(&source, 42, SystemTime::now());
        assert_eq!(tracker.source_for_share(42), Some(source.clone()));
        assert!(tracker.record_audit_ok(42));
        assert!(!tracker.record_audit_ok(42));
        assert!(tracker.source_for_share(99).is_none());
        tracker.close_session(source.session_id);
        // Closed sessions still leave the pair attributable for later audits.
        tracker.record_provisional(&source, 43, SystemTime::now());
        assert_eq!(tracker.tracked_shares(), 0);
    }

    #[test]
    fn shares_outside_revoke_window_are_pruned() {
        let mut policy = policy();
        policy.revoke_window = Duration::from_secs(60);
        let tracker = SourceRiskTracker::new(policy);
        let source = tracker.open_session("addr", "192.0.2.1").unwrap();
        tracker.record_provisional(&source, 1, SystemTime::now() - Duration::from_secs(120));
        tracker.record_provisional(&source, 2, SystemTime::now());
        tracker.inner.lock().last_prune = None;
        let mut inner = tracker.inner.lock();
        tracker.prune_if_due_locked(&mut inner, Instant::now());
        drop(inner);
        assert!(tracker.source_for_share(1).is_none());
        assert!(tracker.source_for_share(2).is_some());
    }
}
