//! Request-protection primitives for the public API: client IP resolution
//! behind the reverse proxy, fixed-window counters for rate limits and failed
//! admin authentication, and a bounded response cache for expensive reads.

use std::collections::HashMap;
use std::net::IpAddr;
use std::time::{Duration, Instant};

use axum::body::Bytes;
use axum::http::{HeaderMap, HeaderName};

/// Header our nginx sets from `$remote_addr`.
pub(crate) const DEFAULT_CLIENT_IP_HEADER: &str = "x-real-ip";

/// Resolves the client address used to key rate limits.
///
/// Forwarding headers are only trusted when the TCP peer is a trusted proxy
/// (loopback, or an address listed in `api_trusted_proxies`). Order:
/// configured header (default `X-Real-IP`), then the rightmost untrusted
/// `X-Forwarded-For` hop. The leftmost XFF entry is client-controlled and is
/// never used. A trusted proxy that sends neither header yields `None` rather
/// than collapsing every user into the proxy's own address.
#[derive(Clone, Debug)]
pub(crate) struct ClientIpPolicy {
    header: Option<HeaderName>,
    trusted_proxies: Vec<IpAddr>,
}

impl Default for ClientIpPolicy {
    fn default() -> Self {
        Self {
            header: Some(HeaderName::from_static(DEFAULT_CLIENT_IP_HEADER)),
            trusted_proxies: Vec::new(),
        }
    }
}

impl ClientIpPolicy {
    /// `header` empty disables the dedicated header (XFF fallback only).
    /// Invalid entries are rejected by config validation; here they are skipped.
    pub(crate) fn new(header: &str, trusted_proxies: &[String]) -> Self {
        let header = header.trim();
        Self {
            header: if header.is_empty() {
                None
            } else {
                HeaderName::from_bytes(header.to_ascii_lowercase().as_bytes()).ok()
            },
            trusted_proxies: trusted_proxies
                .iter()
                .filter_map(|value| value.trim().parse::<IpAddr>().ok())
                .map(canonical_ip)
                .collect(),
        }
    }

    fn is_trusted_proxy(&self, ip: IpAddr) -> bool {
        ip.is_loopback() || self.trusted_proxies.contains(&ip)
    }

    pub(crate) fn resolve(&self, headers: &HeaderMap, peer: Option<IpAddr>) -> Option<IpAddr> {
        if let Some(peer) = peer.map(canonical_ip) {
            if !self.is_trusted_proxy(peer) {
                return Some(peer);
            }
        }
        if let Some(name) = &self.header {
            if let Some(ip) = headers
                .get(name)
                .and_then(|value| value.to_str().ok())
                .and_then(parse_ip)
            {
                return Some(ip);
            }
        }
        let hops = headers
            .get_all("x-forwarded-for")
            .iter()
            .filter_map(|value| value.to_str().ok())
            .flat_map(|value| value.split(','))
            .collect::<Vec<_>>();
        for hop in hops.into_iter().rev() {
            match parse_ip(hop) {
                Some(ip) if self.is_trusted_proxy(ip) => continue,
                Some(ip) => return Some(ip),
                // An unparsable hop means the chain is not ours to interpret.
                None => break,
            }
        }
        None
    }
}

fn canonical_ip(ip: IpAddr) -> IpAddr {
    match ip {
        IpAddr::V6(v6) => v6
            .to_ipv4_mapped()
            .map(IpAddr::V4)
            .unwrap_or(IpAddr::V6(v6)),
        v4 => v4,
    }
}

fn parse_ip(raw: &str) -> Option<IpAddr> {
    let raw = raw.trim();
    if raw.is_empty() {
        return None;
    }
    raw.parse::<IpAddr>()
        .ok()
        .or_else(|| {
            raw.parse::<std::net::SocketAddr>()
                .ok()
                .map(|addr| addr.ip())
        })
        .map(canonical_ip)
}

/// Rate-limit key for a client. IPv6 clients are grouped by /64 because a
/// single subscriber typically controls the whole prefix.
pub(crate) fn client_key(ip: IpAddr) -> String {
    match ip {
        IpAddr::V4(v4) => v4.to_string(),
        IpAddr::V6(v6) => {
            let s = v6.segments();
            format!("{:x}:{:x}:{:x}:{:x}::/64", s[0], s[1], s[2], s[3])
        }
    }
}

/// Fixed-window counters keyed by string. Every operation is O(1); the whole
/// table is dropped once per window, so memory is bounded by `max_keys` and the
/// cleanup cost is amortized over the requests that created the entries. When
/// the table is full (more than `max_keys` distinct clients inside one window)
/// new keys fail open; caches and concurrency limits remain the backstop.
pub(crate) struct WindowedCounter {
    window: Duration,
    max_keys: usize,
    window_started_at: Option<Instant>,
    counts: HashMap<String, u32>,
}

impl WindowedCounter {
    pub(crate) fn new(window: Duration, max_keys: usize) -> Self {
        Self {
            window,
            max_keys,
            window_started_at: None,
            counts: HashMap::new(),
        }
    }

    fn roll(&mut self, now: Instant) {
        if self
            .window_started_at
            .is_none_or(|started| now.saturating_duration_since(started) >= self.window)
        {
            if self.counts.capacity() > self.max_keys.saturating_mul(2) {
                self.counts = HashMap::new();
            } else {
                self.counts.clear();
            }
            self.window_started_at = Some(now);
        }
    }

    /// Count one event for `key` unless it already reached `limit` in the
    /// current window. Returns whether the event is allowed.
    pub(crate) fn try_acquire(&mut self, key: &str, limit: u32, now: Instant) -> bool {
        self.roll(now);
        if let Some(count) = self.counts.get_mut(key) {
            if *count >= limit {
                return false;
            }
            *count += 1;
            return true;
        }
        if limit == 0 {
            return false;
        }
        if self.counts.len() < self.max_keys {
            self.counts.insert(key.to_string(), 1);
        }
        true
    }

    /// Record an event without enforcing a limit.
    pub(crate) fn record(&mut self, key: &str, now: Instant) {
        self.roll(now);
        if let Some(count) = self.counts.get_mut(key) {
            *count = count.saturating_add(1);
        } else if self.counts.len() < self.max_keys {
            self.counts.insert(key.to_string(), 1);
        }
    }

    pub(crate) fn count(&mut self, key: &str, now: Instant) -> u32 {
        self.roll(now);
        self.counts.get(key).copied().unwrap_or(0)
    }

    /// Whole seconds (at least one) until the current window resets.
    pub(crate) fn retry_after_secs(&self, now: Instant) -> u64 {
        let elapsed = self
            .window_started_at
            .map(|started| now.saturating_duration_since(started))
            .unwrap_or_default();
        self.window.saturating_sub(elapsed).as_secs_f64().ceil().max(1.0) as u64
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.counts.len()
    }
}

pub(crate) enum CacheLookup {
    Fresh(Bytes),
    Stale(Bytes),
    Miss,
}

struct CachedBody {
    body: Bytes,
    stored_at: Instant,
}

/// Serialized JSON bodies of expensive public reads. Fresh entries are served
/// to everyone; entries past their TTL are kept for `stale_ttl` so overloaded or
/// rate-limited requests can degrade to slightly old data instead of errors.
/// Bounded by entry count and total bytes; cleanup runs at most once a second.
pub(crate) struct PublicReadCache {
    entries: HashMap<String, CachedBody>,
    bytes: usize,
    max_bytes: usize,
    max_entries: usize,
    stale_ttl: Duration,
    last_cleanup_at: Option<Instant>,
}

impl PublicReadCache {
    pub(crate) fn new(max_entries: usize, max_bytes: usize, stale_ttl: Duration) -> Self {
        Self {
            entries: HashMap::new(),
            bytes: 0,
            max_bytes,
            max_entries,
            stale_ttl,
            last_cleanup_at: None,
        }
    }

    pub(crate) fn lookup(&self, key: &str, ttl: Duration, now: Instant) -> CacheLookup {
        match self.entries.get(key) {
            Some(entry) => {
                let age = now.saturating_duration_since(entry.stored_at);
                if age < ttl {
                    CacheLookup::Fresh(entry.body.clone())
                } else if age < self.stale_ttl {
                    CacheLookup::Stale(entry.body.clone())
                } else {
                    CacheLookup::Miss
                }
            }
            None => CacheLookup::Miss,
        }
    }

    fn cleanup(&mut self, now: Instant) {
        let stale_ttl = self.stale_ttl;
        let mut freed = 0usize;
        self.entries.retain(|_, entry| {
            let keep = now.saturating_duration_since(entry.stored_at) < stale_ttl;
            if !keep {
                freed += entry.body.len();
            }
            keep
        });
        self.bytes = self.bytes.saturating_sub(freed);
        self.last_cleanup_at = Some(now);
    }

    pub(crate) fn insert(&mut self, key: String, body: Bytes, now: Instant) {
        if self
            .last_cleanup_at
            .is_none_or(|last| now.saturating_duration_since(last) >= Duration::from_secs(1))
        {
            self.cleanup(now);
        }
        if let Some(previous) = self.entries.remove(&key) {
            self.bytes = self.bytes.saturating_sub(previous.body.len());
        }
        if self.entries.len() >= self.max_entries
            || self.bytes.saturating_add(body.len()) > self.max_bytes
        {
            // Full of live entries: serve this response uncached rather than
            // scanning for victims on every request.
            return;
        }
        self.bytes += body.len();
        self.entries.insert(
            key,
            CachedBody {
                body,
                stored_at: now,
            },
        );
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.entries.len()
    }

    #[cfg(test)]
    pub(crate) fn bytes(&self) -> usize {
        self.bytes
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;

    fn headers(pairs: &[(&'static str, &'static str)]) -> HeaderMap {
        let mut map = HeaderMap::new();
        for (name, value) in pairs {
            map.append(*name, HeaderValue::from_static(value));
        }
        map
    }

    const LOOPBACK: Option<IpAddr> = Some(IpAddr::V4(std::net::Ipv4Addr::LOCALHOST));

    #[test]
    fn real_ip_header_wins_over_spoofed_forwarded_for() {
        let policy = ClientIpPolicy::default();
        let h = headers(&[
            ("x-forwarded-for", "1.1.1.1, 203.0.113.7"),
            ("x-real-ip", "203.0.113.7"),
        ]);
        assert_eq!(
            policy.resolve(&h, LOOPBACK),
            Some("203.0.113.7".parse().unwrap())
        );
    }

    #[test]
    fn rightmost_forwarded_hop_is_used_without_real_ip() {
        let policy = ClientIpPolicy::default();
        // nginx `$proxy_add_x_forwarded_for` appends the real peer on the right.
        let h = headers(&[("x-forwarded-for", "6.6.6.6, 198.51.100.4")]);
        assert_eq!(
            policy.resolve(&h, LOOPBACK),
            Some("198.51.100.4".parse().unwrap())
        );
        // Multiple header lines: the last line's last hop is closest to us.
        let h = headers(&[
            ("x-forwarded-for", "6.6.6.6"),
            ("x-forwarded-for", "198.51.100.5, 127.0.0.1"),
        ]);
        assert_eq!(
            policy.resolve(&h, LOOPBACK),
            Some("198.51.100.5".parse().unwrap())
        );
    }

    #[test]
    fn rotating_spoofed_leftmost_entries_share_one_key() {
        let policy = ClientIpPolicy::default();
        let keys = ["10.0.0.1", "10.0.0.2", "10.0.0.3"]
            .iter()
            .map(|spoof| {
                let mut h = HeaderMap::new();
                h.insert(
                    "x-forwarded-for",
                    HeaderValue::from_str(&format!("{spoof}, 198.51.100.9")).unwrap(),
                );
                policy.resolve(&h, LOOPBACK).map(client_key)
            })
            .collect::<Vec<_>>();
        assert!(keys.iter().all(|key| key.as_deref() == Some("198.51.100.9")));
    }

    #[test]
    fn untrusted_peer_ignores_forwarding_headers() {
        let policy = ClientIpPolicy::default();
        let h = headers(&[("x-real-ip", "1.2.3.4"), ("x-forwarded-for", "1.2.3.4")]);
        let peer: IpAddr = "198.51.100.20".parse().unwrap();
        assert_eq!(policy.resolve(&h, Some(peer)), Some(peer));
    }

    #[test]
    fn configured_proxy_is_trusted_and_header_is_configurable() {
        let policy = ClientIpPolicy::new("CF-Connecting-IP", &["10.1.2.3".to_string()]);
        let h = headers(&[("cf-connecting-ip", "203.0.113.50"), ("x-real-ip", "9.9.9.9")]);
        assert_eq!(
            policy.resolve(&h, Some("10.1.2.3".parse().unwrap())),
            Some("203.0.113.50".parse().unwrap())
        );
    }

    #[test]
    fn trusted_proxy_without_headers_or_with_garbage_is_unknown() {
        let policy = ClientIpPolicy::default();
        assert_eq!(policy.resolve(&HeaderMap::new(), LOOPBACK), None);
        assert_eq!(policy.resolve(&HeaderMap::new(), None), None);
        let h = headers(&[("x-real-ip", "not-an-ip"), ("x-forwarded-for", "garbage")]);
        assert_eq!(policy.resolve(&h, LOOPBACK), None);
    }

    #[test]
    fn ipv6_clients_are_grouped_by_prefix_and_mapped_v4_is_normalized() {
        let a: IpAddr = "2001:db8:1:2:aaaa::1".parse().unwrap();
        let b: IpAddr = "2001:db8:1:2:bbbb::2".parse().unwrap();
        assert_eq!(client_key(a), client_key(b));
        assert_eq!(client_key(a), "2001:db8:1:2::/64");
        let policy = ClientIpPolicy::default();
        let h = headers(&[("x-real-ip", "::ffff:203.0.113.8")]);
        assert_eq!(
            policy.resolve(&h, LOOPBACK).map(client_key).as_deref(),
            Some("203.0.113.8")
        );
    }

    #[test]
    fn windowed_counter_limits_resets_and_bounds_memory() {
        let mut counter = WindowedCounter::new(Duration::from_secs(5), 3);
        let start = Instant::now();
        for _ in 0..2 {
            assert!(counter.try_acquire("a", 2, start));
        }
        assert!(!counter.try_acquire("a", 2, start));
        assert_eq!(counter.retry_after_secs(start + Duration::from_millis(1500)), 4);
        assert!(counter.try_acquire("b", 2, start));
        assert!(counter.try_acquire("c", 2, start));
        // Table full: unseen keys fail open without growing the table.
        assert!(counter.try_acquire("d", 2, start));
        assert!(counter.try_acquire("d", 2, start));
        assert!(counter.try_acquire("d", 2, start));
        assert_eq!(counter.len(), 3);
        // Next window drops everything.
        let later = start + Duration::from_secs(5);
        assert!(counter.try_acquire("a", 2, later));
        assert_eq!(counter.len(), 1);
    }

    #[test]
    fn windowed_counter_records_failures() {
        let mut counter = WindowedCounter::new(Duration::from_secs(300), 16);
        let start = Instant::now();
        for _ in 0..3 {
            counter.record("ip", start);
        }
        assert_eq!(counter.count("ip", start), 3);
        assert_eq!(counter.count("ip", start + Duration::from_secs(300)), 0);
    }

    #[test]
    fn public_read_cache_serves_fresh_then_stale_then_expires() {
        let mut cache = PublicReadCache::new(8, 1024, Duration::from_secs(60));
        let start = Instant::now();
        cache.insert("k".into(), Bytes::from_static(b"[1]"), start);
        let ttl = Duration::from_secs(10);
        assert!(matches!(cache.lookup("k", ttl, start), CacheLookup::Fresh(_)));
        assert!(matches!(
            cache.lookup("k", ttl, start + Duration::from_secs(11)),
            CacheLookup::Stale(_)
        ));
        assert!(matches!(
            cache.lookup("k", ttl, start + Duration::from_secs(61)),
            CacheLookup::Miss
        ));
        assert!(matches!(cache.lookup("other", ttl, start), CacheLookup::Miss));
    }

    #[test]
    fn public_read_cache_enforces_entry_and_byte_budgets() {
        let mut cache = PublicReadCache::new(2, 10, Duration::from_secs(60));
        let start = Instant::now();
        cache.insert("a".into(), Bytes::from_static(b"12345"), start);
        cache.insert("b".into(), Bytes::from_static(b"1234"), start);
        cache.insert("c".into(), Bytes::from_static(b"1"), start);
        assert_eq!(cache.len(), 2);
        assert_eq!(cache.bytes(), 9);
        // Replacing an entry re-accounts its bytes.
        cache.insert("a".into(), Bytes::from_static(b"12"), start);
        assert_eq!(cache.bytes(), 6);
        // Expired entries are reclaimed by the next cleanup.
        let later = start + Duration::from_secs(61);
        cache.insert("d".into(), Bytes::from_static(b"123"), later);
        assert_eq!(cache.len(), 1);
        assert_eq!(cache.bytes(), 3);
    }
}
