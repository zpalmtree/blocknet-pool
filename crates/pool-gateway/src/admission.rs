//! Per-IP admission for miner sockets, enforced in the accept tasks so a
//! refused miner never reaches the relay's core loop.
//!
//! A miner over its IP group's limit receives a `connection_limit` notice and
//! its socket is held open for the notice's retry delay before it closes, so a
//! client that ignores the notice still reconnects no faster than that.

use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use pool_common::net::ip_group_key;
use pool_common::protocol::{connection_limit_line, CONNECTION_LIMIT_RETRY_MS};
use rand::Rng;
use tokio::io::{AsyncWrite, AsyncWriteExt};

use crate::GatewayMetrics;

/// Refused sockets held open at once, across all IP groups.
const MAX_HELD: usize = 16_384;
/// Bound on writing the notice to a refused miner.
const NOTICE_WRITE_TIMEOUT: Duration = Duration::from_secs(5);

/// The miner listeners' connection limits.
#[derive(Debug, Clone, Copy)]
pub struct MinerLimits {
    /// Miners relayed at once (`gateway_max_miners`).
    pub max_miners: usize,
    /// Miners relayed at once from one IP group
    /// (`stratum_max_connections_per_ip`).
    pub max_per_ip: usize,
}

impl From<usize> for MinerLimits {
    /// A total cap with no tighter per-IP limit.
    fn from(max_miners: usize) -> Self {
        Self {
            max_miners,
            max_per_ip: max_miners,
        }
    }
}

#[derive(Debug, Default)]
struct State {
    open: HashMap<String, usize>,
    held: HashMap<String, usize>,
    held_total: usize,
}

/// Connection counts per IP group, shared by the plain and TLS listeners.
#[derive(Debug)]
pub struct Admission {
    limits: MinerLimits,
    metrics: Arc<GatewayMetrics>,
    state: Mutex<State>,
}

/// What to do with an accepted socket.
pub enum Admit {
    /// Relay it; the slot is released when the guard drops.
    Relay(Slot),
    /// Send the notice and hold the socket for `Slot::hold_for`.
    Hold(Slot),
    /// Close it now: the hold capacity for its group is used up.
    Refuse,
}

/// A counted socket. Dropping it releases its count.
pub struct Slot {
    admission: Arc<Admission>,
    key: String,
    held: bool,
}

impl Admission {
    pub fn new(limits: MinerLimits, metrics: Arc<GatewayMetrics>) -> Arc<Self> {
        metrics
            .max_miners
            .store(limits.max_miners as u64, Ordering::Relaxed);
        metrics
            .max_miners_per_ip
            .store(limits.max_per_ip as u64, Ordering::Relaxed);
        Arc::new(Self {
            limits,
            metrics,
            state: Mutex::new(State::default()),
        })
    }

    pub fn limits(&self) -> MinerLimits {
        self.limits
    }

    pub fn admit(self: &Arc<Self>, ip: IpAddr) -> Admit {
        let key = ip_group_key(ip);
        let mut state = self.state.lock().unwrap_or_else(|err| err.into_inner());
        let open = state.open.get(&key).copied().unwrap_or(0);
        if open < self.limits.max_per_ip {
            state.open.insert(key.clone(), open + 1);
            return Admit::Relay(Slot {
                admission: Arc::clone(self),
                key,
                held: false,
            });
        }
        self.metrics.rejected_per_ip.fetch_add(1, Ordering::Relaxed);
        let held = state.held.get(&key).copied().unwrap_or(0);
        // One group may hold at most as many refused sockets as it may relay.
        if state.held_total >= MAX_HELD || held >= self.limits.max_per_ip {
            return Admit::Refuse;
        }
        state.held.insert(key.clone(), held + 1);
        state.held_total += 1;
        self.metrics
            .held_connections
            .store(state.held_total as u64, Ordering::Relaxed);
        Admit::Hold(Slot {
            admission: Arc::clone(self),
            key,
            held: true,
        })
    }

    fn release(&self, key: &str, held: bool) {
        let mut state = self.state.lock().unwrap_or_else(|err| err.into_inner());
        let counts = if held {
            &mut state.held
        } else {
            &mut state.open
        };
        if let Some(count) = counts.get_mut(key) {
            if *count > 1 {
                *count -= 1;
            } else {
                counts.remove(key);
            }
        }
        if held {
            state.held_total = state.held_total.saturating_sub(1);
            self.metrics
                .held_connections
                .store(state.held_total as u64, Ordering::Relaxed);
        }
    }
}

impl Slot {
    /// A random delay from the notice's retry range.
    pub fn hold_for() -> Duration {
        Duration::from_millis(rand::thread_rng().gen_range(CONNECTION_LIMIT_RETRY_MS))
    }
}

impl Drop for Slot {
    fn drop(&mut self) {
        self.admission.release(&self.key, self.held);
    }
}

/// Sends the `connection_limit` notice on a plain socket, then holds it for
/// the delay the notice names before it closes.
pub async fn hold_plain<W: AsyncWrite + Unpin>(mut writer: W, slot: Slot, hold: Duration) {
    let mut line = connection_limit_line(hold.as_millis() as u64);
    line.push('\n');
    let _ = tokio::time::timeout(NOTICE_WRITE_TIMEOUT, writer.write_all(line.as_bytes())).await;
    tokio::time::sleep(hold).await;
    let _ = tokio::time::timeout(NOTICE_WRITE_TIMEOUT, writer.shutdown()).await;
    drop(slot);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn admission(max_per_ip: usize) -> Arc<Admission> {
        Admission::new(
            MinerLimits {
                max_miners: 100,
                max_per_ip,
            },
            Arc::new(GatewayMetrics::default()),
        )
    }

    #[test]
    fn groups_over_their_limit_are_held_then_refused() {
        let admission = admission(2);
        let ip: IpAddr = "203.0.113.7".parse().unwrap();
        let relayed: Vec<_> = (0..2)
            .map(|_| match admission.admit(ip) {
                Admit::Relay(slot) => slot,
                _ => panic!("under the limit"),
            })
            .collect();
        let held: Vec<_> = (0..2)
            .map(|_| match admission.admit(ip) {
                Admit::Hold(slot) => slot,
                _ => panic!("over the limit with hold capacity"),
            })
            .collect();
        assert!(matches!(admission.admit(ip), Admit::Refuse));
        assert_eq!(admission.metrics.rejected_per_ip.load(Ordering::Relaxed), 3);
        assert_eq!(
            admission.metrics.held_connections.load(Ordering::Relaxed),
            2
        );

        // Another group is unaffected.
        let other: IpAddr = "198.51.100.1".parse().unwrap();
        assert!(matches!(admission.admit(other), Admit::Relay(_)));

        drop(held);
        assert_eq!(
            admission.metrics.held_connections.load(Ordering::Relaxed),
            0
        );
        assert!(matches!(admission.admit(ip), Admit::Hold(_)));
        drop(relayed);
        assert!(matches!(admission.admit(ip), Admit::Relay(_)));
    }

    #[test]
    fn ipv6_addresses_in_one_64_share_a_limit() {
        let admission = admission(1);
        let a: IpAddr = "2001:db8:1:2::1".parse().unwrap();
        let b: IpAddr = "2001:db8:1:2::2".parse().unwrap();
        let _slot = match admission.admit(a) {
            Admit::Relay(slot) => slot,
            _ => panic!("first connection"),
        };
        assert!(matches!(admission.admit(b), Admit::Hold(_)));
    }

    #[tokio::test]
    async fn held_miners_get_the_notice_with_the_hold_delay() {
        let admission = admission(1);
        let ip: IpAddr = "203.0.113.7".parse().unwrap();
        let _relayed = admission.admit(ip);
        let Admit::Hold(slot) = admission.admit(ip) else {
            panic!("over the limit");
        };
        let (mut client, server) = tokio::io::duplex(4096);
        let hold = Duration::from_millis(20);
        let started = std::time::Instant::now();
        let holder = tokio::spawn(hold_plain(server, slot, hold));
        let mut received = String::new();
        tokio::io::AsyncReadExt::read_to_string(&mut client, &mut received)
            .await
            .unwrap();
        assert!(started.elapsed() >= hold);
        holder.await.unwrap();
        let notice: serde_json::Value = serde_json::from_str(received.trim()).unwrap();
        assert_eq!(notice["params"]["kind"], "connection_limit");
        assert_eq!(notice["params"]["retry_after_ms"], 20);
        assert_eq!(
            admission.metrics.held_connections.load(Ordering::Relaxed),
            0
        );
    }
}
