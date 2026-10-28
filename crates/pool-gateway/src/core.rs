//! Routing state of the gateway, driven by one task (see `crate::run`).
//!
//! Every decision about where a miner's line goes lives here, as plain
//! synchronous code taking `now`, so it can be tested without sockets.

use std::collections::{HashMap, HashSet, VecDeque};
use std::net::SocketAddr;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};

use pool_common::gateway::{EngineHello, Frame, FrameKind, GatewayRole, REPLAY_LOGIN_REQUEST_ID};
use serde_json::Value;
use tokio::sync::mpsc;

use crate::GatewayMetrics;

/// Job IDs remembered per miner, newest last; older ones fall back to the
/// miner's current engine.
const JOB_HISTORY: usize = 64;
/// The engine's retryable message: seine resubmits shares rejected with it.
const BUSY_ERROR: &str = "server busy, retry";
/// Wait after a rejected or unanswered replay before trying that miner again.
const REPLAY_BACKOFF: Duration = Duration::from_secs(5);
const MAX_PENDING_LOGINS: usize = 4;
/// Blank keepalive lines are relayed at most this often per miner. The engine
/// drops a logged-in session after 15 idle minutes, and an idle miner (such as
/// seine's parked dev-fee or user session) sends nothing else.
const KEEPALIVE_RELAY_INTERVAL: Duration = Duration::from_secs(60);

#[derive(Debug, Clone)]
pub struct Timings {
    /// Logins replayed on the primary per second while migrating miners.
    pub migrate_per_second: u32,
    /// How long a drained engine keeps serving old-job shares before it is
    /// retired.
    pub retire_grace: Duration,
    /// A replayed login without an answer by then is retried.
    pub replay_timeout: Duration,
    /// Miners left without any engine are disconnected after this.
    pub engine_absent_timeout: Duration,
    /// Miners that never log in are disconnected after this.
    pub login_timeout: Duration,
    /// Miners still on a draining engine after this are reconnected, so a
    /// handover always completes.
    pub drain_deadline: Duration,
    pub tick: Duration,
}

impl Default for Timings {
    fn default() -> Self {
        Self {
            migrate_per_second: 200,
            retire_grace: Duration::from_secs(30),
            replay_timeout: Duration::from_secs(15),
            engine_absent_timeout: Duration::from_secs(120),
            login_timeout: Duration::from_secs(30),
            drain_deadline: Duration::from_secs(180),
            tick: Duration::from_millis(100),
        }
    }
}

/// What the gateway sends a miner's socket.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MinerOut {
    Line(String),
    Close,
}

#[derive(Debug)]
pub enum Event {
    MinerConnected {
        conn: u64,
        peer: SocketAddr,
        tx: mpsc::Sender<MinerOut>,
    },
    MinerLine {
        conn: u64,
        line: String,
    },
    /// A blank line: the miner is alive but has nothing to say.
    MinerKeepalive {
        conn: u64,
    },
    MinerClosed {
        conn: u64,
    },
    EngineConnected {
        engine: u64,
        tx: mpsc::Sender<Frame>,
    },
    EngineFrame {
        engine: u64,
        frame: Frame,
    },
    EngineClosed {
        engine: u64,
    },
    Tick,
}

struct Miner {
    peer: SocketAddr,
    tx: mpsc::Sender<MinerOut>,
    connected_at: Instant,
    /// The login request an engine accepted; replayed on each new engine.
    login_line: Option<String>,
    /// Login requests awaiting an answer, by request ID. A miner may retry
    /// with a new ID after a rejected login on the same connection.
    pending_logins: VecDeque<(u64, String)>,
    /// The most recent login request, accepted or not.
    last_login: Option<String>,
    /// The login arrived while no engine was available; it is forwarded,
    /// unchanged, once one is.
    login_held: bool,
    /// Some engine has accepted this miner's login.
    logged_in: bool,
    /// Engine receiving the miner's requests and whose jobs it works on.
    current: Option<u64>,
    /// Engines holding a virtual connection for this miner.
    opened: HashSet<u64>,
    /// Replayed login awaiting an answer: (engine, sent at).
    replay: Option<(u64, Instant)>,
    replay_not_before: Option<Instant>,
    /// Job ID → issuing engine, newest last.
    jobs: VecDeque<(String, u64)>,
    /// Since when the miner has had no engine.
    orphaned_since: Option<Instant>,
    /// When a line from this miner last reached its current engine.
    last_relayed_at: Option<Instant>,
}

struct EngineLink {
    tx: mpsc::Sender<Frame>,
    hello: Option<EngineHello>,
    role: Option<GatewayRole>,
    /// Since when no miner has used this draining engine as current.
    drained_since: Option<Instant>,
    /// Since when this engine has been draining.
    draining_since: Option<Instant>,
    retired: bool,
}

pub struct Gateway {
    timings: Timings,
    metrics: Arc<GatewayMetrics>,
    miners: HashMap<u64, Miner>,
    engines: HashMap<u64, EngineLink>,
    primary: Option<u64>,
    migrate_credit: f64,
}

impl Gateway {
    pub fn new(timings: Timings, metrics: Arc<GatewayMetrics>) -> Self {
        Self {
            timings,
            metrics,
            miners: HashMap::new(),
            engines: HashMap::new(),
            primary: None,
            migrate_credit: 0.0,
        }
    }

    pub fn handle(&mut self, event: Event, now: Instant) {
        match event {
            Event::MinerConnected { conn, peer, tx } => self.miner_connected(conn, peer, tx, now),
            Event::MinerLine { conn, line } => self.miner_line(conn, line, now),
            Event::MinerKeepalive { conn } => self.miner_keepalive(conn, now),
            Event::MinerClosed { conn } => self.miner_closed(conn),
            Event::EngineConnected { engine, tx } => {
                self.engines.insert(
                    engine,
                    EngineLink {
                        tx,
                        hello: None,
                        role: None,
                        drained_since: None,
                        draining_since: None,
                        retired: false,
                    },
                );
            }
            Event::EngineFrame { engine, frame } => self.engine_frame(engine, frame, now),
            Event::EngineClosed { engine } => self.engine_closed(engine, now),
            Event::Tick => self.tick(now),
        }
        self.publish_gauges();
    }

    fn miner_connected(
        &mut self,
        conn: u64,
        peer: SocketAddr,
        tx: mpsc::Sender<MinerOut>,
        now: Instant,
    ) {
        let mut miner = Miner {
            peer,
            tx,
            connected_at: now,
            login_line: None,
            pending_logins: VecDeque::new(),
            last_login: None,
            login_held: false,
            logged_in: false,
            current: None,
            opened: HashSet::new(),
            replay: None,
            replay_not_before: None,
            jobs: VecDeque::new(),
            orphaned_since: Some(now),
            last_relayed_at: None,
        };
        if let Some(primary) = self.primary {
            if open_on(&self.engines, &mut miner, conn, primary) {
                miner.current = Some(primary);
                miner.orphaned_since = None;
            }
        }
        self.miners.insert(conn, miner);
    }

    fn miner_line(&mut self, conn: u64, line: String, now: Instant) {
        let Some(miner) = self.miners.get_mut(&conn) else {
            return;
        };
        let parsed = serde_json::from_str::<Value>(&line).ok();
        let id = parsed
            .as_ref()
            .and_then(|v| v.get("id"))
            .and_then(Value::as_u64);
        let method = parsed
            .as_ref()
            .and_then(|v| v.get("method"))
            .and_then(Value::as_str);

        if method == Some("login") {
            miner.last_login = Some(line.clone());
            if let Some(id) = id {
                miner.pending_logins.push_back((id, line.clone()));
                while miner.pending_logins.len() > MAX_PENDING_LOGINS {
                    miner.pending_logins.pop_front();
                }
            }
        }
        if miner.current.is_none() {
            if let Some(primary) = self.primary {
                if open_on(&self.engines, miner, conn, primary) {
                    miner.current = Some(primary);
                    miner.orphaned_since = None;
                }
            }
        }

        // A share goes to the engine that issued its job, so a share for an
        // old job (even a block candidate) is still judged and submitted.
        let target = match method {
            Some("submit") => parsed
                .as_ref()
                .and_then(|v| v.get("params"))
                .and_then(|p| p.get("job_id"))
                .and_then(Value::as_str)
                .and_then(|job| {
                    miner
                        .jobs
                        .iter()
                        .rev()
                        .find(|(known, _)| known == job)
                        .map(|(_, engine)| *engine)
                })
                .filter(|engine| miner.opened.contains(engine) && is_live(&self.engines, *engine))
                .or(miner.current),
            _ => miner.current,
        };

        let delivered = target
            .and_then(|engine| self.engines.get(&engine))
            .is_some_and(|link| {
                link.tx
                    .try_send(Frame::new(FrameKind::Line, conn, line.into_bytes()))
                    .is_ok()
            });
        if delivered && target == miner.current {
            miner.last_relayed_at = Some(now);
        }
        if !delivered {
            if miner.current.is_none() && miner.orphaned_since.is_none() {
                miner.orphaned_since = Some(now);
            }
            if method == Some("login") && miner.current.is_none() {
                // An engine is starting (deploy or restart): answer the login
                // late rather than refuse it, which some miners treat as a
                // reason to back off or fail over.
                miner.login_held = true;
                return;
            }
            if let Some(id) = id {
                self.metrics.busy_replies.fetch_add(1, Ordering::Relaxed);
                let reply = serde_json::json!({ "id": id, "error": BUSY_ERROR }).to_string();
                send_miner(miner, MinerOut::Line(reply));
            }
        }
    }

    /// Relays a miner's keepalive to its current engine, which otherwise
    /// times the session out (and so disconnects the miner) after 15 quiet
    /// minutes. Old engines only hold sessions for old-job shares and are
    /// retired, so they are not kept alive.
    fn miner_keepalive(&mut self, conn: u64, now: Instant) {
        let Some(miner) = self.miners.get_mut(&conn) else {
            return;
        };
        let Some(engine) = miner.current.filter(|_| miner.logged_in) else {
            return;
        };
        if miner
            .last_relayed_at
            .is_some_and(|at| now.duration_since(at) < KEEPALIVE_RELAY_INTERVAL)
        {
            return;
        }
        let delivered = self.engines.get(&engine).is_some_and(|link| {
            link.tx
                .try_send(Frame::new(FrameKind::Line, conn, Vec::new()))
                .is_ok()
        });
        if delivered {
            miner.last_relayed_at = Some(now);
        }
    }

    fn miner_closed(&mut self, conn: u64) {
        if let Some(miner) = self.miners.remove(&conn) {
            for engine in miner.opened {
                send_engine(
                    &self.engines,
                    engine,
                    Frame::new(FrameKind::Close, conn, Vec::new()),
                );
            }
        }
    }

    fn engine_frame(&mut self, engine: u64, frame: Frame, now: Instant) {
        match frame.kind {
            FrameKind::Hello => {
                let hello = frame
                    .payload_str()
                    .ok()
                    .and_then(|p| serde_json::from_str::<EngineHello>(p).ok());
                let Some(hello) = hello else {
                    tracing::warn!(engine, "engine sent a malformed hello");
                    return;
                };
                tracing::info!(engine, instance = %hello.instance, pid = hello.pid, "engine connected");
                if let Some(link) = self.engines.get_mut(&engine) {
                    link.hello = Some(hello);
                }
                self.elect_primary(now);
            }
            FrameKind::Line => self.engine_line(engine, frame, now),
            FrameKind::Close => {
                let conn = frame.conn;
                let Some(miner) = self.miners.get_mut(&conn) else {
                    return;
                };
                miner.opened.remove(&engine);
                if miner
                    .replay
                    .is_some_and(|(replay_engine, _)| replay_engine == engine)
                {
                    miner.replay = None;
                    miner.replay_not_before = Some(now + REPLAY_BACKOFF);
                }
                // The miner's own engine ended the session (limits, timeout,
                // ban): end the miner's connection too.
                if miner.current == Some(engine) {
                    send_miner(miner, MinerOut::Close);
                    self.miner_closed(conn);
                }
            }
            FrameKind::Open | FrameKind::Role | FrameKind::Retire => {
                tracing::warn!(engine, kind = ?frame.kind, "ignoring gateway-only frame from engine");
            }
        }
    }

    fn engine_line(&mut self, engine: u64, frame: Frame, now: Instant) {
        let conn = frame.conn;
        let Some(miner) = self.miners.get_mut(&conn) else {
            // Miner already gone; let the engine end its session.
            send_engine(
                &self.engines,
                engine,
                Frame::new(FrameKind::Close, conn, Vec::new()),
            );
            return;
        };
        let Ok(line) = String::from_utf8(frame.payload) else {
            return;
        };
        let parsed = serde_json::from_str::<Value>(&line).ok();
        let id = parsed
            .as_ref()
            .and_then(|v| v.get("id"))
            .and_then(Value::as_u64);
        let method = parsed
            .as_ref()
            .and_then(|v| v.get("method"))
            .and_then(Value::as_str)
            .map(str::to_owned);
        let failed = parsed
            .as_ref()
            .and_then(|v| v.get("error"))
            .is_some_and(|error| !error.is_null());

        if method.is_none() && id == Some(REPLAY_LOGIN_REQUEST_ID) {
            // Answer to our replayed login: never forwarded to the miner.
            if miner
                .replay
                .is_some_and(|(replay_engine, _)| replay_engine == engine)
            {
                miner.replay = None;
                if failed {
                    miner.replay_not_before = Some(now + REPLAY_BACKOFF);
                    self.metrics.replay_failures.fetch_add(1, Ordering::Relaxed);
                    tracing::warn!(conn, engine, peer = %miner.peer, response = %line, "replayed login rejected; miner stays on its engine");
                    miner.opened.remove(&engine);
                    send_engine(
                        &self.engines,
                        engine,
                        Frame::new(FrameKind::Close, conn, Vec::new()),
                    );
                } else {
                    self.metrics.migrations.fetch_add(1, Ordering::Relaxed);
                    miner.current = Some(engine);
                    miner.orphaned_since = None;
                    miner.logged_in = true;
                }
            }
            return;
        }

        let from_current = miner.current == Some(engine);
        match method.as_deref() {
            Some("job") => {
                if !from_current {
                    // A replaced engine still pushes jobs for new templates;
                    // the miner must keep working on its current engine's.
                    self.metrics
                        .stale_engine_jobs
                        .fetch_add(1, Ordering::Relaxed);
                    return;
                }
                let job_id = parsed
                    .as_ref()
                    .and_then(|v| v.get("params"))
                    .and_then(|p| p.get("job_id"))
                    .and_then(Value::as_str);
                if let Some(job_id) = job_id {
                    miner.jobs.push_back((job_id.to_string(), engine));
                    while miner.jobs.len() > JOB_HISTORY {
                        miner.jobs.pop_front();
                    }
                }
            }
            Some(_) => {
                if !from_current {
                    return;
                }
            }
            None => {
                let answered = id.and_then(|id| {
                    miner
                        .pending_logins
                        .iter()
                        .position(|(pending, _)| *pending == id)
                });
                if let (true, Some(index)) = (from_current, answered) {
                    let (_, login) = miner.pending_logins.remove(index).expect("index");
                    if !failed {
                        miner.login_line = Some(login);
                        miner.logged_in = true;
                    }
                }
            }
        }
        send_miner(miner, MinerOut::Line(line));
    }

    fn engine_closed(&mut self, engine: u64, now: Instant) {
        let Some(link) = self.engines.remove(&engine) else {
            return;
        };
        if link.retired {
            tracing::info!(engine, "retired engine disconnected");
        } else {
            tracing::warn!(engine, "engine disconnected");
        }
        for miner in self.miners.values_mut() {
            miner.opened.remove(&engine);
            miner.jobs.retain(|(_, issuer)| *issuer != engine);
            if miner
                .replay
                .is_some_and(|(replay_engine, _)| replay_engine == engine)
            {
                miner.replay = None;
            }
            if miner.current == Some(engine) {
                miner.current = None;
                miner.orphaned_since = Some(now);
            }
        }
        if self.primary == Some(engine) {
            self.primary = None;
            self.elect_primary(now);
        }
    }

    /// The newest engine (by start time) that has said hello is primary; the
    /// others drain. The old primary is demoted before the new one is
    /// promoted, so singleton work never runs on both for longer than an
    /// in-flight tick.
    fn elect_primary(&mut self, now: Instant) {
        let newest = self
            .engines
            .iter()
            .filter(|(_, link)| !link.retired)
            .filter_map(|(id, link)| link.hello.as_ref().map(|hello| (hello.started_at_ms, *id)))
            .max()
            .map(|(_, id)| id);
        for (id, link) in self.engines.iter_mut() {
            if link.hello.is_none() || link.retired || Some(*id) == newest {
                continue;
            }
            if link.role != Some(GatewayRole::Draining) {
                link.role = Some(GatewayRole::Draining);
                link.draining_since = Some(now);
                let _ = link.tx.try_send(role_frame(GatewayRole::Draining));
                tracing::info!(engine = id, "engine draining");
            }
        }
        if let Some(newest) = newest {
            if let Some(link) = self.engines.get_mut(&newest) {
                if link.role != Some(GatewayRole::Primary) {
                    link.role = Some(GatewayRole::Primary);
                    link.drained_since = None;
                    link.draining_since = None;
                    let _ = link.tx.try_send(role_frame(GatewayRole::Primary));
                    tracing::info!(engine = newest, "engine primary");
                }
            }
        }
        self.primary = newest;
    }

    fn tick(&mut self, now: Instant) {
        self.expire(now);
        self.migrate(now);
        self.retire_drained(now);
    }

    fn expire(&mut self, now: Instant) {
        let mut closing = Vec::new();
        for (conn, miner) in self.miners.iter_mut() {
            if miner
                .replay
                .is_some_and(|(_, sent)| now.duration_since(sent) >= self.timings.replay_timeout)
            {
                let (engine, _) = miner.replay.take().expect("checked");
                miner.replay_not_before = Some(now + REPLAY_BACKOFF);
                tracing::warn!(conn, engine, "replayed login unanswered; retrying later");
                miner.opened.remove(&engine);
                send_engine(
                    &self.engines,
                    engine,
                    Frame::new(FrameKind::Close, *conn, Vec::new()),
                );
            }
            let login_expired = miner.last_login.is_none()
                && now.duration_since(miner.connected_at) >= self.timings.login_timeout;
            let orphan_expired = miner.orphaned_since.is_some_and(|since| {
                now.duration_since(since) >= self.timings.engine_absent_timeout
            });
            if login_expired || orphan_expired {
                closing.push(*conn);
            }
        }
        for conn in closing {
            if let Some(miner) = self.miners.get(&conn) {
                send_miner(miner, MinerOut::Close);
            }
            self.miner_closed(conn);
        }
    }

    /// Moves logged-in miners to the primary at a bounded rate by replaying
    /// their login there.
    fn migrate(&mut self, now: Instant) {
        let Some(primary) = self.primary else {
            return;
        };
        let per_tick = self.timings.migrate_per_second as f64 * self.timings.tick.as_secs_f64();
        self.migrate_credit = (self.migrate_credit + per_tick).min(per_tick.max(1.0) * 2.0);
        for (conn, miner) in self.miners.iter_mut() {
            if self.migrate_credit < 1.0 {
                break;
            }
            if miner.login_held && miner.current.is_none() {
                let Some(login) = miner.last_login.clone() else {
                    continue;
                };
                if !open_on(&self.engines, miner, *conn, primary) {
                    break;
                }
                if self.engines[&primary]
                    .tx
                    .try_send(Frame::new(FrameKind::Line, *conn, login.into_bytes()))
                    .is_err()
                {
                    break;
                }
                miner.login_held = false;
                miner.current = Some(primary);
                miner.orphaned_since = None;
                self.migrate_credit -= 1.0;
                continue;
            }
            if miner.current == Some(primary) || miner.replay.is_some() || !miner.logged_in {
                continue;
            }
            if miner
                .replay_not_before
                .is_some_and(|not_before| now < not_before)
            {
                continue;
            }
            let Some(replay) = miner.login_line.as_deref().and_then(replay_login_line) else {
                continue;
            };
            if !open_on(&self.engines, miner, *conn, primary) {
                break;
            }
            if self.engines[&primary]
                .tx
                .try_send(Frame::new(FrameKind::Line, *conn, replay.into_bytes()))
                .is_err()
            {
                break;
            }
            miner.replay = Some((primary, now));
            self.migrate_credit -= 1.0;
        }
    }

    /// Retires a draining engine once no miner works on its jobs and the
    /// grace for late shares has passed.
    fn retire_drained(&mut self, now: Instant) {
        let mut retiring = Vec::new();
        let mut overdue = Vec::new();
        for (id, link) in self.engines.iter_mut() {
            if link.retired || link.role != Some(GatewayRole::Draining) {
                continue;
            }
            let in_use = self.miners.values().any(|miner| miner.current == Some(*id));
            if in_use {
                link.drained_since = None;
                if link
                    .draining_since
                    .is_some_and(|since| now.duration_since(since) >= self.timings.drain_deadline)
                {
                    overdue.push(*id);
                }
                continue;
            }
            let since = *link.drained_since.get_or_insert(now);
            if now.duration_since(since) >= self.timings.retire_grace {
                retiring.push(*id);
            }
        }
        // Miners that could not be moved (e.g. never logged in) reconnect;
        // they land on the primary.
        for engine in overdue {
            let stuck: Vec<u64> = self
                .miners
                .iter()
                .filter(|(_, miner)| miner.current == Some(engine))
                .map(|(conn, _)| *conn)
                .collect();
            tracing::warn!(
                engine,
                miners = stuck.len(),
                "drain deadline passed; reconnecting miners left on the draining engine"
            );
            self.metrics
                .forced_reconnects
                .fetch_add(stuck.len() as u64, Ordering::Relaxed);
            for conn in stuck {
                if let Some(miner) = self.miners.get(&conn) {
                    send_miner(miner, MinerOut::Close);
                }
                self.miner_closed(conn);
            }
        }
        for engine in retiring {
            for (conn, miner) in self.miners.iter_mut() {
                if miner.opened.remove(&engine) {
                    send_engine(
                        &self.engines,
                        engine,
                        Frame::new(FrameKind::Close, *conn, Vec::new()),
                    );
                }
                miner.jobs.retain(|(_, issuer)| *issuer != engine);
            }
            if let Some(link) = self.engines.get_mut(&engine) {
                link.retired = true;
                let _ = link
                    .tx
                    .try_send(Frame::new(FrameKind::Retire, 0, Vec::new()));
            }
            self.metrics.retires.fetch_add(1, Ordering::Relaxed);
            tracing::info!(engine, "engine retired");
        }
    }

    fn publish_gauges(&self) {
        self.metrics
            .miners
            .store(self.miners.len() as u64, Ordering::Relaxed);
        self.metrics.engines.store(
            self.engines.values().filter(|link| !link.retired).count() as u64,
            Ordering::Relaxed,
        );
        let orphaned = self.miners.values().filter(|m| m.current.is_none()).count();
        let on_draining = self
            .miners
            .values()
            .filter(|m| {
                m.current.is_some_and(|engine| {
                    self.engines
                        .get(&engine)
                        .is_some_and(|link| link.role == Some(GatewayRole::Draining))
                })
            })
            .count();
        self.metrics
            .miners_on_draining_engines
            .store(on_draining as u64, Ordering::Relaxed);
        self.metrics
            .miners_without_engine
            .store(orphaned as u64, Ordering::Relaxed);
    }
}

fn is_live(engines: &HashMap<u64, EngineLink>, engine: u64) -> bool {
    engines.get(&engine).is_some_and(|link| !link.retired)
}

/// Opens a virtual connection for `miner` on `engine` unless one is open.
fn open_on(engines: &HashMap<u64, EngineLink>, miner: &mut Miner, conn: u64, engine: u64) -> bool {
    if miner.opened.contains(&engine) {
        return true;
    }
    let Some(link) = engines.get(&engine).filter(|link| !link.retired) else {
        return false;
    };
    let open = Frame::new(FrameKind::Open, conn, miner.peer.to_string().into_bytes());
    if link.tx.try_send(open).is_err() {
        return false;
    }
    miner.opened.insert(engine);
    true
}

fn send_engine(engines: &HashMap<u64, EngineLink>, engine: u64, frame: Frame) {
    if let Some(link) = engines.get(&engine) {
        let _ = link.tx.try_send(frame);
    }
}

/// A miner whose socket cannot keep up is disconnected, as Stratum does.
fn send_miner(miner: &Miner, out: MinerOut) {
    if miner.tx.try_send(out).is_err() {
        let _ = miner.tx.try_send(MinerOut::Close);
    }
}

fn role_frame(role: GatewayRole) -> Frame {
    Frame::new(FrameKind::Role, 0, role.as_str().as_bytes().to_vec())
}

/// The miner's login request with the gateway's reserved request ID.
fn replay_login_line(login_line: &str) -> Option<String> {
    let mut value = serde_json::from_str::<Value>(login_line).ok()?;
    value
        .as_object_mut()?
        .insert("id".to_string(), Value::from(REPLAY_LOGIN_REQUEST_ID));
    Some(value.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Harness {
        gateway: Gateway,
        now: Instant,
        metrics: Arc<GatewayMetrics>,
    }

    struct FakeEngine {
        id: u64,
        rx: mpsc::Receiver<Frame>,
    }

    struct FakeMiner {
        conn: u64,
        rx: mpsc::Receiver<MinerOut>,
    }

    impl Harness {
        fn new() -> Self {
            let metrics = Arc::new(GatewayMetrics::default());
            Self {
                gateway: Gateway::new(Timings::default(), Arc::clone(&metrics)),
                now: Instant::now(),
                metrics,
            }
        }

        fn handle(&mut self, event: Event) {
            self.gateway.handle(event, self.now);
        }

        fn advance(&mut self, by: Duration) {
            self.now += by;
            self.handle(Event::Tick);
        }

        fn engine(&mut self, id: u64, started_at_ms: u64) -> FakeEngine {
            let (tx, rx) = mpsc::channel(1024);
            self.handle(Event::EngineConnected { engine: id, tx });
            let hello = EngineHello {
                instance: format!("e{id}"),
                pid: id as u32,
                started_at_ms,
            };
            self.handle(Event::EngineFrame {
                engine: id,
                frame: Frame::new(FrameKind::Hello, 0, serde_json::to_vec(&hello).unwrap()),
            });
            FakeEngine { id, rx }
        }

        fn miner(&mut self, conn: u64) -> FakeMiner {
            let (tx, rx) = mpsc::channel(1024);
            self.handle(Event::MinerConnected {
                conn,
                peer: format!("203.0.113.{conn}:4000").parse().unwrap(),
                tx,
            });
            FakeMiner { conn, rx }
        }

        fn miner_says(&mut self, miner: &FakeMiner, line: &str) {
            self.handle(Event::MinerLine {
                conn: miner.conn,
                line: line.to_string(),
            });
        }

        fn engine_says(&mut self, engine: &FakeEngine, conn: u64, line: &str) {
            self.handle(Event::EngineFrame {
                engine: engine.id,
                frame: Frame::new(FrameKind::Line, conn, line.as_bytes().to_vec()),
            });
        }

        /// Logs a miner in on `engine` and gives it job `job`.
        fn logged_in_miner(&mut self, conn: u64, engine: &mut FakeEngine, job: &str) -> FakeMiner {
            let mut miner = self.miner(conn);
            self.miner_says(&miner, LOGIN);
            engine.expect(FrameKind::Open, conn);
            assert_eq!(engine.expect(FrameKind::Line, conn), LOGIN);
            self.engine_says(engine, conn, r#"{"id":1,"result":{"protocol_version":2}}"#);
            self.engine_says(engine, conn, &job_line(job));
            assert_eq!(miner.line(), r#"{"id":1,"result":{"protocol_version":2}}"#);
            assert_eq!(miner.line(), job_line(job));
            miner
        }
    }

    const LOGIN: &str = r#"{"id":1,"method":"login","params":{"address":"addr","worker":"rig"}}"#;

    fn job_line(job: &str) -> String {
        format!(r#"{{"method":"job","params":{{"job_id":"{job}"}}}}"#)
    }

    fn submit_line(id: u64, job: &str) -> String {
        format!(r#"{{"id":{id},"method":"submit","params":{{"job_id":"{job}","nonce":7}}}}"#)
    }

    impl FakeEngine {
        fn next(&mut self) -> Option<Frame> {
            self.rx.try_recv().ok()
        }

        fn drain_roles(&mut self) -> Vec<String> {
            let mut roles = Vec::new();
            let mut rest = Vec::new();
            while let Some(frame) = self.next() {
                if frame.kind == FrameKind::Role {
                    roles.push(frame.payload_str().unwrap().to_string());
                } else {
                    rest.push(frame);
                }
            }
            assert!(rest.is_empty(), "unexpected frames: {rest:?}");
            roles
        }

        fn expect(&mut self, kind: FrameKind, conn: u64) -> String {
            loop {
                let frame = self
                    .next()
                    .unwrap_or_else(|| panic!("expected {kind:?} for {conn}"));
                if frame.kind == FrameKind::Role {
                    continue;
                }
                assert_eq!((frame.kind, frame.conn), (kind, conn), "{frame:?}");
                return frame.payload_str().unwrap().to_string();
            }
        }

        fn assert_quiet(&mut self) {
            let frames: Vec<_> = std::iter::from_fn(|| self.next())
                .filter(|frame| frame.kind != FrameKind::Role)
                .collect();
            assert!(frames.is_empty(), "unexpected frames: {frames:?}");
        }
    }

    impl FakeMiner {
        fn line(&mut self) -> String {
            match self.rx.try_recv() {
                Ok(MinerOut::Line(line)) => line,
                other => panic!("expected a line, got {other:?}"),
            }
        }

        fn assert_quiet(&mut self) {
            assert!(
                self.rx.try_recv().is_err(),
                "miner got an unexpected message"
            );
        }
    }

    #[test]
    fn newest_engine_is_primary_and_older_ones_drain() {
        let mut h = Harness::new();
        let mut a = h.engine(1, 100);
        assert_eq!(a.drain_roles(), vec!["primary"]);
        let mut b = h.engine(2, 200);
        assert_eq!(a.drain_roles(), vec!["draining"]);
        assert_eq!(b.drain_roles(), vec!["primary"]);
        // An older engine reconnecting (e.g. after a gateway restart) drains.
        let mut c = h.engine(3, 50);
        assert_eq!(c.drain_roles(), vec!["draining"]);
        b.assert_quiet();
    }

    #[test]
    fn miners_reach_the_primary_and_submits_follow() {
        let mut h = Harness::new();
        let mut a = h.engine(1, 100);
        let mut miner = h.logged_in_miner(7, &mut a, "j1");
        h.miner_says(&miner, &submit_line(2, "j1"));
        assert_eq!(a.expect(FrameKind::Line, 7), submit_line(2, "j1"));
        h.engine_says(&a, 7, r#"{"id":2,"status":"OK"}"#);
        assert_eq!(miner.line(), r#"{"id":2,"status":"OK"}"#);
    }

    #[test]
    fn keepalives_reach_the_current_engine_at_a_bounded_rate() {
        let mut h = Harness::new();
        let mut a = h.engine(1, 100);
        let mut miner = h.logged_in_miner(7, &mut a, "j1");

        // The login just reached the engine, so it is not idle yet.
        h.handle(Event::MinerKeepalive { conn: 7 });
        a.assert_quiet();

        h.advance(KEEPALIVE_RELAY_INTERVAL);
        h.handle(Event::MinerKeepalive { conn: 7 });
        assert_eq!(a.expect(FrameKind::Line, 7), "");
        h.handle(Event::MinerKeepalive { conn: 7 });
        a.assert_quiet();

        // Any relayed request also counts as activity.
        h.advance(KEEPALIVE_RELAY_INTERVAL);
        h.miner_says(&miner, &submit_line(2, "j1"));
        assert_eq!(a.expect(FrameKind::Line, 7), submit_line(2, "j1"));
        h.handle(Event::MinerKeepalive { conn: 7 });
        a.assert_quiet();
        miner.assert_quiet();
    }

    #[test]
    fn keepalives_before_login_are_not_relayed() {
        let mut h = Harness::new();
        let mut a = h.engine(1, 100);
        let _miner = h.miner(7);
        a.expect(FrameKind::Open, 7);
        h.handle(Event::MinerKeepalive { conn: 7 });
        a.assert_quiet();
    }

    #[test]
    fn handover_replays_logins_and_keeps_old_job_shares_on_the_old_engine() {
        let mut h = Harness::new();
        let mut a = h.engine(1, 100);
        let mut miner = h.logged_in_miner(7, &mut a, "old-job");
        let mut b = h.engine(2, 200);

        h.advance(Duration::from_millis(100));
        b.expect(FrameKind::Open, 7);
        let replay = b.expect(FrameKind::Line, 7);
        let replay: Value = serde_json::from_str(&replay).unwrap();
        assert_eq!(replay["id"], Value::from(REPLAY_LOGIN_REQUEST_ID));
        assert_eq!(replay["params"]["address"], "addr");

        // Until the replay is answered, the miner stays on the old engine.
        h.miner_says(&miner, &submit_line(3, "unknown-job"));
        assert_eq!(a.expect(FrameKind::Line, 7), submit_line(3, "unknown-job"));

        h.engine_says(
            &b,
            7,
            &format!(r#"{{"id":{REPLAY_LOGIN_REQUEST_ID},"result":{{"protocol_version":2}}}}"#),
        );
        miner.assert_quiet();
        h.engine_says(&b, 7, &job_line("new-job"));
        assert_eq!(miner.line(), job_line("new-job"));
        assert_eq!(h.metrics.migrations.load(Ordering::Relaxed), 1);

        // The old engine's jobs for new templates are not forwarded.
        h.engine_says(&a, 7, &job_line("old-engine-newer-job"));
        miner.assert_quiet();

        // A late share for the old job (maybe a block) goes to the old engine;
        // its answer still reaches the miner. New jobs go to the new engine.
        h.miner_says(&miner, &submit_line(4, "old-job"));
        assert_eq!(a.expect(FrameKind::Line, 7), submit_line(4, "old-job"));
        h.engine_says(&a, 7, r#"{"id":4,"status":"OK"}"#);
        assert_eq!(miner.line(), r#"{"id":4,"status":"OK"}"#);
        h.miner_says(&miner, &submit_line(5, "new-job"));
        assert_eq!(b.expect(FrameKind::Line, 7), submit_line(5, "new-job"));
        a.assert_quiet();
    }

    #[test]
    fn drained_engine_is_retired_after_the_grace() {
        let mut h = Harness::new();
        let mut a = h.engine(1, 100);
        let _miner = h.logged_in_miner(7, &mut a, "old-job");
        let mut b = h.engine(2, 200);
        h.advance(Duration::from_millis(100));
        b.expect(FrameKind::Open, 7);
        b.expect(FrameKind::Line, 7);
        h.engine_says(
            &b,
            7,
            &format!(r#"{{"id":{REPLAY_LOGIN_REQUEST_ID},"result":{{}}}}"#),
        );
        // The grace starts at the first tick with no miner left on `a`.
        h.advance(Duration::from_millis(100));

        h.advance(Duration::from_secs(29));
        a.assert_quiet();
        h.advance(Duration::from_secs(2));
        a.expect(FrameKind::Close, 7);
        a.expect(FrameKind::Retire, 0);
        assert_eq!(h.metrics.retires.load(Ordering::Relaxed), 1);

        // After retirement even old-job shares go to the current engine.
        let miner = Harness::miner_ref(7);
        h.miner_says(&miner, &submit_line(9, "old-job"));
        assert_eq!(b.expect(FrameKind::Line, 7), submit_line(9, "old-job"));
    }

    #[test]
    fn miners_wait_without_an_engine_and_return_on_the_next() {
        let mut h = Harness::new();
        let mut a = h.engine(1, 100);
        let mut miner = h.logged_in_miner(7, &mut a, "j1");
        h.handle(Event::EngineClosed { engine: 1 });

        h.miner_says(&miner, &submit_line(2, "j1"));
        let busy: Value = serde_json::from_str(&miner.line()).unwrap();
        assert_eq!(busy["id"], 2);
        assert_eq!(busy["error"], BUSY_ERROR);

        let mut b = h.engine(2, 200);
        h.advance(Duration::from_millis(100));
        b.expect(FrameKind::Open, 7);
        b.expect(FrameKind::Line, 7);
        h.engine_says(
            &b,
            7,
            &format!(r#"{{"id":{REPLAY_LOGIN_REQUEST_ID},"result":{{}}}}"#),
        );
        h.engine_says(&b, 7, &job_line("j2"));
        assert_eq!(miner.line(), job_line("j2"));
        h.miner_says(&miner, &submit_line(3, "j2"));
        assert_eq!(b.expect(FrameKind::Line, 7), submit_line(3, "j2"));
    }

    #[test]
    fn logins_without_an_engine_are_held_until_one_starts() {
        let mut h = Harness::new();
        let mut miner = h.miner(7);
        h.miner_says(&miner, LOGIN);
        miner.assert_quiet();

        let mut a = h.engine(1, 100);
        h.advance(Duration::from_millis(100));
        a.expect(FrameKind::Open, 7);
        assert_eq!(
            a.expect(FrameKind::Line, 7),
            LOGIN,
            "the original login, not a replay"
        );
        h.engine_says(&a, 7, r#"{"id":1,"status":"ok"}"#);
        assert_eq!(miner.line(), r#"{"id":1,"status":"ok"}"#);
        h.engine_says(&a, 7, &job_line("j1"));
        assert_eq!(miner.line(), job_line("j1"));
        h.miner_says(&miner, &submit_line(2, "j1"));
        assert_eq!(a.expect(FrameKind::Line, 7), submit_line(2, "j1"));
    }

    #[test]
    fn miners_are_dropped_when_no_engine_returns() {
        let mut h = Harness::new();
        let mut a = h.engine(1, 100);
        let mut miner = h.logged_in_miner(7, &mut a, "j1");
        h.handle(Event::EngineClosed { engine: 1 });
        h.advance(Duration::from_secs(119));
        miner.assert_quiet();
        h.advance(Duration::from_secs(2));
        assert_eq!(miner.rx.try_recv().unwrap(), MinerOut::Close);
    }

    #[test]
    fn rejected_replay_keeps_the_miner_and_backs_off() {
        let mut h = Harness::new();
        let mut a = h.engine(1, 100);
        let mut miner = h.logged_in_miner(7, &mut a, "j1");
        let mut b = h.engine(2, 200);
        h.advance(Duration::from_millis(100));
        b.expect(FrameKind::Open, 7);
        b.expect(FrameKind::Line, 7);
        h.engine_says(
            &b,
            7,
            &format!(r#"{{"id":{REPLAY_LOGIN_REQUEST_ID},"error":"too many connections"}}"#),
        );
        b.expect(FrameKind::Close, 7);
        miner.assert_quiet();
        assert_eq!(h.metrics.replay_failures.load(Ordering::Relaxed), 1);

        h.miner_says(&miner, &submit_line(2, "j1"));
        assert_eq!(a.expect(FrameKind::Line, 7), submit_line(2, "j1"));

        h.advance(Duration::from_secs(1));
        b.assert_quiet();
        h.advance(Duration::from_secs(5));
        b.expect(FrameKind::Open, 7);
        b.expect(FrameKind::Line, 7);
    }

    #[test]
    fn engine_ending_the_current_session_disconnects_the_miner() {
        let mut h = Harness::new();
        let mut a = h.engine(1, 100);
        let mut miner = h.logged_in_miner(7, &mut a, "j1");
        h.handle(Event::EngineFrame {
            engine: 1,
            frame: Frame::new(FrameKind::Close, 7, Vec::new()),
        });
        assert_eq!(miner.rx.try_recv().unwrap(), MinerOut::Close);
        h.handle(Event::MinerClosed { conn: 7 });
        a.assert_quiet();
    }

    #[test]
    fn miners_that_never_log_in_time_out() {
        let mut h = Harness::new();
        let mut a = h.engine(1, 100);
        let mut miner = h.miner(7);
        a.expect(FrameKind::Open, 7);
        h.advance(Duration::from_secs(31));
        assert_eq!(miner.rx.try_recv().unwrap(), MinerOut::Close);
        a.expect(FrameKind::Close, 7);
    }

    #[test]
    fn a_login_retried_with_a_new_id_is_tracked_and_migrates() {
        let mut h = Harness::new();
        let mut a = h.engine(1, 100);
        let mut miner = h.miner(7);
        a.expect(FrameKind::Open, 7);
        let bad = r#"{"id":1,"method":"login","params":{"address":"typo"}}"#;
        let good = r#"{"id":2,"method":"login","params":{"address":"addr","worker":"rig"}}"#;
        h.miner_says(&miner, bad);
        assert_eq!(a.expect(FrameKind::Line, 7), bad);
        h.engine_says(&a, 7, r#"{"id":1,"error":"invalid login params"}"#);
        assert_eq!(miner.line(), r#"{"id":1,"error":"invalid login params"}"#);
        h.miner_says(&miner, good);
        assert_eq!(a.expect(FrameKind::Line, 7), good);
        h.engine_says(&a, 7, r#"{"id":2,"status":"ok"}"#);
        assert_eq!(miner.line(), r#"{"id":2,"status":"ok"}"#);

        let mut b = h.engine(2, 200);
        h.advance(Duration::from_millis(100));
        b.expect(FrameKind::Open, 7);
        let replay: Value = serde_json::from_str(&b.expect(FrameKind::Line, 7)).unwrap();
        assert_eq!(
            replay["params"]["address"], "addr",
            "replays the accepted login"
        );
        h.engine_says(
            &b,
            7,
            &format!(r#"{{"id":{REPLAY_LOGIN_REQUEST_ID},"status":"ok"}}"#),
        );
        assert_eq!(h.metrics.migrations.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn miners_that_cannot_move_are_reconnected_after_the_drain_deadline() {
        let mut h = Harness::new();
        let mut a = h.engine(1, 100);
        let mut miner = h.miner(7);
        a.expect(FrameKind::Open, 7);
        // Its login is never answered, so it cannot be replayed elsewhere.
        h.miner_says(&miner, LOGIN);
        a.expect(FrameKind::Line, 7);
        let _b = h.engine(2, 200);
        h.advance(Duration::from_secs(60));
        assert_eq!(
            h.metrics.miners_on_draining_engines.load(Ordering::Relaxed),
            1
        );
        miner.assert_quiet();

        h.advance(Duration::from_secs(121));
        assert_eq!(miner.rx.try_recv().unwrap(), MinerOut::Close);
        assert_eq!(h.metrics.forced_reconnects.load(Ordering::Relaxed), 1);
        a.expect(FrameKind::Close, 7);
        h.advance(Duration::from_millis(100));
        h.advance(Duration::from_secs(31));
        a.expect(FrameKind::Retire, 0);
    }

    #[test]
    fn migration_is_rate_limited() {
        let mut h = Harness::new();
        let mut a = h.engine(1, 100);
        let _miners: Vec<_> = (1..=60)
            .map(|conn| h.logged_in_miner(conn, &mut a, "j"))
            .collect();
        let mut b = h.engine(2, 200);
        h.advance(Duration::from_millis(100));
        let replays = std::iter::from_fn(|| b.next())
            .filter(|frame| frame.kind == FrameKind::Line)
            .count();
        // 200 per second, 100 ms ticks.
        assert_eq!(replays, 20);
    }

    impl Harness {
        /// A handle for a miner whose receiver the test no longer needs.
        fn miner_ref(conn: u64) -> FakeMiner {
            let (_tx, rx) = mpsc::channel(1);
            FakeMiner { conn, rx }
        }
    }
}
