//! Stratum gateway: holds miners' TCP connections and relays them to
//! Stratum engines over a unix socket, so engines can be replaced without
//! disconnecting miners (STRATUM_GATEWAY.md).

mod core;
pub mod tls;

use std::collections::BTreeMap;
use std::future::Future;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::Result;
use pool_common::gateway::{read_frame, write_frame, Frame};
use pool_common::latency::LATENCY_BUCKET_BOUNDS_MILLIS;
use socket2::{SockRef, TcpKeepalive};
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream, UnixListener};
use tokio::sync::{mpsc, oneshot};

pub use crate::core::{Event, Gateway, MinerOut, Timings};
pub use crate::tls::TlsCerts;

/// Same request bound as Stratum.
const MAX_MINER_LINE: usize = 8 * 1024;
const EVENT_QUEUE: usize = 65_536;
const MINER_QUEUE: usize = 256;
const ENGINE_QUEUE: usize = 65_536;
/// A miner that has not taken a line for this long is disconnected.
const MINER_WRITE_TIMEOUT: Duration = Duration::from_secs(60);
/// Bound on closing a miner's socket; a TLS close writes to the peer.
const MINER_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);
/// Pause after a failed accept (for example out of file descriptors).
const ACCEPT_RETRY_DELAY: Duration = Duration::from_millis(100);
const TLS_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
/// Keepalive probes on an idle miner socket: first after this idle time,
/// then every interval, dropping the connection after the retries fail.
const MINER_KEEPALIVE_IDLE: Duration = Duration::from_secs(60);
const MINER_KEEPALIVE_INTERVAL: Duration = Duration::from_secs(10);
const MINER_KEEPALIVE_RETRIES: u32 = 3;
/// A miner socket whose sent data stays unacknowledged this long is dropped.
const MINER_USER_TIMEOUT: Duration = Duration::from_secs(60);

#[derive(Debug, Default)]
pub struct GatewayMetrics {
    pub miners: AtomicU64,
    pub miners_without_engine: AtomicU64,
    pub engines: AtomicU64,
    pub migrations: AtomicU64,
    pub replay_failures: AtomicU64,
    pub busy_replies: AtomicU64,
    pub stale_engine_jobs: AtomicU64,
    pub retires: AtomicU64,
    pub rejected_connections: AtomicU64,
    pub miners_on_draining_engines: AtomicU64,
    pub forced_reconnects: AtomicU64,
    /// The miner connection cap (`gateway_max_miners`).
    pub max_miners: AtomicU64,
    /// Miners connected over TLS, included in `miners`.
    pub tls_miners: AtomicU64,
    pub tls_handshake_failures: AtomicU64,
    /// Relay latency by lane: event-loop queue wait and handling per event
    /// kind, and miner socket writes.
    pub latency: LatencyHistograms,
}

/// Process-lifetime latency histograms keyed by lane, rendered as one
/// Prometheus histogram with the pool's shared bucket bounds.
#[derive(Debug, Default)]
pub struct LatencyHistograms {
    lanes: Mutex<BTreeMap<&'static str, LatencyHistogram>>,
}

#[derive(Debug)]
struct LatencyHistogram {
    counts: [u64; LATENCY_BUCKET_BOUNDS_MILLIS.len() + 1],
    sum_micros: u64,
}

impl LatencyHistograms {
    pub fn record(&self, lane: &'static str, elapsed: Duration) {
        let micros = elapsed.as_micros().min(u64::MAX as u128) as u64;
        let idx = LATENCY_BUCKET_BOUNDS_MILLIS
            .partition_point(|bound| bound.saturating_mul(1_000) < micros);
        let mut lanes = self.lanes.lock().unwrap_or_else(|err| err.into_inner());
        let histogram = lanes.entry(lane).or_insert(LatencyHistogram {
            counts: [0; LATENCY_BUCKET_BOUNDS_MILLIS.len() + 1],
            sum_micros: 0,
        });
        histogram.counts[idx] = histogram.counts[idx].saturating_add(1);
        histogram.sum_micros = histogram.sum_micros.saturating_add(micros);
    }

    fn render(&self, out: &mut String, name: &str) {
        let lanes = self.lanes.lock().unwrap_or_else(|err| err.into_inner());
        for (lane, histogram) in lanes.iter() {
            let mut cumulative = 0u64;
            for (idx, count) in histogram.counts.iter().enumerate() {
                cumulative = cumulative.saturating_add(*count);
                let le = LATENCY_BUCKET_BOUNDS_MILLIS
                    .get(idx)
                    .map(u64::to_string)
                    .unwrap_or_else(|| "+Inf".to_string());
                out.push_str(&format!(
                    "{name}_bucket{{lane=\"{lane}\",le=\"{le}\"}} {cumulative}\n"
                ));
            }
            out.push_str(&format!(
                "{name}_sum{{lane=\"{lane}\"}} {}\n",
                histogram.sum_micros as f64 / 1_000.0
            ));
            out.push_str(&format!("{name}_count{{lane=\"{lane}\"}} {cumulative}\n"));
        }
    }
}

impl GatewayMetrics {
    pub fn render(&self) -> String {
        let mut out = String::new();
        for (name, value) in [
            ("blocknet_pool_gateway_miners", &self.miners),
            (
                "blocknet_pool_gateway_miners_without_engine",
                &self.miners_without_engine,
            ),
            ("blocknet_pool_gateway_engines", &self.engines),
            ("blocknet_pool_gateway_migrations_total", &self.migrations),
            (
                "blocknet_pool_gateway_replay_failures_total",
                &self.replay_failures,
            ),
            (
                "blocknet_pool_gateway_busy_replies_total",
                &self.busy_replies,
            ),
            (
                "blocknet_pool_gateway_stale_engine_jobs_total",
                &self.stale_engine_jobs,
            ),
            ("blocknet_pool_gateway_retires_total", &self.retires),
            (
                "blocknet_pool_gateway_rejected_connections_total",
                &self.rejected_connections,
            ),
            (
                "blocknet_pool_gateway_miners_on_draining_engines",
                &self.miners_on_draining_engines,
            ),
            (
                "blocknet_pool_gateway_forced_reconnects_total",
                &self.forced_reconnects,
            ),
            ("blocknet_pool_gateway_max_miners", &self.max_miners),
            ("blocknet_pool_gateway_tls_miners", &self.tls_miners),
            (
                "blocknet_pool_gateway_tls_handshake_failures_total",
                &self.tls_handshake_failures,
            ),
        ] {
            out.push_str(&format!("{name} {}\n", value.load(Ordering::Relaxed)));
        }
        // Each miner holds a descriptor; running out stops accepting miners.
        if let Ok(fds) = std::fs::read_dir("/proc/self/fd") {
            out.push_str(&format!("blocknet_pool_gateway_open_fds {}\n", fds.count()));
        }
        if let Some(limit) = open_files_limit() {
            out.push_str(&format!("blocknet_pool_gateway_open_fds_limit {limit}\n"));
        }
        self.latency
            .render(&mut out, "blocknet_pool_gateway_latency_millis");
        out
    }
}

/// An event stamped when its reader task produced it, so the core loop's
/// queue wait is measurable.
struct TimedEvent {
    at: Instant,
    event: Event,
}

impl TimedEvent {
    fn now(event: Event) -> Self {
        Self {
            at: Instant::now(),
            event,
        }
    }
}

/// Latency lanes (queue wait, handling) for the event kinds on the relay path.
fn event_lanes(event: &Event) -> Option<(&'static str, &'static str)> {
    match event {
        Event::EngineFrame { .. } => Some(("engine_frame_wait", "engine_frame_handle")),
        Event::MinerLine { .. } => Some(("miner_line_wait", "miner_line_handle")),
        _ => None,
    }
}

/// The soft "Max open files" limit of this process.
fn open_files_limit() -> Option<u64> {
    let limits = std::fs::read_to_string("/proc/self/limits").ok()?;
    let line = limits.lines().find(|l| l.starts_with("Max open files"))?;
    line["Max open files".len()..]
        .split_whitespace()
        .next()?
        .parse()
        .ok()
}

pub struct GatewayListeners {
    pub miners: TcpListener,
    pub engines: UnixListener,
}

/// The optional `stratum+ssl` listener: TLS over TCP, then the same lines
/// as plain Stratum.
pub struct TlsListener {
    pub listener: TcpListener,
    pub certs: Arc<TlsCerts>,
}

/// Runs the gateway until a listener fails.
pub async fn run(
    listeners: GatewayListeners,
    timings: Timings,
    max_miners: usize,
    metrics: Arc<GatewayMetrics>,
) -> Result<()> {
    run_with_tls(listeners, None, timings, max_miners, metrics).await
}

/// [`run`], also accepting TLS miners on `tls`. They share the relay,
/// connection cap and metrics with plain miners.
pub async fn run_with_tls(
    listeners: GatewayListeners,
    tls: Option<TlsListener>,
    timings: Timings,
    max_miners: usize,
    metrics: Arc<GatewayMetrics>,
) -> Result<()> {
    let (events_tx, mut events_rx) = mpsc::channel::<TimedEvent>(EVENT_QUEUE);
    let tick = timings.tick;
    let mut gateway = Gateway::new(timings, Arc::clone(&metrics));
    metrics
        .max_miners
        .store(max_miners as u64, Ordering::Relaxed);

    // Connection IDs are unique across both miner listeners.
    let conns = Arc::new(AtomicU64::new(1));
    let miners = tokio::spawn(accept_miners(
        listeners.miners,
        None,
        Arc::clone(&conns),
        events_tx.clone(),
        max_miners,
        Arc::clone(&metrics),
    ));
    let tls_tasks = tls.map(|tls| {
        let watch = tokio::spawn(tls::watch(Arc::clone(&tls.certs), tls::CERT_CHECK_INTERVAL));
        let accept = tokio::spawn(accept_miners(
            tls.listener,
            Some(tls.certs),
            conns,
            events_tx.clone(),
            max_miners,
            Arc::clone(&metrics),
        ));
        (watch, accept)
    });
    let engines = tokio::spawn(accept_engines(listeners.engines, events_tx.clone()));
    let ticker = tokio::spawn(async move {
        let mut interval = tokio::time::interval(tick);
        loop {
            interval.tick().await;
            if events_tx.send(TimedEvent::now(Event::Tick)).await.is_err() {
                return;
            }
        }
    });

    while let Some(TimedEvent { at, event }) = events_rx.recv().await {
        let now = Instant::now();
        let lanes = event_lanes(&event);
        gateway.handle(event, now);
        if let Some((wait_lane, handle_lane)) = lanes {
            metrics
                .latency
                .record(wait_lane, now.saturating_duration_since(at));
            metrics.latency.record(handle_lane, now.elapsed());
        }
        if miners.is_finished() || engines.is_finished() {
            break;
        }
    }
    ticker.abort();
    if let Some((watch, accept)) = tls_tasks {
        watch.abort();
        accept.abort();
    }
    let miners = miners.await;
    let engines = engines.await;
    match (miners, engines) {
        (Ok(Err(err)), _) | (_, Ok(Err(err))) => Err(err),
        _ => Ok(()),
    }
}

/// Accepts miners on one listener; with `tls`, each completes a TLS
/// handshake first, in its own task so a slow peer cannot stall accepts.
async fn accept_miners(
    listener: TcpListener,
    tls: Option<Arc<TlsCerts>>,
    conns: Arc<AtomicU64>,
    events: mpsc::Sender<TimedEvent>,
    max_miners: usize,
    metrics: Arc<GatewayMetrics>,
) -> Result<()> {
    loop {
        let (stream, peer) = match listener.accept().await {
            Ok(accepted) => accepted,
            Err(err) => {
                tracing::warn!(error = %err, tls = tls.is_some(), "miner accept failed");
                tokio::time::sleep(ACCEPT_RETRY_DELAY).await;
                continue;
            }
        };
        if metrics.miners.load(Ordering::Relaxed) as usize >= max_miners {
            metrics.rejected_connections.fetch_add(1, Ordering::Relaxed);
            continue;
        }
        configure_miner_socket(&stream);
        let conn = conns.fetch_add(1, Ordering::Relaxed);
        let Some(certs) = &tls else {
            let (reader, writer) = stream.into_split();
            let Some(read) = start_miner(conn, peer, reader, writer, &events, &metrics).await
            else {
                return Ok(());
            };
            tokio::spawn(read);
            continue;
        };
        if events.is_closed() {
            return Ok(());
        }
        let acceptor = certs.acceptor();
        let events = events.clone();
        let metrics = Arc::clone(&metrics);
        tokio::spawn(async move {
            let stream =
                match tokio::time::timeout(TLS_HANDSHAKE_TIMEOUT, acceptor.accept(stream)).await {
                    Ok(Ok(stream)) => stream,
                    Ok(Err(err)) => {
                        metrics
                            .tls_handshake_failures
                            .fetch_add(1, Ordering::Relaxed);
                        tracing::debug!(peer = %peer, error = %err, "miner TLS handshake failed");
                        return;
                    }
                    Err(_) => {
                        metrics
                            .tls_handshake_failures
                            .fetch_add(1, Ordering::Relaxed);
                        tracing::debug!(peer = %peer, "miner TLS handshake timed out");
                        return;
                    }
                };
            let (reader, writer) = tokio::io::split(stream);
            let Some(read) = start_miner(conn, peer, reader, writer, &events, &metrics).await
            else {
                return;
            };
            metrics.tls_miners.fetch_add(1, Ordering::Relaxed);
            read.await;
            metrics.tls_miners.fetch_sub(1, Ordering::Relaxed);
        });
    }
}

/// Bounds how long a miner that vanished without closing (no FIN or RST)
/// holds its descriptor and slot: keepalive probes an idle connection, and
/// the user timeout drops one whose sent data goes unacknowledged.
fn configure_miner_socket(stream: &TcpStream) {
    let _ = stream.set_nodelay(true);
    let socket = SockRef::from(stream);
    let keepalive = TcpKeepalive::new().with_time(MINER_KEEPALIVE_IDLE);
    #[cfg(target_os = "linux")]
    let keepalive = keepalive
        .with_interval(MINER_KEEPALIVE_INTERVAL)
        .with_retries(MINER_KEEPALIVE_RETRIES);
    if let Err(err) = socket.set_tcp_keepalive(&keepalive) {
        tracing::debug!(error = %err, "miner keepalive not set");
    }
    #[cfg(target_os = "linux")]
    if let Err(err) = socket.set_tcp_user_timeout(Some(MINER_USER_TIMEOUT)) {
        tracing::debug!(error = %err, "miner TCP user timeout not set");
    }
}

/// Registers an accepted miner with the relay and starts its writer.
/// Returns the miner's reader loop, or None once the relay has stopped.
async fn start_miner<R, W>(
    conn: u64,
    peer: SocketAddr,
    reader: R,
    mut writer: W,
    events: &mpsc::Sender<TimedEvent>,
    metrics: &Arc<GatewayMetrics>,
) -> Option<impl Future<Output = ()> + Send + 'static>
where
    R: AsyncRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send + 'static,
{
    let (tx, mut rx) = mpsc::channel::<MinerOut>(MINER_QUEUE);
    if events
        .send(TimedEvent::now(Event::MinerConnected { conn, peer, tx }))
        .await
        .is_err()
    {
        return None;
    }
    // The reader holds the socket open until it ends, so the writer
    // ending (Close, a failed or stalled write) must end the reader too;
    // otherwise a peer that never closes keeps the descriptor forever.
    let (closed_tx, closed_rx) = oneshot::channel::<()>();
    let write_metrics = Arc::clone(metrics);
    tokio::spawn(async move {
        while let Some(out) = rx.recv().await {
            match out {
                MinerOut::Line(line) => {
                    let mut bytes = line.into_bytes();
                    bytes.push(b'\n');
                    let write_started_at = Instant::now();
                    match tokio::time::timeout(MINER_WRITE_TIMEOUT, writer.write_all(&bytes)).await
                    {
                        Ok(Ok(())) => {}
                        Ok(Err(_)) | Err(_) => break,
                    }
                    write_metrics
                        .latency
                        .record("miner_write", write_started_at.elapsed());
                }
                MinerOut::Close => break,
            }
        }
        let _ = tokio::time::timeout(MINER_SHUTDOWN_TIMEOUT, writer.shutdown()).await;
        drop(closed_tx);
    });
    Some(read_miner(conn, peer, reader, closed_rx, events.clone()))
}

async fn read_miner<R: AsyncRead + Unpin>(
    conn: u64,
    peer: SocketAddr,
    reader: R,
    mut closed: oneshot::Receiver<()>,
    events: mpsc::Sender<TimedEvent>,
) {
    let mut reader = BufReader::new(reader);
    let mut line = Vec::with_capacity(256);
    loop {
        line.clear();
        let mut limited = (&mut reader).take(MAX_MINER_LINE as u64 + 1);
        let read = tokio::select! {
            read = limited.read_until(b'\n', &mut line) => read,
            _ = &mut closed => break,
        };
        match read {
            Ok(0) => break,
            Ok(_) if line.len() > MAX_MINER_LINE => {
                tracing::warn!(peer = %peer, "miner request exceeds {MAX_MINER_LINE} bytes");
                break;
            }
            Ok(_) => {
                while matches!(line.last(), Some(b'\n' | b'\r')) {
                    line.pop();
                }
                let Ok(text) = String::from_utf8(line.clone()) else {
                    break;
                };
                let event = if text.trim().is_empty() {
                    Event::MinerKeepalive { conn }
                } else {
                    Event::MinerLine { conn, line: text }
                };
                if events.send(TimedEvent::now(event)).await.is_err() {
                    return;
                }
            }
            Err(_) => break,
        }
    }
    let _ = events
        .send(TimedEvent::now(Event::MinerClosed { conn }))
        .await;
}

async fn accept_engines(listener: UnixListener, events: mpsc::Sender<TimedEvent>) -> Result<()> {
    let mut next_engine = 1u64;
    loop {
        let (stream, _) = listener.accept().await?;
        let engine = next_engine;
        next_engine += 1;
        let (mut reader, mut writer) = stream.into_split();
        let (tx, mut rx) = mpsc::channel::<Frame>(ENGINE_QUEUE);
        if events
            .send(TimedEvent::now(Event::EngineConnected { engine, tx }))
            .await
            .is_err()
        {
            return Ok(());
        }
        tokio::spawn(async move {
            while let Some(frame) = rx.recv().await {
                if write_frame(&mut writer, &frame).await.is_err() {
                    break;
                }
            }
        });
        let events = events.clone();
        tokio::spawn(async move {
            loop {
                match read_frame(&mut reader).await {
                    Ok(Some(frame)) => {
                        if events
                            .send(TimedEvent::now(Event::EngineFrame { engine, frame }))
                            .await
                            .is_err()
                        {
                            return;
                        }
                    }
                    Ok(None) => break,
                    Err(err) => {
                        tracing::warn!(engine, error = %err, "engine link failed");
                        break;
                    }
                }
            }
            let _ = events
                .send(TimedEvent::now(Event::EngineClosed { engine }))
                .await;
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pool_common::gateway::FrameKind;
    use std::path::{Path, PathBuf};
    use tokio::net::UnixStream;
    use tokio_rustls::rustls::crypto::ring;
    use tokio_rustls::rustls::pki_types::{CertificateDer, ServerName};
    use tokio_rustls::rustls::{ClientConfig, RootCertStore};
    use tokio_rustls::TlsConnector;

    /// Writes a fresh self-signed certificate for `localhost` and its key to
    /// `cert` and `key` (via a rename, as a renewal would replace them).
    fn write_self_signed(cert: &Path, key: &Path) -> CertificateDer<'static> {
        let generated = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
        for (path, pem) in [
            (cert, generated.cert.pem()),
            (key, generated.key_pair.serialize_pem()),
        ] {
            let tmp = path.with_extension("tmp");
            std::fs::write(&tmp, pem).unwrap();
            std::fs::rename(&tmp, path).unwrap();
        }
        generated.cert.der().clone()
    }

    fn cert_paths(dir: &Path) -> (PathBuf, PathBuf) {
        (dir.join("fullchain.pem"), dir.join("privkey.pem"))
    }

    /// A client that trusts only `cert`.
    fn tls_client(cert: &CertificateDer<'static>) -> TlsConnector {
        let mut roots = RootCertStore::empty();
        roots.add(cert.clone()).unwrap();
        let config = ClientConfig::builder_with_provider(Arc::new(ring::default_provider()))
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_root_certificates(roots)
            .with_no_client_auth();
        TlsConnector::from(Arc::new(config))
    }

    async fn tls_connect(
        addr: SocketAddr,
        cert: &CertificateDer<'static>,
    ) -> std::io::Result<tokio_rustls::client::TlsStream<tokio::net::TcpStream>> {
        let tcp = tokio::net::TcpStream::connect(addr).await?;
        tls_client(cert)
            .connect(ServerName::try_from("localhost").unwrap(), tcp)
            .await
    }

    async fn wait_until(what: &str, ready: impl Fn() -> bool) {
        for _ in 0..500 {
            if ready() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("timed out waiting for {what}");
    }

    async fn next_frame(engine: &mut UnixStream) -> Frame {
        tokio::time::timeout(Duration::from_secs(5), read_frame(engine))
            .await
            .expect("engine frame")
            .unwrap()
            .expect("gateway open")
    }

    /// The next frame other than a role change.
    async fn next_miner_frame(engine: &mut UnixStream) -> Frame {
        loop {
            let frame = next_frame(engine).await;
            if frame.kind != FrameKind::Role {
                return frame;
            }
        }
    }

    #[tokio::test]
    async fn tls_miner_is_relayed_like_a_plain_miner() {
        let dir = tempfile::tempdir().unwrap();
        let (cert_path, key_path) = cert_paths(dir.path());
        let cert = write_self_signed(&cert_path, &key_path);
        let socket = dir.path().join("gateway.sock");
        let miners = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let plain_addr = miners.local_addr().unwrap();
        let tls_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let tls_addr = tls_listener.local_addr().unwrap();
        let metrics = Arc::new(GatewayMetrics::default());
        let gateway = tokio::spawn(run_with_tls(
            GatewayListeners {
                miners,
                engines: UnixListener::bind(&socket).unwrap(),
            },
            Some(TlsListener {
                listener: tls_listener,
                certs: Arc::new(TlsCerts::load(&cert_path, &key_path).unwrap()),
            }),
            Timings {
                tick: Duration::from_millis(20),
                ..Timings::default()
            },
            16,
            Arc::clone(&metrics),
        ));

        let mut engine = UnixStream::connect(&socket).await.unwrap();
        let hello = r#"{"instance":"a","pid":1,"started_at_ms":0}"#;
        write_frame(&mut engine, &Frame::new(FrameKind::Hello, 0, hello))
            .await
            .unwrap();
        let role = next_frame(&mut engine).await;
        assert_eq!(role.kind, FrameKind::Role);
        assert_eq!(role.payload_str().unwrap(), "primary");

        type Reader = BufReader<Box<dyn AsyncRead + Unpin + Send>>;
        type Writer = Box<dyn AsyncWrite + Unpin + Send>;
        let mut opened: Vec<(u64, Reader, Writer)> = Vec::new();
        for tls in [false, true] {
            let (reader, writer): (Box<dyn AsyncRead + Unpin + Send>, Writer) = if tls {
                let (reader, writer) =
                    tokio::io::split(tls_connect(tls_addr, &cert).await.unwrap());
                (Box::new(reader), Box::new(writer))
            } else {
                let (reader, writer) = tokio::net::TcpStream::connect(plain_addr)
                    .await
                    .unwrap()
                    .into_split();
                (Box::new(reader), Box::new(writer))
            };
            let (mut reader, mut writer) = (BufReader::new(reader), writer);
            let request = format!(r#"{{"id":7,"method":"getjob","params":{{"tls":{tls}}}}}"#);
            writer
                .write_all(format!("{request}\n").as_bytes())
                .await
                .unwrap();
            let open = next_miner_frame(&mut engine).await;
            assert_eq!(open.kind, FrameKind::Open);
            assert!(open.payload_str().unwrap().starts_with("127.0.0.1:"));
            let line = next_miner_frame(&mut engine).await;
            assert_eq!((line.kind, line.conn), (FrameKind::Line, open.conn));
            assert_eq!(line.payload_str().unwrap(), request.as_str());

            let response = format!(r#"{{"id":7,"result":{{"conn":{}}}}}"#, open.conn);
            write_frame(
                &mut engine,
                &Frame::new(FrameKind::Line, open.conn, response.clone()),
            )
            .await
            .unwrap();
            let mut got = String::new();
            tokio::time::timeout(Duration::from_secs(5), reader.read_line(&mut got))
                .await
                .expect("miner reply")
                .unwrap();
            assert_eq!(got.trim_end(), response);
            opened.push((open.conn, reader, writer));
        }
        assert_ne!(opened[0].0, opened[1].0, "connection IDs are unique");
        wait_until("both miners counted", || {
            metrics.miners.load(Ordering::Relaxed) == 2
        })
        .await;
        assert_eq!(metrics.tls_miners.load(Ordering::Relaxed), 1);

        // The TLS miner leaving closes its virtual connection on the engine.
        let (tls_conn, ..) = opened.pop().unwrap();
        let close = next_miner_frame(&mut engine).await;
        assert_eq!((close.kind, close.conn), (FrameKind::Close, tls_conn));
        wait_until("TLS miner released", || {
            metrics.tls_miners.load(Ordering::Relaxed) == 0
        })
        .await;
        assert!(metrics
            .render()
            .contains("blocknet_pool_gateway_tls_miners 0\n"));
        gateway.abort();
    }

    #[tokio::test]
    async fn failed_tls_handshakes_are_counted_and_the_listener_keeps_accepting() {
        let dir = tempfile::tempdir().unwrap();
        let (cert_path, key_path) = cert_paths(dir.path());
        let cert = write_self_signed(&cert_path, &key_path);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let metrics = Arc::new(GatewayMetrics::default());
        let (events_tx, mut events_rx) = mpsc::channel(16);
        let accept = tokio::spawn(accept_miners(
            listener,
            Some(Arc::new(TlsCerts::load(&cert_path, &key_path).unwrap())),
            Arc::new(AtomicU64::new(1)),
            events_tx,
            16,
            Arc::clone(&metrics),
        ));

        // Plaintext Stratum on the TLS port.
        let mut plaintext = tokio::net::TcpStream::connect(addr).await.unwrap();
        plaintext
            .write_all(b"{\"id\":1,\"method\":\"login\"}\n")
            .await
            .unwrap();
        // A client that does not trust the certificate.
        let other_dir = tempfile::tempdir().unwrap();
        let (other_cert, other_key) = cert_paths(other_dir.path());
        let untrusted = write_self_signed(&other_cert, &other_key);
        assert!(tls_connect(addr, &untrusted).await.is_err());
        wait_until("both handshakes fail", || {
            metrics.tls_handshake_failures.load(Ordering::Relaxed) == 2
        })
        .await;
        assert!(metrics
            .render()
            .contains("blocknet_pool_gateway_tls_handshake_failures_total 2\n"));

        let _client = tls_connect(addr, &cert).await.unwrap();
        let connected = tokio::time::timeout(Duration::from_secs(5), events_rx.recv())
            .await
            .expect("TLS miner connected")
            .unwrap();
        assert!(matches!(connected.event, Event::MinerConnected { .. }));
        accept.abort();
    }

    #[tokio::test]
    async fn tls_certificate_reloads_when_its_files_change() {
        let dir = tempfile::tempdir().unwrap();
        let (cert_path, key_path) = cert_paths(dir.path());
        let old = write_self_signed(&cert_path, &key_path);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let certs = Arc::new(TlsCerts::load(&cert_path, &key_path).unwrap());
        let (events_tx, mut events_rx) = mpsc::channel(16);
        let accept = tokio::spawn(accept_miners(
            listener,
            Some(Arc::clone(&certs)),
            Arc::new(AtomicU64::new(1)),
            events_tx,
            16,
            Arc::new(GatewayMetrics::default()),
        ));
        let drain = tokio::spawn(async move { while events_rx.recv().await.is_some() {} });
        assert!(!certs.reload_if_changed().unwrap(), "unchanged files");
        let _old_client = tls_connect(addr, &old).await.unwrap();

        // A broken file is rejected and the current certificate stays.
        std::fs::write(&cert_path, "not a certificate").unwrap();
        assert!(certs.reload_if_changed().is_err());
        let _still_old = tls_connect(addr, &old).await.unwrap();

        let new = write_self_signed(&cert_path, &key_path);
        let watch = tokio::spawn(tls::watch(Arc::clone(&certs), Duration::from_millis(20)));
        let mut reloaded = false;
        for _ in 0..250 {
            if tls_connect(addr, &new).await.is_ok() {
                reloaded = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(reloaded, "the new certificate is served");
        assert!(tls_connect(addr, &old).await.is_err());
        watch.abort();
        accept.abort();
        drain.abort();
    }

    #[tokio::test]
    async fn miner_sockets_detect_dead_peers() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let _client = tokio::net::TcpStream::connect(listener.local_addr().unwrap())
            .await
            .unwrap();
        let (stream, _) = listener.accept().await.unwrap();
        configure_miner_socket(&stream);
        let socket = SockRef::from(&stream);
        assert!(socket.keepalive().unwrap());
        assert!(stream.nodelay().unwrap());
        #[cfg(target_os = "linux")]
        {
            assert_eq!(socket.tcp_keepalive_time().unwrap(), MINER_KEEPALIVE_IDLE);
            assert_eq!(
                socket.tcp_keepalive_interval().unwrap(),
                MINER_KEEPALIVE_INTERVAL
            );
            assert_eq!(
                socket.tcp_keepalive_retries().unwrap(),
                MINER_KEEPALIVE_RETRIES
            );
            assert_eq!(socket.tcp_user_timeout().unwrap(), Some(MINER_USER_TIMEOUT));
        }
    }

    #[tokio::test]
    async fn closing_a_miner_releases_its_socket_while_the_peer_stays_open() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (events_tx, mut events_rx) = mpsc::channel(16);
        let accept = tokio::spawn(accept_miners(
            listener,
            None,
            Arc::new(AtomicU64::new(1)),
            events_tx,
            16,
            Arc::new(GatewayMetrics::default()),
        ));
        let mut client = tokio::net::TcpStream::connect(addr).await.unwrap();
        let tx = match events_rx.recv().await.unwrap().event {
            Event::MinerConnected { tx, .. } => tx,
            other => panic!("unexpected event {other:?}"),
        };
        tx.send(MinerOut::Close).await.unwrap();

        // The client never closes its side; the gateway must still end the
        // connection rather than wait on the read half.
        let closed = tokio::time::timeout(Duration::from_secs(5), events_rx.recv())
            .await
            .expect("reader ended")
            .unwrap();
        assert!(matches!(closed.event, Event::MinerClosed { .. }));
        let mut buf = [0u8; 1];
        assert_eq!(client.read(&mut buf).await.unwrap(), 0);
        accept.abort();
    }

    #[test]
    fn metrics_report_the_cap_and_descriptor_use() {
        let metrics = GatewayMetrics::default();
        metrics.max_miners.store(32_768, Ordering::Relaxed);
        let out = metrics.render();
        assert!(out.contains("blocknet_pool_gateway_max_miners 32768\n"));
        assert!(out.contains("blocknet_pool_gateway_open_fds "));
        assert!(open_files_limit().is_some_and(|limit| limit > 0));
    }

    #[test]
    fn latency_histograms_render_cumulative_prometheus_buckets() {
        let metrics = GatewayMetrics::default();
        metrics
            .latency
            .record("miner_write", Duration::from_micros(400));
        metrics
            .latency
            .record("miner_write", Duration::from_millis(3));
        let out = metrics.render();
        assert!(out.contains(
            "blocknet_pool_gateway_latency_millis_bucket{lane=\"miner_write\",le=\"1\"} 1\n"
        ));
        assert!(out.contains(
            "blocknet_pool_gateway_latency_millis_bucket{lane=\"miner_write\",le=\"5\"} 2\n"
        ));
        assert!(out.contains(
            "blocknet_pool_gateway_latency_millis_bucket{lane=\"miner_write\",le=\"+Inf\"} 2\n"
        ));
        assert!(
            out.contains("blocknet_pool_gateway_latency_millis_sum{lane=\"miner_write\"} 3.4\n")
        );
        assert!(
            out.contains("blocknet_pool_gateway_latency_millis_count{lane=\"miner_write\"} 2\n")
        );
    }
}
