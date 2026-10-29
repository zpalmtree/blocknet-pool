//! Stratum gateway: holds miners' TCP connections and relays them to
//! Stratum engines over a unix socket, so engines can be replaced without
//! disconnecting miners (STRATUM_GATEWAY.md).

mod core;

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::Result;
use pool_common::gateway::{read_frame, write_frame, Frame};
use pool_common::latency::LATENCY_BUCKET_BOUNDS_MILLIS;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, UnixListener};
use tokio::sync::{mpsc, oneshot};

pub use crate::core::{Event, Gateway, MinerOut, Timings};

/// Same request bound as Stratum.
const MAX_MINER_LINE: usize = 8 * 1024;
const EVENT_QUEUE: usize = 65_536;
const MINER_QUEUE: usize = 256;
const ENGINE_QUEUE: usize = 65_536;
/// A miner that has not taken a line for this long is disconnected.
const MINER_WRITE_TIMEOUT: Duration = Duration::from_secs(60);
/// Pause after a failed accept (for example out of file descriptors).
const ACCEPT_RETRY_DELAY: Duration = Duration::from_millis(100);

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
        ] {
            out.push_str(&format!("{name} {}\n", value.load(Ordering::Relaxed)));
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

pub struct GatewayListeners {
    pub miners: TcpListener,
    pub engines: UnixListener,
}

/// Runs the gateway until a listener fails.
pub async fn run(
    listeners: GatewayListeners,
    timings: Timings,
    max_miners: usize,
    metrics: Arc<GatewayMetrics>,
) -> Result<()> {
    let (events_tx, mut events_rx) = mpsc::channel::<TimedEvent>(EVENT_QUEUE);
    let tick = timings.tick;
    let mut gateway = Gateway::new(timings, Arc::clone(&metrics));

    let miners = tokio::spawn(accept_miners(
        listeners.miners,
        events_tx.clone(),
        max_miners,
        Arc::clone(&metrics),
    ));
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
    let miners = miners.await;
    let engines = engines.await;
    match (miners, engines) {
        (Ok(Err(err)), _) | (_, Ok(Err(err))) => Err(err),
        _ => Ok(()),
    }
}

async fn accept_miners(
    listener: TcpListener,
    events: mpsc::Sender<TimedEvent>,
    max_miners: usize,
    metrics: Arc<GatewayMetrics>,
) -> Result<()> {
    let mut next_conn = 1u64;
    loop {
        let (stream, peer) = match listener.accept().await {
            Ok(accepted) => accepted,
            Err(err) => {
                tracing::warn!(error = %err, "miner accept failed");
                tokio::time::sleep(ACCEPT_RETRY_DELAY).await;
                continue;
            }
        };
        if metrics.miners.load(Ordering::Relaxed) as usize >= max_miners {
            metrics.rejected_connections.fetch_add(1, Ordering::Relaxed);
            continue;
        }
        let _ = stream.set_nodelay(true);
        let conn = next_conn;
        next_conn += 1;
        let (reader, mut writer) = stream.into_split();
        let (tx, mut rx) = mpsc::channel::<MinerOut>(MINER_QUEUE);
        if events
            .send(TimedEvent::now(Event::MinerConnected { conn, peer, tx }))
            .await
            .is_err()
        {
            return Ok(());
        }
        // The reader holds the socket open until it ends, so the writer
        // ending (Close, a failed or stalled write) must end the reader too;
        // otherwise a peer that never closes keeps the descriptor forever.
        let (closed_tx, closed_rx) = oneshot::channel::<()>();
        let write_metrics = Arc::clone(&metrics);
        tokio::spawn(async move {
            while let Some(out) = rx.recv().await {
                match out {
                    MinerOut::Line(line) => {
                        let mut bytes = line.into_bytes();
                        bytes.push(b'\n');
                        let write_started_at = Instant::now();
                        match tokio::time::timeout(MINER_WRITE_TIMEOUT, writer.write_all(&bytes))
                            .await
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
            let _ = writer.shutdown().await;
            drop(closed_tx);
        });
        let events = events.clone();
        tokio::spawn(read_miner(conn, peer, reader, closed_rx, events));
    }
}

async fn read_miner(
    conn: u64,
    peer: SocketAddr,
    reader: tokio::net::tcp::OwnedReadHalf,
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

    #[tokio::test]
    async fn closing_a_miner_releases_its_socket_while_the_peer_stays_open() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (events_tx, mut events_rx) = mpsc::channel(16);
        let accept = tokio::spawn(accept_miners(
            listener,
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
