//! Stratum gateway: holds miners' TCP connections and relays them to
//! Stratum engines over a unix socket, so engines can be replaced without
//! disconnecting miners (STRATUM_GATEWAY.md).

mod core;

use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use anyhow::Result;
use pool_common::gateway::{read_frame, write_frame, Frame};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, UnixListener};
use tokio::sync::mpsc;

pub use crate::core::{Event, Gateway, MinerOut, Timings};

/// Same request bound as Stratum.
const MAX_MINER_LINE: usize = 8 * 1024;
const EVENT_QUEUE: usize = 65_536;
const MINER_QUEUE: usize = 256;
const ENGINE_QUEUE: usize = 65_536;

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
        out
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
    let (events_tx, mut events_rx) = mpsc::channel::<Event>(EVENT_QUEUE);
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
            if events_tx.send(Event::Tick).await.is_err() {
                return;
            }
        }
    });

    while let Some(event) = events_rx.recv().await {
        gateway.handle(event, Instant::now());
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
    events: mpsc::Sender<Event>,
    max_miners: usize,
    metrics: Arc<GatewayMetrics>,
) -> Result<()> {
    let mut next_conn = 1u64;
    loop {
        let (stream, peer) = listener.accept().await?;
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
            .send(Event::MinerConnected { conn, peer, tx })
            .await
            .is_err()
        {
            return Ok(());
        }
        tokio::spawn(async move {
            while let Some(out) = rx.recv().await {
                match out {
                    MinerOut::Line(line) => {
                        let mut bytes = line.into_bytes();
                        bytes.push(b'\n');
                        if writer.write_all(&bytes).await.is_err() {
                            break;
                        }
                    }
                    MinerOut::Close => break,
                }
            }
            let _ = writer.shutdown().await;
        });
        let events = events.clone();
        tokio::spawn(read_miner(conn, peer, reader, events));
    }
}

async fn read_miner(
    conn: u64,
    peer: SocketAddr,
    reader: tokio::net::tcp::OwnedReadHalf,
    events: mpsc::Sender<Event>,
) {
    let mut reader = BufReader::new(reader);
    let mut line = Vec::with_capacity(256);
    loop {
        line.clear();
        let read = (&mut reader)
            .take(MAX_MINER_LINE as u64 + 1)
            .read_until(b'\n', &mut line)
            .await;
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
                if events.send(event).await.is_err() {
                    return;
                }
            }
            Err(_) => break,
        }
    }
    let _ = events.send(Event::MinerClosed { conn }).await;
}

async fn accept_engines(listener: UnixListener, events: mpsc::Sender<Event>) -> Result<()> {
    let mut next_engine = 1u64;
    loop {
        let (stream, _) = listener.accept().await?;
        let engine = next_engine;
        next_engine += 1;
        let (mut reader, mut writer) = stream.into_split();
        let (tx, mut rx) = mpsc::channel::<Frame>(ENGINE_QUEUE);
        if events
            .send(Event::EngineConnected { engine, tx })
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
                            .send(Event::EngineFrame { engine, frame })
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
            let _ = events.send(Event::EngineClosed { engine }).await;
        });
    }
}
