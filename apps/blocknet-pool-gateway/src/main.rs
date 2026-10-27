use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use pool_common::{cli::parse_config_path, logging::init_logging};
use pool_gateway::{GatewayListeners, GatewayMetrics, Timings};
use serde::Deserialize;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, UnixListener, UnixStream};
use tokio::signal::unix::{signal, SignalKind};
use tracing::{info, warn};

/// The settings the gateway reads from the pool's config.json.
#[derive(Deserialize)]
#[serde(default)]
struct GatewaySettings {
    stratum_host: String,
    stratum_port: u16,
    gateway_socket: String,
    gateway_metrics_addr: String,
    gateway_max_miners: usize,
}

impl Default for GatewaySettings {
    fn default() -> Self {
        Self {
            stratum_host: "127.0.0.1".to_string(),
            stratum_port: 3333,
            gateway_socket: String::new(),
            gateway_metrics_addr: "127.0.0.1:24786".to_string(),
            gateway_max_miners: 16_384,
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    init_logging();
    let config_path = parse_config_path("blocknet-pool-gateway", std::env::args().skip(1));
    let settings: GatewaySettings = serde_json::from_slice(
        &std::fs::read(&config_path).with_context(|| format!("read {}", config_path.display()))?,
    )
    .with_context(|| format!("parse {}", config_path.display()))?;
    let socket = settings.gateway_socket.trim();
    if socket.is_empty() {
        bail!("gateway_socket is not set in {}", config_path.display());
    }

    let engines = bind_engine_socket(Path::new(socket)).await?;
    let miners_addr = format!("{}:{}", settings.stratum_host, settings.stratum_port);
    let miners = TcpListener::bind(&miners_addr)
        .await
        .with_context(|| format!("bind miners on {miners_addr}"))?;
    let metrics = Arc::new(GatewayMetrics::default());
    let metrics_addr: SocketAddr = settings
        .gateway_metrics_addr
        .parse()
        .context("gateway_metrics_addr")?;
    tokio::spawn(serve_metrics(metrics_addr, Arc::clone(&metrics)));
    info!(miners = %miners_addr, engines = socket, metrics = %metrics_addr, "stratum gateway listening");

    let mut terminate = signal(SignalKind::terminate())?;
    tokio::select! {
        result = pool_gateway::run(
            GatewayListeners { miners, engines },
            Timings::default(),
            settings.gateway_max_miners,
            metrics,
        ) => result,
        _ = terminate.recv() => {
            info!("received SIGTERM; miners will reconnect to the next gateway");
            Ok(())
        }
        _ = tokio::signal::ctrl_c() => Ok(()),
    }
}

/// Binds the engine socket, replacing a stale socket file but refusing to
/// start beside a gateway that is still answering on it.
async fn bind_engine_socket(path: &Path) -> Result<UnixListener> {
    if path.exists() {
        if UnixStream::connect(path).await.is_ok() {
            bail!("another gateway is listening on {}", path.display());
        }
        std::fs::remove_file(path).with_context(|| format!("remove stale {}", path.display()))?;
    }
    UnixListener::bind(path).with_context(|| format!("bind {}", path.display()))
}

async fn serve_metrics(addr: SocketAddr, metrics: Arc<GatewayMetrics>) {
    let listener = match TcpListener::bind(addr).await {
        Ok(listener) => listener,
        Err(err) => {
            warn!(addr = %addr, error = %err, "gateway metrics disabled");
            return;
        }
    };
    loop {
        let Ok((mut stream, _)) = listener.accept().await else {
            continue;
        };
        let body = metrics.render();
        tokio::spawn(async move {
            let mut request = [0u8; 1024];
            let _ = stream.read(&mut request).await;
            let response = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: text/plain; version=0.0.4\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                body.len()
            );
            let _ = stream.write_all(response.as_bytes()).await;
        });
    }
}
