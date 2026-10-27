use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Result};
use pool_common::{cli::parse_config_path, logging::init_logging};
use pool_runtime::runtime::{
    bootstrap_shared_runtime, build_engine, build_stratum_server, start_config_reload_on_hangup,
    start_stratum_background_tasks,
};
use tokio::signal::unix::{signal, SignalKind};
use tracing::info;

/// Upper bound on waiting for an in-flight payout pass before exiting.
const PAYOUT_QUIESCE_TIMEOUT: Duration = Duration::from_secs(120);

#[tokio::main]
async fn main() -> Result<()> {
    init_logging();

    let config_path = parse_config_path("blocknet-pool-stratum", std::env::args().skip(1));
    let shared = bootstrap_shared_runtime(&config_path).await?;
    let engine = build_engine(&shared).await?;
    let stratum = build_stratum_server(&shared, Arc::clone(&engine))?;
    start_config_reload_on_hangup(config_path.clone(), Arc::clone(&engine));
    let background = start_stratum_background_tasks(&shared, engine, Arc::clone(&stratum));

    info!(pool = %shared.cfg.pool_name, "stratum runtime started");

    let gateway_socket = shared.cfg.gateway_socket.trim().to_string();
    let serve = async {
        if gateway_socket.is_empty() {
            Arc::clone(&stratum).run().await
        } else {
            // systemd template instances pass their name (`%i`).
            let instance =
                std::env::var("BLOCKNET_POOL_INSTANCE").unwrap_or_else(|_| "default".to_string());
            Arc::clone(&stratum)
                .run_gateway_client(gateway_socket.into(), instance, Arc::clone(&shared.role))
                .await
        }
    };
    let mut terminate = signal(SignalKind::terminate())?;
    let outcome = tokio::select! {
        result = serve => result.map_err(|err| anyhow!("stratum server exited: {err}")),
        _ = tokio::signal::ctrl_c() => {
            info!("received interrupt");
            Ok(())
        }
        _ = terminate.recv() => {
            info!("received SIGTERM");
            Ok(())
        }
    };
    background.quiesce(PAYOUT_QUIESCE_TIMEOUT).await;
    // Exit now: dropping the runtime would wait for long-running blocking
    // work (validation, payout loops) and keep a retired engine alive.
    match outcome {
        Ok(()) => {
            info!("stratum stopped");
            std::process::exit(0);
        }
        Err(err) => {
            tracing::error!(error = %format!("{err:#}"), "stratum stopped");
            std::process::exit(1);
        }
    }
}
