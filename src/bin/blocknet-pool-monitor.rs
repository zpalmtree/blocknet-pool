use std::env;
use std::path::PathBuf;

use anyhow::Result;
use blocknet_pool_rs::logging::init_logging;
use blocknet_pool_rs::monitor::run_monitor;

#[tokio::main]
async fn main() -> Result<()> {
    init_logging();

    let config_path = parse_config_path(env::args().skip(1))?;
    run_monitor(&config_path).await
}

fn parse_config_path(mut args: impl Iterator<Item = String>) -> Result<PathBuf> {
    let first = args.next();
    if matches!(first.as_deref(), Some("--help") | Some("-h")) {
        println!("usage: blocknet-pool-monitor [flags]");
        println!();
        println!("flags:");
        println!("  -c, --config  path to config file (default: config.json)");
        std::process::exit(0);
    }

    let mut config_path = PathBuf::from("config.json");
    if let Some(flag) = first {
        if flag == "--config" || flag == "-c" {
            if let Some(path) = args.next() {
                config_path = PathBuf::from(path);
            }
        }
    }
    Ok(config_path)
}
