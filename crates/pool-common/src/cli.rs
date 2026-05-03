use std::path::PathBuf;

pub fn parse_config_path(binary_name: &str, mut args: impl Iterator<Item = String>) -> PathBuf {
    let first = args.next();
    if matches!(first.as_deref(), Some("--help") | Some("-h")) {
        println!("usage: {binary_name} [flags]");
        println!();
        println!("flags:");
        println!("  --config  path to config file (default: config.json)");
        std::process::exit(0);
    }

    let mut config_path = PathBuf::from("config.json");
    if let Some(flag) = first {
        if flag == "--config" {
            if let Some(path) = args.next() {
                config_path = PathBuf::from(path);
            }
        }
    }
    config_path
}
