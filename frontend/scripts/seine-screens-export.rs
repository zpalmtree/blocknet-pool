// Exports the seine screens shown in Get Started's "Seine in Action" (src/data/seine-screens.json).
// Uses seine's real terminal renderer with example data; no hashing, network I/O or wallet is started.
//
// 1. In a seine checkout, append this file to src/miner/thread_showcase.rs (do not commit it there).
// 2. SEINE_POOL_SCREENS=/tmp/seine-screens.json cargo test --bin seine export_pool_get_started_screens -- --ignored
// 3. node frontend/scripts/seine-screens.mjs /tmp/seine-screens.json   (from the pool repo root)
fn runs_from_buffer(buffer: &ratatui::buffer::Buffer, width: u16, height: u16) -> serde_json::Value {
    let mut lines = Vec::new();
    for y in 0..height {
        let mut runs: Vec<serde_json::Value> = Vec::new();
        let mut text = String::new();
        let mut previous: Option<(Color, Color, bool)> = None;
        let flush = |runs: &mut Vec<serde_json::Value>, text: &mut String, style: Option<(Color, Color, bool)>| {
            if let Some((fg, bg, bold)) = style {
                if !text.is_empty() {
                    let bg = match bg { Color::Reset => serde_json::Value::Null, bg => color(bg).into() };
                    runs.push(serde_json::json!([text.clone(), color(fg), bg, bold]));
                }
            }
            text.clear();
        };
        for x in 0..width {
            let cell = &buffer[(x, y)];
            let style = (cell.fg, cell.bg, cell.modifier.contains(ratatui::style::Modifier::BOLD));
            if previous != Some(style) {
                flush(&mut runs, &mut text, previous);
                previous = Some(style);
            }
            text.push_str(cell.symbol());
        }
        flush(&mut runs, &mut text, previous);
        lines.push(serde_json::Value::Array(runs));
    }
    serde_json::json!({ "cols": width, "rows": height, "lines": lines })
}

fn form_runs(help: &str, label: &str, initial: &str, typed: &str, width: u16, height: u16) -> serde_json::Value {
    let validate = |_: &str| Ok(());
    let mut terminal = Terminal::new(TestBackend::new(width, height)).unwrap();
    terminal.draw(|frame| crate::miner::tui_prompt::draw_for_showcase(
        frame, crate::miner::tui_prompt::PromptSpec { help, label, initial, validate: &validate }, typed, None)).unwrap();
    runs_from_buffer(terminal.backend().buffer(), width, height)
}

fn dashboard_runs(state: &TuiStateInner, width: u16, height: u16) -> serde_json::Value {
    let mut terminal = Terminal::new(TestBackend::new(width, height)).unwrap();
    terminal.draw(|frame| draw_dashboard(frame, frame.area(), state)).unwrap();
    runs_from_buffer(terminal.backend().buffer(), width, height)
}

#[test]
#[ignore = "exports bntpool.com Get Started screens; pass SEINE_POOL_SCREENS to choose the file"]
fn export_pool_get_started_screens() {
    use LogLevel::{Info, Success, Warn};
    let steady: &[(LogLevel, &str, &str, f64)] = &[
        (Info, "BACKEND", "cpu: starting 16 threads (~32.0 GiB RAM)", 7_500.0),
        (Success, "CONN", "connected to pool", 7_500.0),
        (Success, "CPU", "tuned to 16 threads", 7_200.0),
        (Success, "SHARE", "share accepted", 2_400.0),
        (Warn, "SHARE", "rejected (stale share)", 2_380.0),
        (Success, "SHARE", "share accepted", 4.0),
    ];
    let mut mining = mining_fixture("working", steady);
    mining.log_entries[3].count = 96;
    mining.log_entries[5].count = 115;
    mining.round_hashrate = "52.40 H/s".into();
    mining.avg_hashrate = "51.87 H/s".into();
    mining.total_hashes = 389_025;
    mining.submitted = 212;
    mining.accepted = 211;
    mining.stale_shares = 1;
    mining.height = "71,742".into();
    mining.network_hashrate = "57.10 KH/s".into();
    mining.device_hashrates = vec![DeviceHashrate { name: "cpu#1".into(), current: "52.40 H/s".into(), average: "51.87 H/s".into() }];
    let address = ("Welcome to seine. Mining rewards are paid to your Blocknet wallet.", "Your address or shortname (@name / $name)", "", "@yourname");
    let pool = ("Press Enter to mine on the default pool, or type another.", "Pool", "stratum+tcp://bntpool.com:3333", "");
    let mut out = serde_json::Map::new();
    for (name, prompt_cols, prompt_rows, dash_cols, dash_rows) in [("desktop", 60u16, 12u16, 120u16, 23u16), ("mobile", 46, 14, 46, 34)] {
        out.insert(format!("address-{name}"), form_runs(address.0, address.1, address.2, address.3, prompt_cols, prompt_rows));
        out.insert(format!("pool-{name}"), form_runs(pool.0, pool.1, pool.2, pool.3, prompt_cols, prompt_rows));
        out.insert(format!("mining-{name}"), dashboard_runs(&mining, dash_cols, dash_rows));
    }
    let path = std::env::var("SEINE_POOL_SCREENS").unwrap_or_else(|_| "/tmp/seine-pool-screens.json".into());
    std::fs::write(path, serde_json::to_string(&serde_json::Value::Object(out)).unwrap()).unwrap();
}
