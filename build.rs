use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::SystemTime;

const REQUIRED_UI_ASSETS: &[&str] = &["index.html", "app.js", "app.css"];

fn main() {
    let repo_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").expect("manifest dir"));
    let frontend_dir = repo_dir.join("frontend");
    let frontend_src_dir = frontend_dir.join("src");
    let frontend_public_dir = frontend_dir.join("public");
    let frontend_dist_dir = frontend_dir.join("dist");

    for path in [
        frontend_dir.join("package.json"),
        frontend_dir.join("package-lock.json"),
        frontend_dir.join("tsconfig.json"),
        frontend_dir.join("vite.config.ts"),
        frontend_dir.join("index.html"),
    ] {
        track_path(&path);
    }
    track_tree(&frontend_src_dir);
    if frontend_public_dir.is_dir() {
        track_tree(&frontend_public_dir);
    }
    for asset in REQUIRED_UI_ASSETS {
        track_path(&frontend_dist_dir.join(asset));
    }

    verify_ui_bundle(
        &frontend_dir,
        &frontend_src_dir,
        &frontend_public_dir,
        &frontend_dist_dir,
    );
}

fn track_path(path: &Path) {
    println!("cargo:rerun-if-changed={}", path.display());
}

fn track_tree(root: &Path) {
    if !root.exists() {
        return;
    }
    if root.is_file() {
        track_path(root);
        return;
    }

    let mut dirs = vec![root.to_path_buf()];
    while let Some(dir) = dirs.pop() {
        let entries = match fs::read_dir(&dir) {
            Ok(entries) => entries,
            Err(err) => panic!("failed to read {}: {err}", dir.display()),
        };
        for entry in entries {
            let entry = match entry {
                Ok(entry) => entry,
                Err(err) => panic!("failed to read entry in {}: {err}", dir.display()),
            };
            let path = entry.path();
            if path.is_dir() {
                dirs.push(path);
            } else if path.is_file() {
                track_path(&path);
            }
        }
    }
}

fn verify_ui_bundle(
    frontend_dir: &Path,
    frontend_src_dir: &Path,
    frontend_public_dir: &Path,
    frontend_dist_dir: &Path,
) {
    let required_paths = REQUIRED_UI_ASSETS
        .iter()
        .map(|asset| frontend_dist_dir.join(asset))
        .collect::<Vec<_>>();
    let missing = required_paths
        .iter()
        .filter(|path| !path.is_file())
        .map(|path| path.display().to_string())
        .collect::<Vec<_>>();
    if !missing.is_empty() {
        fail_missing_or_stale(&format!("missing generated assets: {}", missing.join(", ")));
    }

    let mut source_files = vec![
        frontend_dir.join("package.json"),
        frontend_dir.join("package-lock.json"),
        frontend_dir.join("tsconfig.json"),
        frontend_dir.join("vite.config.ts"),
        frontend_dir.join("index.html"),
    ];
    source_files.extend(walk_files(frontend_src_dir));
    if frontend_public_dir.is_dir() {
        source_files.extend(walk_files(frontend_public_dir));
    }

    let newest_source = newest_mtime(&source_files)
        .unwrap_or_else(|| fail_missing_or_stale("frontend source tree is empty"));
    let oldest_output = oldest_mtime(&required_paths)
        .unwrap_or_else(|| fail_missing_or_stale("generated frontend assets are unreadable"));

    if oldest_output < newest_source {
        fail_missing_or_stale("frontend/dist is older than the frontend source files");
    }
}

fn walk_files(root: &Path) -> Vec<PathBuf> {
    if !root.exists() {
        return Vec::new();
    }
    if root.is_file() {
        return vec![root.to_path_buf()];
    }

    let mut files = Vec::new();
    let mut dirs = vec![root.to_path_buf()];
    while let Some(dir) = dirs.pop() {
        let entries = match fs::read_dir(&dir) {
            Ok(entries) => entries,
            Err(err) => panic!("failed to read {}: {err}", dir.display()),
        };
        for entry in entries {
            let entry = match entry {
                Ok(entry) => entry,
                Err(err) => panic!("failed to read entry in {}: {err}", dir.display()),
            };
            let path = entry.path();
            if path.is_dir() {
                dirs.push(path);
            } else if path.is_file() {
                files.push(path);
            }
        }
    }
    files
}

fn newest_mtime(paths: &[PathBuf]) -> Option<SystemTime> {
    paths.iter().filter_map(|path| file_mtime(path)).max()
}

fn oldest_mtime(paths: &[PathBuf]) -> Option<SystemTime> {
    paths.iter().filter_map(|path| file_mtime(path)).min()
}

fn file_mtime(path: &Path) -> Option<SystemTime> {
    fs::metadata(path).ok()?.modified().ok()
}

fn fail_missing_or_stale(reason: &str) -> ! {
    panic!(
        "{reason}. Build the UI first with `npm --prefix frontend ci && npm --prefix frontend run build`."
    );
}
