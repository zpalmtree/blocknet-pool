use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::SystemTime;

const REQUIRED_UI_ASSETS: &[&str] = &["index.html", "app.js", "app.css", "public-pages.json"];
const FRONTEND_SOURCE_FILES: &[&str] = &[
    "package.json",
    "package-lock.json",
    "tsconfig.json",
    "vite.config.ts",
    "index.html",
];

fn main() {
    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").expect("manifest dir"));
    let repo_dir = manifest_dir
        .ancestors()
        .nth(2)
        .expect("repo root")
        .to_path_buf();
    let frontend_dir = repo_dir.join("frontend");
    let frontend_src_dir = frontend_dir.join("src");
    let frontend_dist_dir = frontend_dir.join("dist");

    println!(
        "cargo:rustc-env=BLOCKNET_POOL_FRONTEND_DIST_DIR={}",
        frontend_dist_dir.display()
    );

    for file in FRONTEND_SOURCE_FILES {
        track_path(&frontend_dir.join(file));
    }
    for path in walk_files(&frontend_src_dir) {
        track_path(&path);
    }
    for asset in REQUIRED_UI_ASSETS {
        track_path(&frontend_dist_dir.join(asset));
    }

    verify_ui_bundle(&frontend_dir, &frontend_src_dir, &frontend_dist_dir);
    embed_languages(&frontend_dist_dir);
    embed_blog(&frontend_dir, &frontend_dist_dir.join("blog"));
}

// Vite's ?url imports emit content-hashed catalogs instead of putting all
// languages into app.js. Embed the exact filenames; never read request paths.
fn embed_languages(frontend_dist_dir: &Path) {
    let mut files = walk_files(&frontend_dist_dir.join("assets"));
    files.sort();
    let mut entries = String::new();
    for file in files {
        if file.extension().and_then(|ext| ext.to_str()) != Some("json") {
            continue;
        }
        track_path(&file);
        let name = file
            .file_name()
            .expect("catalog filename")
            .to_string_lossy();
        entries.push_str(&format!(
            "    ({:?}, include_bytes!({:?})),\n",
            format!("assets/{name}"),
            file.display().to_string()
        ));
    }
    assert!(!entries.is_empty(), "missing built language catalogs");
    let out = PathBuf::from(env::var("OUT_DIR").expect("out dir")).join("ui_languages.rs");
    fs::write(
        out,
        format!("static UI_LANGUAGES: &[(&str, &[u8])] = &[\n{entries}];\n"),
    )
    .expect("write ui_languages.rs");
}

/// Embed the pre-rendered blog (frontend/dist/blog, written by scripts/build-blog.mjs) as a
/// static table for src/blog.rs, and fail early if it is missing or older than its sources.
fn embed_blog(frontend_dir: &Path, blog_dist_dir: &Path) {
    let sources: Vec<PathBuf> = walk_files(&frontend_dir.join("blog"))
        .into_iter()
        .chain(walk_files(&frontend_dir.join("scripts")))
        .collect();
    for path in &sources {
        track_path(path);
    }
    let marker = blog_dist_dir.join("sitemap-paths.txt");
    track_path(&marker);
    let built = file_mtime(&marker)
        .unwrap_or_else(|| fail_missing_or_stale("missing frontend/dist/blog (the blog build)"));
    if sources
        .iter()
        .filter_map(|path| file_mtime(path))
        .max()
        .is_some_and(|newest| newest > built)
    {
        fail_missing_or_stale("frontend/dist/blog is older than frontend/blog or frontend/scripts");
    }

    let mut files = walk_files(blog_dist_dir);
    files.sort();
    let mut entries = String::new();
    for file in &files {
        let rel = file
            .strip_prefix(blog_dist_dir)
            .expect("blog file under dist/blog")
            .to_string_lossy()
            .replace('\\', "/");
        if rel == "sitemap-paths.txt" {
            continue;
        }
        let url = match rel.strip_suffix("index.html") {
            Some(dir) => format!("/blog/{dir}"),
            None => format!("/blog/{rel}"),
        };
        let content_type = match file.extension().and_then(|ext| ext.to_str()) {
            Some("html") => "text/html; charset=utf-8",
            Some("css") => "text/css; charset=utf-8",
            Some("js") => "text/javascript; charset=utf-8",
            Some("xml") => "application/rss+xml; charset=utf-8",
            Some("png") => "image/png",
            Some("webp") => "image/webp",
            Some("jpg" | "jpeg") => "image/jpeg",
            Some("svg") => "image/svg+xml",
            _ => "application/octet-stream",
        };
        entries.push_str(&format!(
            "    ({url:?}, {content_type:?}, include_bytes!({:?})),\n",
            file.display().to_string()
        ));
    }
    let sitemap = fs::read_to_string(&marker).expect("read blog sitemap paths");
    let paths: String = sitemap
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| format!("    {:?},\n", line.trim()))
        .collect();
    let generated = format!(
        "pub(crate) static BLOG_FILES: &[(&str, &str, &[u8])] = &[\n{entries}];\n\
         pub(crate) static BLOG_SITEMAP_PATHS: &[&str] = &[\n{paths}];\n"
    );
    let out = PathBuf::from(env::var("OUT_DIR").expect("out dir")).join("blog_files.rs");
    fs::write(out, generated).expect("write blog_files.rs");
}

fn track_path(path: &Path) {
    println!("cargo:rerun-if-changed={}", path.display());
}

fn verify_ui_bundle(frontend_dir: &Path, frontend_src_dir: &Path, frontend_dist_dir: &Path) {
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

    let mut source_files = FRONTEND_SOURCE_FILES
        .iter()
        .map(|file| frontend_dir.join(file))
        .collect::<Vec<_>>();
    source_files.extend(walk_files(frontend_src_dir));

    let newest_source = source_files
        .iter()
        .filter_map(|path| file_mtime(path))
        .max()
        .unwrap_or_else(|| fail_missing_or_stale("frontend source tree is empty"));
    let oldest_output = required_paths
        .iter()
        .filter_map(|path| file_mtime(path))
        .min()
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

fn file_mtime(path: &Path) -> Option<SystemTime> {
    fs::metadata(path).ok()?.modified().ok()
}

fn fail_missing_or_stale(reason: &str) -> ! {
    panic!(
        "{reason}. Build the UI first with `npm --prefix frontend ci && npm --prefix frontend run build`."
    );
}
