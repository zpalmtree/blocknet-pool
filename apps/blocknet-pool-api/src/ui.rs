use axum::body::Body;
use axum::extract::{Path, Request, State};
use axum::http::{header, HeaderName, HeaderValue, Method, StatusCode, Uri};
use axum::middleware::Next;
use axum::response::{Html, IntoResponse, Response};

use crate::api::{error_response, ApiState};

const UI_INDEX_HTML: &str = include_str!(concat!(
    env!("BLOCKNET_POOL_FRONTEND_DIST_DIR"),
    "/index.html"
));
const UI_ASSET_APP_JS: &str =
    include_str!(concat!(env!("BLOCKNET_POOL_FRONTEND_DIST_DIR"), "/app.js"));
const UI_ASSET_APP_CSS: &str =
    include_str!(concat!(env!("BLOCKNET_POOL_FRONTEND_DIST_DIR"), "/app.css"));
const UI_ASSET_POOL_ENTERED_PNG: &[u8] = include_bytes!("ui/assets/pool-entered.png");
const UI_ASSET_MINING_TUI_PNG: &[u8] = include_bytes!("ui/assets/mining-tui.png");

/// Per-route social preview cards, served from `/ui-assets/<name>`.
const OG_IMAGES: &[(&str, &[u8])] = &[
    (
        "og-dashboard.png",
        include_bytes!("ui/assets/og-dashboard.png"),
    ),
    ("og-start.png", include_bytes!("ui/assets/og-start.png")),
    (
        "og-checkpoints.png",
        include_bytes!("ui/assets/og-checkpoints.png"),
    ),
    ("og-luck.png", include_bytes!("ui/assets/og-luck.png")),
    ("og-blocks.png", include_bytes!("ui/assets/og-blocks.png")),
    ("og-payouts.png", include_bytes!("ui/assets/og-payouts.png")),
    ("og-stats.png", include_bytes!("ui/assets/og-stats.png")),
    ("og-status.png", include_bytes!("ui/assets/og-status.png")),
    ("og-admin.png", include_bytes!("ui/assets/og-admin.png")),
];

const UI_FAVICON_SVG: &str = r##"<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 32 32" role="img" aria-label="Blocknet Pool"><rect x="4" y="4" width="24" height="24" rx="4" fill="#16a34a"/><rect x="9" y="9" width="14" height="14" rx="2" fill="#fff" opacity=".9"/><rect x="12" y="12" width="8" height="8" rx="1" fill="#16a34a"/></svg>"##;

/// Marker placed in `frontend/index.html` where per-route `<head>` metadata is injected.
const SSR_META_MARKER: &str = "<!--SSR-META-->";

// Shared with the browser so route transitions retain the server's canonical,
// robots, description and social-card metadata.
#[derive(serde::Deserialize)]
struct GuideLink {
    href: String,
    label: String,
}

#[derive(serde::Deserialize)]
struct PageMeta {
    title: String,
    description: String,
    image: String,
    index: bool,
    label: String,
    guides: Vec<GuideLink>,
}

static PAGE_METADATA: std::sync::LazyLock<std::collections::HashMap<String, PageMeta>> =
    std::sync::LazyLock::new(|| {
        serde_json::from_str(include_str!("../../../frontend/src/lib/page-meta.json"))
            .expect("valid public page metadata")
    });

fn page_meta(path: &str) -> &'static PageMeta {
    PAGE_METADATA
        .get(normalize_path(path))
        .unwrap_or(&PAGE_METADATA["/404"])
}

fn is_spa_route(path: &str) -> bool {
    let path = normalize_path(path);
    path == "/admin" || SITEMAP_PATHS.contains(&path)
}

fn normalize_path(path: &str) -> &str {
    let trimmed = path.trim_end_matches('/');
    if trimmed.is_empty() {
        "/"
    } else {
        trimmed
    }
}

fn html_escape(input: &str) -> String {
    input
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

/// Canonical public origin for page metadata, from the configured `pool_url`.
/// Request `Host`/`X-Forwarded-Host` headers are client-controlled and must not
/// end up in og:url/og:image of a cacheable page.
pub(crate) fn public_base_url(pool_url: &str) -> String {
    let base = pool_url.trim().trim_end_matches('/');
    if base.is_empty() {
        "https://bntpool.com".to_string()
    } else {
        base.to_string()
    }
}

fn host_label(base_url: &str) -> &str {
    base_url.split("://").nth(1).unwrap_or(base_url)
}

fn meta_tags(path: &str, base_url: &str) -> String {
    let meta = page_meta(path);
    let title = html_escape(&meta.title);
    let description = html_escape(&meta.description);
    let site_name = html_escape(host_label(base_url));
    let url = html_escape(&format!("{base_url}{}", normalize_path(path)));
    let image = html_escape(&format!("{base_url}/ui-assets/{}", meta.image));

    let nl = "\n    ";
    let robots = if meta.index {
        "index, follow"
    } else {
        "noindex, follow"
    };

    let mut out = format!("<title>{title}</title>");
    out.push_str(&format!(
        "{nl}<meta name=\"description\" content=\"{description}\" />"
    ));
    out.push_str(&format!(
        "{nl}<meta name=\"robots\" content=\"{robots}\" />"
    ));
    out.push_str(&format!("{nl}<link rel=\"canonical\" href=\"{url}\" />"));
    out.push_str(&format!(
        "{nl}<meta property=\"og:type\" content=\"website\" />"
    ));
    out.push_str(&format!(
        "{nl}<meta property=\"og:site_name\" content=\"{site_name}\" />"
    ));
    out.push_str(&format!(
        "{nl}<meta property=\"og:title\" content=\"{title}\" />"
    ));
    out.push_str(&format!(
        "{nl}<meta property=\"og:description\" content=\"{description}\" />"
    ));
    out.push_str(&format!(
        "{nl}<meta property=\"og:url\" content=\"{url}\" />"
    ));
    out.push_str(&format!(
        "{nl}<meta property=\"og:image\" content=\"{image}\" />"
    ));
    out.push_str(&format!(
        "{nl}<meta property=\"og:image:width\" content=\"1200\" />"
    ));
    out.push_str(&format!(
        "{nl}<meta property=\"og:image:height\" content=\"630\" />"
    ));
    out.push_str(&format!(
        "{nl}<meta name=\"twitter:card\" content=\"summary_large_image\" />"
    ));
    out.push_str(&format!(
        "{nl}<meta name=\"twitter:title\" content=\"{title}\" />"
    ));
    out.push_str(&format!(
        "{nl}<meta name=\"twitter:description\" content=\"{description}\" />"
    ));
    out.push_str(&format!(
        "{nl}<meta name=\"twitter:image\" content=\"{image}\" />"
    ));
    out
}

fn render_index_html(path: &str, base_url: &str) -> String {
    let tags = meta_tags(path, base_url);
    let html = if UI_INDEX_HTML.contains(SSR_META_MARKER) {
        UI_INDEX_HTML.replace(SSR_META_MARKER, &tags)
    } else {
        // Defensive fallback if the build ever drops the marker: inject into <head>.
        UI_INDEX_HTML.replacen("<head>", &format!("<head>\n    {tags}"), 1)
    };
    if SITEMAP_PATHS.contains(&normalize_path(path)) {
        html.replace(r#"<div id="root"></div>"#, &public_page_html(path))
    } else if is_spa_route(path) {
        html
    } else {
        // Keep recovery links usable before JavaScript loads or if it is disabled.
        html.replace(r#"<div id="root"></div>"#, r#"<div id="root"><main class="container" id="page-not-found"><div class="page-header"><span class="page-kicker">404</span><h1>Page not found</h1><p class="page-intro">This link does not lead to a page. Open the dashboard, get started, or browse the mining guides.</p></div><div class="page-actions"><a class="btn btn-primary" href="/">Dashboard</a><a class="btn btn-secondary" href="/start">Get Started</a><a class="btn btn-secondary" href="/blog/">Blog</a></div></main></div>"#)
    }
}

/// Useful initial content for all visitors, including when the app bundle fails.
/// React replaces this shell when ready; changing live metrics are never fabricated.
fn public_page_html(path: &str) -> String {
    let meta = page_meta(path);
    let mut html = String::from(
        r#"<div id="root"><nav aria-label="Main navigation"><a class="nav-brand" href="/">bntpool.com</a></nav><main class="container" id="public-page"><div class="page-header">"#,
    );
    html.push_str(&format!(
        "<h1>{}</h1><p class=\"page-intro\">{}</p></div>",
        html_escape(&meta.title),
        html_escape(&meta.description)
    ));
    if normalize_path(path) == "/start" {
        html.push_str(r#"<section class="card section"><h2>Connect your miner</h2><ol><li><a href="https://seineminer.com/#download">Download Seine for your operating system</a>.</li><li>Run Seine and enter your Blocknet wallet address or Blocknet ID when prompted.</li><li>Choose bntpool.com, then open <a href="/stats">My Stats</a> to check your miner's accepted shares.</li></ol></section>"#);
    }
    html.push_str(r#"<section class="card section"><h2>Mining guides</h2><ul>"#);
    for guide in &meta.guides {
        html.push_str(&format!(
            "<li><a href=\"{}\">{}</a></li>",
            html_escape(&guide.href),
            html_escape(&guide.label)
        ));
    }
    html.push_str(r#"</ul><a href="/blog/">All mining guides</a></section><section class="card section"><h2>Pool pages</h2><div class="page-actions">"#);
    for route in SITEMAP_PATHS {
        let item = page_meta(route);
        html.push_str(&format!(
            "<a class=\"btn btn-secondary\" href=\"{}\">{}</a>",
            route,
            html_escape(&item.label)
        ));
    }
    html.push_str(r#"</div><p>Live charts and miner lookups load with JavaScript. You can open the setup and mining guides above without it.</p></section></main></div>"#);
    html
}

fn og_image_bytes(name: &str) -> Option<&'static [u8]> {
    OG_IMAGES
        .iter()
        .find(|(asset, _)| *asset == name)
        .map(|(_, bytes)| *bytes)
}

fn ui_response(base_url: &str, path: &str) -> Response {
    let mut response = Html(render_index_html(path, base_url)).into_response();
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-cache"));
    response
}

pub(crate) async fn handle_ui(State(state): State<ApiState>) -> Response {
    ui_response(&state.public_base_url(), "/")
}

pub(crate) async fn handle_app_fallback(
    State(state): State<ApiState>,
    method: Method,
    uri: Uri,
) -> Response {
    app_fallback_response(method, &uri, &state.public_base_url())
}

pub(crate) fn app_fallback_response(method: Method, uri: &Uri, base_url: &str) -> Response {
    if is_api_request_path(uri.path()) {
        return error_response(StatusCode::NOT_FOUND, "not found");
    }

    if matches!(method, Method::GET | Method::HEAD) {
        let mut response = ui_response(base_url, uri.path());
        if !is_spa_route(uri.path()) {
            *response.status_mut() = StatusCode::NOT_FOUND;
        }
        if method == Method::HEAD {
            *response.body_mut() = Body::empty();
        }
        return response;
    }

    StatusCode::NOT_FOUND.into_response()
}

/// Content-Security-Policy for the embedded SPA. Everything is served from
/// this origin except Google Fonts (stylesheet + font files) and the
/// blocknet.id handle lookup the stats page performs with `fetch`. React
/// `style` props go through CSSOM and need no `'unsafe-inline'`.
pub(crate) const CONTENT_SECURITY_POLICY: &str = "default-src 'self'; \
script-src 'self'; \
style-src 'self' https://fonts.googleapis.com; \
font-src 'self' https://fonts.gstatic.com; \
img-src 'self' data:; \
connect-src 'self' https://blocknet.id; \
object-src 'none'; \
base-uri 'none'; \
form-action 'self'; \
frame-ancestors 'none'";

/// Defense-in-depth headers on every response (HTML, assets, and JSON).
/// Values a handler already set are left alone.
pub(crate) async fn apply_security_headers(req: Request, next: Next) -> Response {
    let mut response = next.run(req).await;
    let headers = response.headers_mut();
    for (name, value) in [
        (header::X_CONTENT_TYPE_OPTIONS, "nosniff"),
        (header::REFERRER_POLICY, "strict-origin-when-cross-origin"),
        (header::X_FRAME_OPTIONS, "DENY"),
        (header::CONTENT_SECURITY_POLICY, CONTENT_SECURITY_POLICY),
        (
            HeaderName::from_static("cross-origin-opener-policy"),
            "same-origin",
        ),
    ] {
        headers
            .entry(name)
            .or_insert(HeaderValue::from_static(value));
    }
    response
}

pub(crate) fn is_api_request_path(path: &str) -> bool {
    path == "/api" || path.starts_with("/api/")
}

/// Public routes listed in the sitemap, in navigation order.
const SITEMAP_PATHS: &[&str] = &[
    "/",
    "/start",
    "/blocks",
    "/payouts",
    "/stats",
    "/luck",
    "/status",
    "/checkpoints",
];

pub(crate) async fn handle_robots_txt(State(state): State<ApiState>) -> impl IntoResponse {
    let base_url = state.public_base_url();
    (
        [
            (header::CONTENT_TYPE, "text/plain; charset=utf-8"),
            (header::CACHE_CONTROL, "public, max-age=3600"),
        ],
        format!("User-agent: *\nAllow: /\nDisallow: /api/\n\nSitemap: {base_url}/sitemap.xml\n"),
    )
}

pub(crate) async fn handle_sitemap_xml(State(state): State<ApiState>) -> impl IntoResponse {
    let base_url = state.public_base_url();
    let urls: String = SITEMAP_PATHS
        .iter()
        .chain(crate::blog::blog_sitemap_paths())
        .map(|path| format!("  <url><loc>{}{path}</loc></url>\n", html_escape(&base_url)))
        .collect();
    (
        [
            (header::CONTENT_TYPE, "application/xml; charset=utf-8"),
            (header::CACHE_CONTROL, "public, max-age=3600"),
        ],
        format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<urlset xmlns=\"http://www.sitemaps.org/schemas/sitemap/0.9\">\n{urls}</urlset>\n"
        ),
    )
}

pub(crate) async fn handle_favicon_svg() -> impl IntoResponse {
    (
        [
            (header::CONTENT_TYPE, "image/svg+xml"),
            (header::CACHE_CONTROL, "public, max-age=86400"),
        ],
        UI_FAVICON_SVG,
    )
}

pub(crate) async fn handle_ui_asset(Path(name): Path<String>) -> Response {
    match name.as_str() {
        "app.js" => (
            [
                (
                    header::CONTENT_TYPE,
                    "application/javascript; charset=utf-8",
                ),
                (header::CACHE_CONTROL, "no-cache"),
            ],
            UI_ASSET_APP_JS,
        )
            .into_response(),
        "app.css" => (
            [
                (header::CONTENT_TYPE, "text/css; charset=utf-8"),
                (header::CACHE_CONTROL, "no-cache"),
            ],
            UI_ASSET_APP_CSS,
        )
            .into_response(),
        "pool-entered.png" => (
            [
                (header::CONTENT_TYPE, "image/png"),
                (header::CACHE_CONTROL, "public, max-age=3600"),
            ],
            UI_ASSET_POOL_ENTERED_PNG,
        )
            .into_response(),
        "mining-tui.png" => (
            [
                (header::CONTENT_TYPE, "image/png"),
                (header::CACHE_CONTROL, "public, max-age=3600"),
            ],
            UI_ASSET_MINING_TUI_PNG,
        )
            .into_response(),
        other => match og_image_bytes(other) {
            Some(bytes) => (
                [
                    (header::CONTENT_TYPE, "image/png"),
                    (header::CACHE_CONTROL, "public, max-age=86400"),
                ],
                bytes,
            )
                .into_response(),
            None => StatusCode::NOT_FOUND.into_response(),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn known_spa_routes_keep_success_status_with_queries_and_trailing_slashes() {
        for path in SITEMAP_PATHS.iter().copied().chain(["/admin"]) {
            for suffix in ["", "/", "?lang=th", "/?lang=ar&worker=demo"] {
                let uri: Uri = format!("{path}{suffix}").parse().unwrap();
                for method in [Method::GET, Method::HEAD] {
                    let response =
                        app_fallback_response(method.clone(), &uri, "https://bntpool.com");
                    assert_eq!(response.status(), StatusCode::OK, "{method} {uri}");
                    assert_eq!(response.headers()[header::CACHE_CONTROL], "no-cache");
                    if method == Method::HEAD {
                        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
                            .await
                            .unwrap();
                        assert!(body.is_empty());
                    }
                }
            }
        }
    }

    #[tokio::test]
    async fn unknown_spa_paths_return_404_with_recovery_links_and_noindex() {
        for path in [
            "/does-not-exist",
            "/404",
            "/stats/typo",
            "/th/blog/missing",
            "/missing.js",
            "/missing/?lang=ar",
            "/%3Cscript%3E",
        ] {
            let uri: Uri = path.parse().unwrap();
            for method in [Method::GET, Method::HEAD] {
                let response = app_fallback_response(method.clone(), &uri, "https://bntpool.com");
                assert_eq!(response.status(), StatusCode::NOT_FOUND, "{method} {uri}");
                assert_eq!(
                    response.headers()[header::CONTENT_TYPE],
                    "text/html; charset=utf-8"
                );
                let body = axum::body::to_bytes(response.into_body(), usize::MAX)
                    .await
                    .unwrap();
                if method == Method::HEAD {
                    assert!(body.is_empty());
                } else {
                    let html = String::from_utf8(body.to_vec()).unwrap();
                    assert!(html.contains("<title>Page not found</title>"));
                    assert!(html.contains(r#"content="noindex, follow""#));
                    assert!(html.contains("<h1>Page not found</h1>"));
                    for link in [r#"href="/""#, r#"href="/start""#, r#"href="/blog/""#] {
                        assert!(html.contains(link));
                    }
                }
            }
            assert_eq!(
                app_fallback_response(Method::POST, &uri, "https://bntpool.com").status(),
                StatusCode::NOT_FOUND
            );
        }
    }

    #[test]
    fn injects_per_route_title_description_and_image() {
        let html = render_index_html("/checkpoints", "https://bntpool.com");
        assert!(html.contains("<title>Blocknet Checkpoints</title>"));
        assert!(html.contains(r#"<meta property="og:title" content="Blocknet Checkpoints" />"#));
        assert!(html.contains(
            r#"<meta property="og:image" content="https://bntpool.com/ui-assets/og-checkpoints.png" />"#
        ));
        assert!(html
            .contains(r#"<meta property="og:url" content="https://bntpool.com/checkpoints" />"#));
        assert!(html.contains(r#"<meta name="twitter:card" content="summary_large_image" />"#));
        // Marker must be fully consumed.
        assert!(!html.contains(SSR_META_MARKER));
    }

    #[test]
    fn unknown_routes_have_not_found_metadata() {
        let root = render_index_html("/", "https://bntpool.com");
        assert!(root.contains("<title>Blocknet Mining Pool</title>"));
        assert!(root.contains(r#"content="https://bntpool.com/ui-assets/og-dashboard.png""#));
        assert!(root.contains(r#"<meta property="og:url" content="https://bntpool.com/" />"#));

        let unknown = render_index_html("/does-not-exist", "https://bntpool.com");
        assert!(unknown.contains("<title>Page not found</title>"));
        assert!(unknown.contains(r#"content="https://bntpool.com/ui-assets/og-dashboard.png""#));
    }

    #[test]
    fn public_routes_are_indexable_and_operator_routes_are_not() {
        let root = render_index_html("/", "https://bntpool.com");
        assert!(root.contains(r#"<meta name="robots" content="index, follow" />"#));
        assert!(root.contains(r#"<link rel="canonical" href="https://bntpool.com/" />"#));
        assert!(!root.contains("noindex"));

        for path in ["/admin", "/does-not-exist"] {
            let html = render_index_html(path, "https://bntpool.com");
            assert!(
                html.contains(r#"<meta name="robots" content="noindex, follow" />"#),
                "{path}"
            );
        }
        for path in SITEMAP_PATHS {
            assert!(
                page_meta(path).index,
                "{path} is in the sitemap but not indexable"
            );
        }
    }

    /// Export the actual server HTML for browser checks without starting a pool or DB.
    #[test]
    #[ignore = "set BNTPOOL_UI_PREVIEW_DIR to export a disposable visual fixture"]
    fn export_public_pages_for_visual_check() {
        let dir = std::path::PathBuf::from(std::env::var("BNTPOOL_UI_PREVIEW_DIR").unwrap());
        std::fs::create_dir_all(&dir).unwrap();
        for path in SITEMAP_PATHS.iter().copied().chain(["/admin", "/missing"]) {
            let name = if path == "/" { "index" } else { &path[1..] };
            std::fs::write(
                dir.join(format!("{name}.html")),
                render_index_html(path, "https://bntpool.com"),
            )
            .unwrap();
        }
    }

    #[test]
    fn public_pages_have_useful_html_before_javascript() {
        for path in SITEMAP_PATHS {
            let html = render_index_html(path, "https://bntpool.com");
            assert_eq!(html.matches("<h1>").count(), 1, "{path}");
            assert!(html.contains(&html_escape(&page_meta(path).description)));
            for route in SITEMAP_PATHS {
                assert!(
                    html.contains(&format!("href=\"{route}\"")),
                    "{path} -> {route}"
                );
            }
            for guide in &page_meta(path).guides {
                assert!(
                    crate::blog::blog_sitemap_paths()
                        .contains(&guide.href.split('#').next().unwrap()),
                    "{}",
                    guide.href
                );
                assert!(html.contains(&format!("href=\"{}\"", guide.href)));
            }
            assert!(!html.contains(r#"<div id="root"></div>"#));
        }
        assert!(render_index_html("/start/", "https://bntpool.com").contains("Download Seine"));
        assert!(!render_index_html("/admin", "https://bntpool.com").contains("id=\"public-page\""));
    }

    #[test]
    fn base_url_comes_from_configured_pool_url() {
        assert_eq!(
            public_base_url("https://bntpool.com/"),
            "https://bntpool.com"
        );
        assert_eq!(
            public_base_url(" http://localhost:24783 "),
            "http://localhost:24783"
        );
        assert_eq!(public_base_url(""), "https://bntpool.com");
    }

    #[test]
    fn spoofed_host_headers_do_not_reach_page_metadata() {
        let uri: Uri = "/blocks".parse().unwrap();
        let response =
            app_fallback_response(Method::GET, &uri, &public_base_url("https://bntpool.com"));
        let body = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(axum::body::to_bytes(response.into_body(), usize::MAX))
            .unwrap();
        let html = String::from_utf8(body.to_vec()).unwrap();
        assert!(html.contains(r#"<meta property="og:url" content="https://bntpool.com/blocks" />"#));
        assert!(!html.contains("evil.example"));
    }

    #[test]
    fn security_headers_are_added_without_overriding_handlers() {
        use axum::routing::get;
        use axum::Router;
        use tower::ServiceExt;

        let app = Router::new()
            .route("/", get(|| async { "ok" }))
            .route(
                "/framed",
                get(|| async { ([(header::X_FRAME_OPTIONS, "SAMEORIGIN")], "ok") }),
            )
            .layer(axum::middleware::from_fn(apply_security_headers));
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let response = runtime
            .block_on(
                app.clone()
                    .oneshot(Request::builder().uri("/").body(Body::empty()).unwrap()),
            )
            .unwrap();
        let headers = response.headers();
        assert_eq!(headers[header::X_CONTENT_TYPE_OPTIONS], "nosniff");
        assert_eq!(headers[header::X_FRAME_OPTIONS], "DENY");
        assert_eq!(
            headers[header::REFERRER_POLICY],
            "strict-origin-when-cross-origin"
        );
        assert!(headers[header::CONTENT_SECURITY_POLICY]
            .to_str()
            .unwrap()
            .contains("frame-ancestors 'none'"));
        assert_eq!(
            headers
                .get_all(header::X_CONTENT_TYPE_OPTIONS)
                .iter()
                .count(),
            1
        );
        let framed = runtime
            .block_on(
                app.oneshot(
                    Request::builder()
                        .uri("/framed")
                        .body(Body::empty())
                        .unwrap(),
                ),
            )
            .unwrap();
        assert_eq!(framed.headers()[header::X_FRAME_OPTIONS], "SAMEORIGIN");
    }

    #[test]
    fn og_images_are_registered_for_every_route_card() {
        for path in [
            "/",
            "/start",
            "/checkpoints",
            "/luck",
            "/blocks",
            "/payouts",
            "/stats",
            "/status",
            "/admin",
        ] {
            let image = &page_meta(path).image;
            assert!(
                og_image_bytes(image).is_some(),
                "missing embedded og image for {path}: {image}"
            );
        }
    }
}
