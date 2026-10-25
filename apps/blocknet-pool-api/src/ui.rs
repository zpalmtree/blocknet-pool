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
    ("og-dashboard.png", include_bytes!("ui/assets/og-dashboard.png")),
    ("og-start.png", include_bytes!("ui/assets/og-start.png")),
    ("og-checkpoints.png", include_bytes!("ui/assets/og-checkpoints.png")),
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

const DEFAULT_DESCRIPTION: &str =
    "Blocknet mining pool with low fees and fast PPLNS payouts. Live hashrate, blocks, payouts, and pool status.";

struct PageMeta {
    /// Used for `<title>`, `og:title`, and `twitter:title`.
    title: &'static str,
    description: &'static str,
    /// Social card filename under `/ui-assets/`.
    image: &'static str,
    /// Whether search engines may index the route. Operator and unknown paths stay out.
    index: bool,
}

/// Resolve per-route preview metadata. Crawlers never run the SPA, so this is
/// the only place page-specific titles/descriptions/images reach them.
fn page_meta(path: &str) -> PageMeta {
    match normalize_path(path) {
        "/" => PageMeta {
            title: "Blocknet Mining Pool",
            description: DEFAULT_DESCRIPTION,
            image: "og-dashboard.png",
            index: true,
        },
        "/start" => PageMeta {
            title: "Mine Blocknet With Seine",
            description:
                "Start mining Blocknet with the Seine miner. Quick setup, low fees, and fast PPLNS payouts.",
            image: "og-start.png",
            index: true,
        },
        "/checkpoints" => PageMeta {
            title: "Blocknet Checkpoints",
            description: "Pool daemon checkpoints for fast initial sync and trusted chain pinning.",
            image: "og-checkpoints.png",
            index: true,
        },
        "/luck" => PageMeta {
            title: "Blocknet Pool Luck",
            description: "Round luck and effort history for the Blocknet mining pool.",
            image: "og-luck.png",
            index: true,
        },
        "/blocks" => PageMeta {
            title: "Recent Blocknet Blocks",
            description: "Recent blocks found by the Blocknet mining pool, with rewards and confirmations.",
            image: "og-blocks.png",
            index: true,
        },
        "/payouts" => PageMeta {
            title: "Blocknet Pool Payouts",
            description: "Recent payouts from the Blocknet mining pool.",
            image: "og-payouts.png",
            index: true,
        },
        "/stats" => PageMeta {
            title: "Blocknet Miner Stats",
            description: "Look up your Blocknet miner hashrate, shares, and balance.",
            image: "og-stats.png",
            index: true,
        },
        "/status" => PageMeta {
            title: "Blocknet Pool Status",
            description: "Live operational status and health of the Blocknet mining pool.",
            image: "og-status.png",
            index: true,
        },
        "/admin" => PageMeta {
            title: "Pool Admin Dashboard",
            description: "Operator dashboard for the Blocknet mining pool.",
            image: "og-admin.png",
            index: false,
        },
        _ => PageMeta {
            title: "Blocknet Mining Pool",
            description: DEFAULT_DESCRIPTION,
            image: "og-dashboard.png",
            index: false,
        },
    }
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
    let title = html_escape(meta.title);
    let description = html_escape(meta.description);
    let site_name = html_escape(host_label(base_url));
    let url = html_escape(&format!("{base_url}{}", normalize_path(path)));
    let image = html_escape(&format!("{base_url}/ui-assets/{}", meta.image));

    let nl = "\n    ";
    let robots = if meta.index { "index, follow" } else { "noindex, follow" };

    let mut out = format!("<title>{title}</title>");
    out.push_str(&format!("{nl}<meta name=\"description\" content=\"{description}\" />"));
    out.push_str(&format!("{nl}<meta name=\"robots\" content=\"{robots}\" />"));
    out.push_str(&format!("{nl}<link rel=\"canonical\" href=\"{url}\" />"));
    out.push_str(&format!("{nl}<meta property=\"og:type\" content=\"website\" />"));
    out.push_str(&format!("{nl}<meta property=\"og:site_name\" content=\"{site_name}\" />"));
    out.push_str(&format!("{nl}<meta property=\"og:title\" content=\"{title}\" />"));
    out.push_str(&format!("{nl}<meta property=\"og:description\" content=\"{description}\" />"));
    out.push_str(&format!("{nl}<meta property=\"og:url\" content=\"{url}\" />"));
    out.push_str(&format!("{nl}<meta property=\"og:image\" content=\"{image}\" />"));
    out.push_str(&format!("{nl}<meta property=\"og:image:width\" content=\"1200\" />"));
    out.push_str(&format!("{nl}<meta property=\"og:image:height\" content=\"630\" />"));
    out.push_str(&format!("{nl}<meta name=\"twitter:card\" content=\"summary_large_image\" />"));
    out.push_str(&format!("{nl}<meta name=\"twitter:title\" content=\"{title}\" />"));
    out.push_str(&format!("{nl}<meta name=\"twitter:description\" content=\"{description}\" />"));
    out.push_str(&format!("{nl}<meta name=\"twitter:image\" content=\"{image}\" />"));
    out
}

fn render_index_html(path: &str, base_url: &str) -> String {
    let tags = meta_tags(path, base_url);
    if UI_INDEX_HTML.contains(SSR_META_MARKER) {
        UI_INDEX_HTML.replace(SSR_META_MARKER, &tags)
    } else {
        // Defensive fallback if the build ever drops the marker: inject into <head>.
        UI_INDEX_HTML.replacen("<head>", &format!("<head>\n    {tags}"), 1)
    }
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
    "/", "/start", "/blocks", "/payouts", "/stats", "/luck", "/status", "/checkpoints",
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

    #[test]
    fn injects_per_route_title_description_and_image() {
        let html = render_index_html("/checkpoints", "https://bntpool.com");
        assert!(html.contains("<title>Blocknet Checkpoints</title>"));
        assert!(html.contains(
            r#"<meta property="og:title" content="Blocknet Checkpoints" />"#
        ));
        assert!(html.contains(
            r#"<meta property="og:image" content="https://bntpool.com/ui-assets/og-checkpoints.png" />"#
        ));
        assert!(html.contains(r#"<meta property="og:url" content="https://bntpool.com/checkpoints" />"#));
        assert!(html.contains(r#"<meta name="twitter:card" content="summary_large_image" />"#));
        // Marker must be fully consumed.
        assert!(!html.contains(SSR_META_MARKER));
    }

    #[test]
    fn root_and_unknown_routes_fall_back_to_default_card() {
        let root = render_index_html("/", "https://bntpool.com");
        assert!(root.contains("<title>Blocknet Mining Pool</title>"));
        assert!(root.contains(r#"content="https://bntpool.com/ui-assets/og-dashboard.png""#));
        assert!(root.contains(r#"<meta property="og:url" content="https://bntpool.com/" />"#));

        let unknown = render_index_html("/does-not-exist", "https://bntpool.com");
        assert!(unknown.contains("<title>Blocknet Mining Pool</title>"));
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
            assert!(html.contains(r#"<meta name="robots" content="noindex, follow" />"#), "{path}");
        }
        for path in SITEMAP_PATHS {
            assert!(page_meta(path).index, "{path} is in the sitemap but not indexable");
        }
    }

    #[test]
    fn base_url_comes_from_configured_pool_url() {
        assert_eq!(public_base_url("https://bntpool.com/"), "https://bntpool.com");
        assert_eq!(public_base_url(" http://localhost:24783 "), "http://localhost:24783");
        assert_eq!(public_base_url(""), "https://bntpool.com");
    }

    #[test]
    fn spoofed_host_headers_do_not_reach_page_metadata() {
        let uri: Uri = "/blocks".parse().unwrap();
        let response = app_fallback_response(Method::GET, &uri, &public_base_url("https://bntpool.com"));
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
            .block_on(app.clone().oneshot(Request::builder().uri("/").body(Body::empty()).unwrap()))
            .unwrap();
        let headers = response.headers();
        assert_eq!(headers[header::X_CONTENT_TYPE_OPTIONS], "nosniff");
        assert_eq!(headers[header::X_FRAME_OPTIONS], "DENY");
        assert_eq!(headers[header::REFERRER_POLICY], "strict-origin-when-cross-origin");
        assert!(headers[header::CONTENT_SECURITY_POLICY]
            .to_str()
            .unwrap()
            .contains("frame-ancestors 'none'"));
        assert_eq!(headers.get_all(header::X_CONTENT_TYPE_OPTIONS).iter().count(), 1);
        let framed = runtime
            .block_on(app.oneshot(Request::builder().uri("/framed").body(Body::empty()).unwrap()))
            .unwrap();
        assert_eq!(framed.headers()[header::X_FRAME_OPTIONS], "SAMEORIGIN");
    }

    #[test]
    fn og_images_are_registered_for_every_route_card() {
        for path in [
            "/", "/start", "/checkpoints", "/luck", "/blocks", "/payouts", "/stats", "/status",
            "/admin",
        ] {
            let image = page_meta(path).image;
            assert!(
                og_image_bytes(image).is_some(),
                "missing embedded og image for {path}: {image}"
            );
        }
    }
}
