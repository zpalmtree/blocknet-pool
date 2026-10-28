use axum::body::Body;
use axum::extract::{Path, Query, Request, State};
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
include!(concat!(env!("OUT_DIR"), "/ui_languages.rs"));
include!(concat!(env!("OUT_DIR"), "/ui_wasm.rs"));

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
struct PageMeta {
    title: String,
    description: String,
    image: String,
    index: bool,
}

static PAGE_METADATA: std::sync::LazyLock<std::collections::HashMap<String, PageMeta>> =
    std::sync::LazyLock::new(|| {
        serde_json::from_str(include_str!("../../../frontend/src/lib/page-meta.json"))
            .expect("valid public page metadata")
    });

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct PageLanguage {
    code: String,
    html_lang: String,
    og_locale: String,
    dir: String,
    pages: std::collections::HashMap<String, TranslatedPage>,
}

#[derive(serde::Deserialize)]
struct TranslatedPage {
    title: String,
    description: String,
    body: String,
}

static PAGE_LANGUAGES: std::sync::LazyLock<Vec<PageLanguage>> = std::sync::LazyLock::new(|| {
    serde_json::from_str(include_str!(concat!(
        env!("BLOCKNET_POOL_FRONTEND_DIST_DIR"),
        "/public-pages.json"
    )))
    .expect("valid localized public pages")
});

fn page_language(uri: &Uri) -> &'static PageLanguage {
    // Private and unknown routes never participate in the public language cluster.
    if !page_meta(uri.path()).index {
        return &PAGE_LANGUAGES[0];
    }
    let query = Query::<Vec<(String, String)>>::try_from_uri(uri).ok();
    let requested = query
        .as_ref()
        .and_then(|pairs| pairs.iter().find(|(key, _)| key == "lang"));
    let code = requested
        .map(|(_, value)| value.to_lowercase())
        .unwrap_or_default();
    if let Some(language) = PAGE_LANGUAGES
        .iter()
        .find(|language| language.code.eq_ignore_ascii_case(&code))
    {
        return language;
    }
    let parts: Vec<_> = code.split('-').collect();
    let code = match parts[0] {
        "zh" => {
            if parts
                .iter()
                .any(|part| matches!(*part, "hant" | "tw" | "hk" | "mo"))
            {
                "zh-Hant"
            } else {
                "zh-Hans"
            }
        }
        "pt" => "pt-BR",
        base => base,
    };
    PAGE_LANGUAGES
        .iter()
        .find(|language| language.code == code)
        .unwrap_or(&PAGE_LANGUAGES[0])
}

fn language_path(path: &str, code: &str) -> String {
    let path = normalize_path(path);
    if code == "en" {
        path.to_owned()
    } else {
        format!("{path}?lang={code}")
    }
}

fn page_meta(path: &str) -> &'static PageMeta {
    PAGE_METADATA
        .get(normalize_path(path))
        .unwrap_or(&PAGE_METADATA["/404"])
}

fn is_spa_route(path: &str) -> bool {
    let path = normalize_path(path);
    matches!(path, "/admin" | "/wallet") || SITEMAP_PATHS.contains(&path)
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

fn meta_tags(path: &str, base_url: &str, language: &PageLanguage) -> String {
    let meta = page_meta(path);
    let translated = language.pages.get(normalize_path(path));
    let title = html_escape(translated.map_or(&meta.title, |page| &page.title));
    let description = html_escape(translated.map_or(&meta.description, |page| &page.description));
    let site_name = html_escape(host_label(base_url));
    let url = html_escape(&format!(
        "{base_url}{}",
        language_path(path, &language.code)
    ));
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
    if meta.index {
        for alternate in PAGE_LANGUAGES.iter() {
            out.push_str(&format!(
                "{nl}<link rel=\"alternate\" hreflang=\"{}\" href=\"{}\" />",
                alternate.html_lang,
                html_escape(&format!(
                    "{base_url}{}",
                    language_path(path, &alternate.code)
                ))
            ));
        }
        out.push_str(&format!(
            "{nl}<link rel=\"alternate\" hreflang=\"x-default\" href=\"{}\" />",
            html_escape(&format!("{base_url}{}", normalize_path(path)))
        ));
    }
    out.push_str(&format!(
        "{nl}<meta property=\"og:locale\" content=\"{}\" />",
        language.og_locale
    ));
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
    let uri: Uri = path.parse().expect("valid page URI");
    let language = page_language(&uri);
    let path = uri.path();
    let tags = meta_tags(path, base_url, language);
    let html = if UI_INDEX_HTML.contains(SSR_META_MARKER) {
        UI_INDEX_HTML.replace(SSR_META_MARKER, &tags)
    } else {
        // Defensive fallback if the build ever drops the marker: inject into <head>.
        UI_INDEX_HTML.replacen("<head>", &format!("<head>\n    {tags}"), 1)
    };
    let html = html.replacen(
        r#"<html lang="en">"#,
        &format!(
            r#"<html lang="{}" dir="{}">"#,
            language.html_lang, language.dir
        ),
        1,
    );
    if SITEMAP_PATHS.contains(&normalize_path(path)) {
        html.replace(
            r#"<div id="root"></div>"#,
            &language.pages[normalize_path(path)].body,
        )
    } else if is_spa_route(path) {
        html
    } else {
        // Keep recovery links usable before JavaScript loads or if it is disabled.
        html.replace(r#"<div id="root"></div>"#, r#"<div id="root"><main class="container" id="page-not-found"><div class="page-header"><span class="page-kicker">404</span><h1>Page not found</h1><p class="page-intro">This link does not lead to a page. Open the dashboard, get started, or browse the mining guides.</p></div><div class="page-actions"><a class="btn btn-primary" href="/">Dashboard</a><a class="btn btn-secondary" href="/start">Get Started</a><a class="btn btn-secondary" href="/blog/">Blog</a></div></main></div>"#)
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

pub(crate) async fn handle_ui(State(state): State<ApiState>, method: Method, uri: Uri) -> Response {
    app_fallback_response(method, &uri, &state.public_base_url())
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
        let mut response = ui_response(
            base_url,
            uri.path_and_query()
                .map_or(uri.path(), |value| value.as_str()),
        );
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
/// `'wasm-unsafe-eval'` lets pages compile the WebAssembly modules served from
/// this origin; it does not allow JavaScript `eval`.
pub(crate) const CONTENT_SECURITY_POLICY: &str = "default-src 'self'; \
script-src 'self' 'wasm-unsafe-eval'; \
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

fn sitemap_entries(base_url: &str) -> String {
    SITEMAP_PATHS
        .iter()
        .flat_map(|path| {
            PAGE_LANGUAGES
                .iter()
                .map(move |locale| language_path(path, &locale.code))
        })
        .chain(
            crate::blog::blog_sitemap_paths()
                .iter()
                .map(|path| (*path).to_owned()),
        )
        .map(|path| {
            format!(
                "  <url><loc>{}</loc></url>\n",
                html_escape(&format!("{base_url}{path}"))
            )
        })
        .collect()
}

pub(crate) async fn handle_sitemap_xml(State(state): State<ApiState>) -> impl IntoResponse {
    let base_url = state.public_base_url();
    let urls = sitemap_entries(&base_url);
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
    if let Some((_, bytes)) = UI_WASM.iter().find(|(path, _)| *path == name) {
        return (
            [
                (header::CONTENT_TYPE, "application/wasm"),
                (header::CACHE_CONTROL, "public, max-age=31536000, immutable"),
            ],
            *bytes,
        )
            .into_response();
    }
    if let Some((_, bytes)) = UI_LANGUAGES.iter().find(|(path, _)| *path == name) {
        return (
            [
                (header::CONTENT_TYPE, "application/json; charset=utf-8"),
                (header::CACHE_CONTROL, "public, max-age=31536000, immutable"),
            ],
            *bytes,
        )
            .into_response();
    }
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
    async fn every_public_language_has_localized_html_and_reciprocal_search_links() {
        assert_eq!(PAGE_LANGUAGES.len(), 20);
        let sitemap = sitemap_entries("https://pool.example");
        assert_eq!(
            sitemap.matches("<loc>").count(),
            160 + crate::blog::blog_sitemap_paths().len()
        );
        for locale in PAGE_LANGUAGES.iter() {
            for path in SITEMAP_PATHS {
                let canonical =
                    format!("https://pool.example{}", language_path(path, &locale.code));
                let uri: Uri =
                    format!("{path}?worker=example&lang={}&utm_source=test", locale.code)
                        .parse()
                        .unwrap();
                let response = app_fallback_response(Method::GET, &uri, "https://pool.example");
                assert_eq!(response.status(), StatusCode::OK);
                let body = axum::body::to_bytes(response.into_body(), usize::MAX)
                    .await
                    .unwrap();
                let html = String::from_utf8(body.to_vec()).unwrap();
                assert!(html.contains(&format!(
                    r#"<html lang="{}" dir="{}">"#,
                    locale.html_lang, locale.dir
                )));
                assert!(html.contains(&format!(r#"<link rel="canonical" href="{canonical}" />"#)));
                assert!(html.contains(&format!(
                    "<title>{}</title>",
                    html_escape(&locale.pages[*path].title)
                )));
                let heading = if *path == "/" && locale.code == "en" {
                    "Blocknet Mining Pool"
                } else {
                    &locale.pages[*path].title
                };
                assert!(html.contains(&format!("<h1>{}</h1>", html_escape(heading))));
                assert!(html.contains(&html_escape(&locale.pages[*path].description)));
                assert_eq!(html.matches("<h1>").count(), 1);
                assert_eq!(
                    html.matches(r#"<link rel="alternate" hreflang="#).count(),
                    21
                );
                assert!(!html.contains("utm_source"));
                assert!(sitemap.contains(&format!("<loc>{canonical}</loc>")));
                for other in PAGE_LANGUAGES.iter() {
                    assert!(html.contains(&format!(
                        r#"<link rel="alternate" hreflang="{}" href="https://pool.example{}" />"#,
                        other.html_lang,
                        language_path(path, &other.code)
                    )));
                }
            }
        }
    }

    #[test]
    fn language_queries_match_the_browser_and_do_not_create_private_alternates() {
        for (query, expected) in [
            ("lang=th", "th"),
            ("lang=AR", "ar"),
            ("lang=es-MX", "es"),
            ("lang=zh-TW", "zh-Hant"),
            ("lang=zh-CN", "zh-Hans"),
            ("lang=pt-PT", "pt-BR"),
            ("lang=%74h", "th"),
            ("lang=th&lang=ar", "th"),
            ("lang=unknown", "en"),
            ("lang=javascript%3Aalert(1)", "en"),
            ("lang=", "en"),
        ] {
            let uri: Uri = format!("/?{query}").parse().unwrap();
            assert_eq!(page_language(&uri).code, expected, "{query}");
        }
        for path in ["/admin?lang=ar", "/unknown?lang=th"] {
            let html = render_index_html(path, "https://pool.example");
            assert!(html.contains(r#"content="noindex, follow""#));
            assert!(!html.contains(r#"rel="alternate""#));
        }
    }

    #[tokio::test]
    async fn hashed_wasm_assets_are_served_as_wasm_and_cacheable() {
        for (name, expected) in UI_WASM {
            assert!(name.starts_with("assets/") && name.ends_with(".wasm"));
            let response = handle_ui_asset(Path((*name).to_owned())).await;
            assert_eq!(response.status(), StatusCode::OK);
            assert_eq!(response.headers()[header::CONTENT_TYPE], "application/wasm");
            assert_eq!(
                response.headers()[header::CACHE_CONTROL],
                "public, max-age=31536000, immutable"
            );
            let body = axum::body::to_bytes(response.into_body(), usize::MAX)
                .await
                .unwrap();
            assert_eq!(body.as_ref(), *expected);
        }
    }

    #[tokio::test]
    async fn hashed_language_assets_are_embedded_complete_and_cacheable() {
        assert_eq!(UI_LANGUAGES.len(), 19);
        let english: serde_json::Value =
            serde_json::from_str(include_str!("../../../frontend/src/i18n/messages/en.json"))
                .unwrap();
        for (name, expected) in UI_LANGUAGES {
            let response = handle_ui_asset(Path((*name).to_owned())).await;
            assert_eq!(response.status(), StatusCode::OK);
            assert_eq!(
                response.headers()[header::CONTENT_TYPE],
                "application/json; charset=utf-8"
            );
            assert_eq!(
                response.headers()[header::CACHE_CONTROL],
                "public, max-age=31536000, immutable"
            );
            let body = axum::body::to_bytes(response.into_body(), usize::MAX)
                .await
                .unwrap();
            assert_eq!(body.as_ref(), *expected);
            let catalog: serde_json::Value = serde_json::from_slice(&body).unwrap();
            assert_eq!(
                catalog.as_object().unwrap().keys().collect::<Vec<_>>(),
                english.as_object().unwrap().keys().collect::<Vec<_>>()
            );
        }
        for name in [
            "assets/missing.json",
            "assets/../../index.html",
            "index.html",
            "chunks/missing.js",
        ] {
            assert_eq!(
                handle_ui_asset(Path(name.to_owned())).await.status(),
                StatusCode::NOT_FOUND
            );
        }
    }

    #[tokio::test]
    async fn known_spa_routes_keep_success_status_with_queries_and_trailing_slashes() {
        for path in SITEMAP_PATHS.iter().copied().chain(["/admin", "/wallet"]) {
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
        assert!(root.contains("<title>Blocknet Mining Pool — 0% Fee</title>"));
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
        for language in PAGE_LANGUAGES.iter() {
            let locale_dir = dir.join(&language.code);
            std::fs::create_dir_all(&locale_dir).unwrap();
            for path in SITEMAP_PATHS.iter().copied().chain(["/admin", "/missing"]) {
                let name = if path == "/" { "index" } else { &path[1..] };
                std::fs::write(
                    locale_dir.join(format!("{name}.html")),
                    render_index_html(&language_path(path, &language.code), "https://bntpool.com"),
                )
                .unwrap();
            }
        }
        std::fs::write(
            dir.join("sitemap-entries.xml"),
            sitemap_entries("https://bntpool.com"),
        )
        .unwrap();
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
            let pages: serde_json::Value =
                serde_json::from_str(include_str!("../../../frontend/src/lib/page-meta.json"))
                    .unwrap();
            for guide in pages[path]["guides"].as_array().unwrap() {
                let href = guide["href"].as_str().unwrap();
                assert!(
                    crate::blog::blog_sitemap_paths().contains(&href.split('#').next().unwrap()),
                    "{}",
                    href
                );
                assert!(html.contains(&format!("href=\"{}\"", href)));
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
        let csp = headers[header::CONTENT_SECURITY_POLICY].to_str().unwrap();
        assert!(csp.contains("frame-ancestors 'none'"));
        assert!(csp.contains("script-src 'self' 'wasm-unsafe-eval';"));
        assert!(
            !csp.contains(" 'unsafe-eval'"),
            "JavaScript eval stays disallowed"
        );
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
