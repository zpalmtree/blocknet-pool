use axum::body::Body;
use axum::extract::Path;
use axum::http::{header, HeaderValue, Method, StatusCode, Uri};
use axum::response::{Html, IntoResponse, Response};

use crate::api::error_response;

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
const UI_FAVICON_SVG: &str = r##"<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 32 32" role="img" aria-label="Blocknet Pool"><rect x="4" y="4" width="24" height="24" rx="4" fill="#16a34a"/><rect x="9" y="9" width="14" height="14" rx="2" fill="#fff" opacity=".9"/><rect x="12" y="12" width="8" height="8" rx="1" fill="#16a34a"/></svg>"##;

fn ui_response() -> Response {
    let mut response = Html(UI_INDEX_HTML).into_response();
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-cache"));
    response
}

pub(crate) async fn handle_ui() -> Response {
    ui_response()
}

pub(crate) async fn handle_app_fallback(method: Method, uri: Uri) -> Response {
    if is_api_request_path(uri.path()) {
        return error_response(StatusCode::NOT_FOUND, "not found");
    }

    if matches!(method, Method::GET | Method::HEAD) {
        let mut response = ui_response();
        if method == Method::HEAD {
            *response.body_mut() = Body::empty();
        }
        return response;
    }

    StatusCode::NOT_FOUND.into_response()
}

pub(crate) fn is_api_request_path(path: &str) -> bool {
    path == "/api" || path.starts_with("/api/")
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
        _ => StatusCode::NOT_FOUND.into_response(),
    }
}
