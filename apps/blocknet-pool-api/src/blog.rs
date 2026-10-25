//! The bntpool.com blog: static pages pre-rendered by `frontend/scripts/build-blog.mjs` into
//! `frontend/dist/blog` and embedded at build time (see build.rs). They are plain files, so they
//! skip the public read limiter and response cache.

use axum::http::{header, HeaderValue, StatusCode, Uri};
use axum::response::{IntoResponse, Redirect, Response};

// Defines BLOG_FILES: &[(path, content type, bytes)] and BLOG_SITEMAP_PATHS: &[path].
include!(concat!(env!("OUT_DIR"), "/blog_files.rs"));

fn lookup(path: &str) -> Option<(&'static str, &'static [u8])> {
    BLOG_FILES
        .iter()
        .find(|(file, _, _)| *file == path)
        .map(|(_, content_type, bytes)| (*content_type, *bytes))
}

pub(crate) async fn handle_blog_root() -> Redirect {
    Redirect::permanent("/blog/")
}

pub(crate) async fn handle_blog(uri: Uri) -> Response {
    let path = uri.path();
    match lookup(path) {
        Some((content_type, bytes)) => {
            let cache = if content_type.starts_with("text/html") {
                "public, max-age=300"
            } else {
                "public, max-age=3600"
            };
            let mut response = bytes.into_response();
            let headers = response.headers_mut();
            headers.insert(header::CONTENT_TYPE, HeaderValue::from_static(content_type));
            headers.insert(header::CACHE_CONTROL, HeaderValue::from_static(cache));
            response
        }
        // Pages live at directory URLs; send /blog/<slug> to /blog/<slug>/.
        None if !path.ends_with('/') && lookup(&format!("{path}/")).is_some() => {
            Redirect::permanent(&format!("{path}/")).into_response()
        }
        None => StatusCode::NOT_FOUND.into_response(),
    }
}

/// Blog pages to list in /sitemap.xml, as site paths.
pub(crate) fn blog_sitemap_paths() -> &'static [&'static str] {
    BLOG_SITEMAP_PATHS
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn index_and_assets_are_embedded() {
        assert!(lookup("/blog/").is_some_and(|(ct, _)| ct.starts_with("text/html")));
        assert!(lookup("/blog/blog.css").is_some_and(|(ct, _)| ct.starts_with("text/css")));
        assert!(lookup("/blog/theme.js").is_some());
        assert!(blog_sitemap_paths().contains(&"/blog/"));
        for path in blog_sitemap_paths() {
            assert!(
                lookup(path).is_some(),
                "sitemap lists {path} but it isn't embedded"
            );
        }
    }
}
