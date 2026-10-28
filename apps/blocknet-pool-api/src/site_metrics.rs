//! Best-effort website counters, separate from miner telemetry and accounting.
//! No cookies, wallet addresses, URLs, IPs or individual event records are persisted.
use axum::{
    body::Bytes,
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use parking_lot::Mutex;
use pool_runtime::store::PoolStore;
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

const META_KEY: &str = "website_counts_v1";
const RETENTION_DAYS: u64 = 30;
const MAX_KEYS: usize = 65_000;
const ARTICLES: &str = include_str!("site_articles.json");
const LOCALES: &[&str] = &[
    "en", "id", "es", "de", "fr", "pt-BR", "ru", "it", "pl", "tr", "vi", "ar", "nl", "uk", "el",
    "ja", "ko", "th", "zh-Hans", "zh-Hant",
];
type Counts = BTreeMap<String, u64>;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Event {
    article: String,
    locale: String,
    action: String,
    #[serde(default)]
    source: Option<String>,
}

struct Inner {
    ready: bool,
    counts: Counts,
    generation: u64,
    persisted: u64,
    last_persisted_at: Option<u64>,
    window: Instant,
    received: usize,
}

pub(crate) struct SiteMetrics {
    inner: Mutex<Inner>,
    articles: BTreeMap<String, Vec<String>>,
}

impl Default for SiteMetrics {
    fn default() -> Self {
        Self {
            inner: Mutex::new(Inner {
                ready: false,
                counts: Counts::new(),
                generation: 0,
                persisted: 0,
                last_persisted_at: None,
                window: Instant::now(),
                received: 0,
            }),
            articles: serde_json::from_str(ARTICLES).expect("checked website article registry"),
        }
    }
}

fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}
fn prune(counts: &mut Counts, day: u64) {
    counts.retain(|key, _| {
        key.split('|')
            .next()
            .and_then(|s| s.parse::<u64>().ok())
            .is_some_and(|d| d <= day && d > day.saturating_sub(RETENTION_DAYS))
    });
}
fn site(headers: &HeaderMap) -> Option<&'static str> {
    match headers.get("origin")?.to_str().ok()? {
        "https://seineminer.com" => Some("seineminer.com"),
        "https://bntpool.com" => Some("bntpool.com"),
        _ => None,
    }
}

impl SiteMetrics {
    // Fail closed until the saved counters are loaded: a DB outage must never erase them.
    pub(crate) fn start(self: &Arc<Self>, store: Arc<PoolStore>) {
        let metrics = Arc::clone(self);
        tokio::spawn(async move {
            loop {
                let db = Arc::clone(&store);
                let loaded = tokio::task::spawn_blocking(move || -> anyhow::Result<Counts> {
                    let counts = match db.get_meta(META_KEY)? {
                        Some(bytes) => serde_json::from_slice(&bytes)?,
                        None => Counts::new(),
                    };
                    Ok(counts)
                })
                .await;
                if let Ok(Ok(mut counts)) = loaded {
                    prune(&mut counts, now() / 86400);
                    if counts.len() <= MAX_KEYS {
                        let mut inner = metrics.inner.lock();
                        inner.counts = counts;
                        inner.ready = true;
                        inner.generation = 1; // Also persist retention pruning.
                        break;
                    }
                }
                tracing::warn!("website counters unavailable; retrying load");
                tokio::time::sleep(Duration::from_secs(30)).await;
            }
            loop {
                tokio::time::sleep(Duration::from_secs(30)).await;
                let snapshot = {
                    let mut inner = metrics.inner.lock();
                    let before = inner.counts.len();
                    prune(&mut inner.counts, now() / 86400);
                    if before != inner.counts.len() {
                        inner.generation += 1;
                    }
                    (inner.generation != inner.persisted)
                        .then(|| (inner.generation, inner.counts.clone()))
                };
                let Some((generation, counts)) = snapshot else {
                    continue;
                };
                let db = Arc::clone(&store);
                // One writer, one blocking DB operation per batch; no request-path database work.
                let saved = tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
                    db.set_meta(META_KEY, &serde_json::to_vec(&counts)?)
                })
                .await;
                if matches!(saved, Ok(Ok(()))) {
                    let mut inner = metrics.inner.lock();
                    inner.persisted = generation;
                    inner.last_persisted_at = Some(now());
                } else {
                    tracing::warn!("website counter flush failed; retaining batch for retry");
                }
            }
        });
    }

    fn record(&self, site: &str, bytes: &[u8], timestamp: u64) -> StatusCode {
        // Independent global budget: website events cannot spend the miner API's limit.
        let mut inner = self.inner.lock();
        if inner.window.elapsed() >= Duration::from_secs(1) {
            inner.window = Instant::now();
            inner.received = 0;
        }
        if inner.received >= 100 {
            return StatusCode::TOO_MANY_REQUESTS;
        }
        inner.received += 1;
        if bytes.len() > 1024 {
            return StatusCode::PAYLOAD_TOO_LARGE;
        }
        let Ok(event) = serde_json::from_slice::<Event>(bytes) else {
            return StatusCode::BAD_REQUEST;
        };
        if !LOCALES.contains(&event.locale.as_str())
            || ![
                "view",
                "download",
                "pool_setup",
                "setup_guide",
                "worker_stats",
            ]
            .contains(&event.action.as_str())
            || event.source.as_deref().is_some_and(|source| {
                !["search", "direct", "internal", "referral"].contains(&source)
            })
            || !self
                .articles
                .get(site)
                .is_some_and(|slugs| slugs.contains(&event.article))
        {
            return StatusCode::BAD_REQUEST;
        }
        if !inner.ready {
            return StatusCode::SERVICE_UNAVAILABLE;
        }
        let day = timestamp / 86400;
        let mut key = format!(
            "{day}|{site}|{}|{}|{}",
            event.article, event.locale, event.action
        );
        if let Some(source) = event.source {
            key.push('|');
            key.push_str(&source);
        }
        if inner.counts.len() >= MAX_KEYS && !inner.counts.contains_key(&key) {
            return StatusCode::SERVICE_UNAVAILABLE;
        }
        let value = inner.counts.entry(key).or_default();
        *value = value.saturating_add(1);
        inner.generation += 1;
        StatusCode::ACCEPTED
    }

    pub(crate) fn collect(&self, headers: HeaderMap, bytes: Bytes) -> Response {
        let Some(site) = site(&headers) else {
            return StatusCode::FORBIDDEN.into_response();
        };
        let status = self.record(site, &bytes, now());
        let mut response = status.into_response();
        response
            .headers_mut()
            .insert("access-control-allow-origin", headers["origin"].clone());
        response
            .headers_mut()
            .insert("vary", "Origin".parse().unwrap());
        response
            .headers_mut()
            .insert("cache-control", "no-store".parse().unwrap());
        response
    }

    pub(crate) fn report(&self) -> Response {
        #[derive(Serialize)]
        struct Report {
            ready: bool,
            retention_days: u64,
            last_persisted_at: Option<u64>,
            counts: Counts,
        }
        let inner = self.inner.lock();
        let mut counts = inner.counts.clone();
        prune(&mut counts, now() / 86400);
        let mut response = Json(Report {
            ready: inner.ready,
            retention_days: RETENTION_DAYS,
            last_persisted_at: inner.last_persisted_at,
            counts,
        })
        .into_response();
        response
            .headers_mut()
            .insert("cache-control", "no-store".parse().unwrap());
        response
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn event() -> Vec<u8> {
        br#"{"article":"track-your-miner-on-bntpool","locale":"en","action":"view"}"#.to_vec()
    }
    #[tokio::test]
    async fn http_collector_enforces_body_limit_and_cors() {
        use axum::http::Request;
        use axum::{
            body::{to_bytes, Body},
            extract::{DefaultBodyLimit, State},
            routing::post,
            Router,
        };
        use tower::ServiceExt;
        async fn collect(
            State(metrics): State<Arc<SiteMetrics>>,
            headers: HeaderMap,
            body: Bytes,
        ) -> Response {
            metrics.collect(headers, body)
        }
        let metrics = Arc::new(SiteMetrics::default());
        metrics.inner.lock().ready = true;
        let app = Router::new()
            .route("/events", post(collect).layer(DefaultBodyLimit::max(1024)))
            .with_state(metrics);
        for (origin, body, expected) in [
            ("https://seineminer.com", event(), StatusCode::BAD_REQUEST), // Wrong site's article.
            ("https://bntpool.com", event(), StatusCode::ACCEPTED),
            ("https://evil.test", event(), StatusCode::FORBIDDEN),
            (
                "https://bntpool.com",
                vec![b'a'; 1025],
                StatusCode::PAYLOAD_TOO_LARGE,
            ),
        ] {
            let response = app
                .clone()
                .oneshot(
                    Request::post("/events")
                        .header("Origin", origin)
                        .body(Body::from(body))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), expected);
            if expected == StatusCode::ACCEPTED {
                assert_eq!(response.headers()["access-control-allow-origin"], origin);
                assert_eq!(response.headers()["cache-control"], "no-store");
                assert!(to_bytes(response.into_body(), 1024)
                    .await
                    .unwrap()
                    .is_empty());
            } else if expected == StatusCode::FORBIDDEN {
                assert!(response
                    .headers()
                    .get("access-control-allow-origin")
                    .is_none());
            }
        }
    }
    #[test]
    fn strict_origins_and_fields() {
        let metrics = SiteMetrics::default();
        let mut headers = HeaderMap::new();
        for origin in [
            "null",
            "https://bntpool.com.evil.test",
            "http://bntpool.com",
            "https://localhost",
        ] {
            headers.insert("origin", origin.parse().unwrap());
            assert!(site(&headers).is_none());
        }
        headers.insert("origin", "https://bntpool.com".parse().unwrap());
        assert_eq!(site(&headers), Some("bntpool.com"));
        for bad in [
            r#"{"article":"not-published","locale":"en","action":"view"}"#,
            r#"{"article":"track-your-miner-on-bntpool","locale":"en","action":"view","wallet":"private"}"#,
            r#"{"article":"track-your-miner-on-bntpool","locale":"xx","action":"view"}"#,
            r#"{"article":"track-your-miner-on-bntpool","locale":"en","action":"https://private"}"#,
        ] {
            assert_eq!(
                metrics.record("bntpool.com", bad.as_bytes(), now()),
                StatusCode::BAD_REQUEST
            );
        }
        assert_eq!(
            metrics.record("bntpool.com", &vec![b'a'; 1025], now()),
            StatusCode::PAYLOAD_TOO_LARGE
        );
        assert_eq!(
            metrics.record("bntpool.com", &event(), now()),
            StatusCode::SERVICE_UNAVAILABLE
        );
    }
    #[test]
    fn landing_events_accept_only_known_pages_actions_and_source_buckets() {
        let metrics = SiteMetrics::default();
        metrics.inner.lock().ready = true;
        for (site, page) in [
            ("seineminer.com", "home"),
            ("bntpool.com", "home"),
            ("bntpool.com", "pool-start"),
        ] {
            let event = serde_json::json!({"article": page, "locale": "en", "action": "setup_guide", "source": "search"});
            assert_eq!(
                metrics.record(site, &serde_json::to_vec(&event).unwrap(), now()),
                StatusCode::ACCEPTED
            );
        }
        for extra in [
            serde_json::json!({"source": "https://google.com/?q=secret"}),
            serde_json::json!({"source": "search", "wallet": "secret"}),
        ] {
            let mut event =
                serde_json::json!({"article": "home", "locale": "en", "action": "view"});
            event
                .as_object_mut()
                .unwrap()
                .extend(extra.as_object().unwrap().clone());
            assert_eq!(
                metrics.record("bntpool.com", &serde_json::to_vec(&event).unwrap(), now()),
                StatusCode::BAD_REQUEST
            );
        }
        assert!(metrics
            .inner
            .lock()
            .counts
            .keys()
            .all(|key| key.ends_with("|search")));
        // Older deployed article scripts still use five-part keys.
        assert_eq!(
            metrics.record("bntpool.com", &event(), now()),
            StatusCode::ACCEPTED
        );
    }
    #[test]
    fn counters_are_bounded_and_roundtrip_without_identifiers() {
        let metrics = SiteMetrics::default();
        metrics.inner.lock().ready = true;
        for _ in 0..100 {
            assert_eq!(
                metrics.record("bntpool.com", &event(), now()),
                StatusCode::ACCEPTED
            );
        }
        assert_eq!(
            metrics.record("bntpool.com", &event(), now()),
            StatusCode::TOO_MANY_REQUESTS
        );
        let inner = metrics.inner.lock();
        assert_eq!(inner.counts.len(), 1);
        assert_eq!(inner.counts.values().next(), Some(&100));
        let encoded = serde_json::to_vec(&inner.counts).unwrap();
        assert_eq!(
            serde_json::from_slice::<Counts>(&encoded).unwrap(),
            inner.counts
        );
    }
    #[test]
    fn retention_excludes_old_and_future_days() {
        let mut counts: Counts = ["70|a", "71|b", "100|c", "101|d", "invalid"]
            .into_iter()
            .map(|s| (s.to_owned(), 1))
            .collect();
        prune(&mut counts, 100);
        assert_eq!(
            counts.keys().cloned().collect::<Vec<_>>(),
            vec!["100|c", "71|b"]
        );
    }
}
