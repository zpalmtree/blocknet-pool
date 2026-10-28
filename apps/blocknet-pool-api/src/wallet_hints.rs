//! Payout-hint feed for the hosted wallet service
//! (contract in the private wallet repo). Off unless
//! `wallet_hints.enabled`; callers need the bearer token from a 0600 file.
//! The pool does not know which payees are hosted wallets, so every payout
//! recipient is listed and the wallet service filters by its own addresses.
use std::{path::Path, sync::Arc};

use anyhow::{bail, Context, Result};
use axum::{
    extract::{Query, State},
    http::{header, HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::get,
    Json, Router,
};
use pool_runtime::store::PoolStore;
use serde::Deserialize;
use serde_json::json;

const DEFAULT_LIMIT: i64 = 200;
const MAX_LIMIT: i64 = 500;

#[derive(Clone, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct WalletHintsConfig {
    /// Explicit opt-in; a token file alone never enables the feed.
    pub enabled: bool,
    pub token_file: String,
}

#[derive(Clone)]
struct HintsState {
    store: Arc<PoolStore>,
    expected_auth: Arc<Vec<u8>>,
}

fn load_token(path: &Path) -> Result<String> {
    let metadata = std::fs::metadata(path).context("wallet hints token")?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if metadata.permissions().mode() & 0o077 != 0 {
            bail!("wallet hints token must have mode 0600");
        }
    }
    if !metadata.is_file() {
        bail!("wallet hints token must be a regular file");
    }
    let token = std::fs::read_to_string(path).context("read wallet hints token")?;
    let token = token.trim();
    if token.len() != 64 || !token.bytes().all(|c| c.is_ascii_hexdigit()) {
        bail!("wallet hints token must contain 32 random bytes encoded as hex");
    }
    Ok(token.to_ascii_lowercase())
}

pub(crate) fn router<S>(
    config: Option<&WalletHintsConfig>,
    store: Arc<PoolStore>,
) -> Result<Router<S>>
where
    S: Clone + Send + Sync + 'static,
{
    let Some(config) = config.filter(|config| config.enabled) else {
        return Ok(Router::new());
    };
    let token = load_token(Path::new(&config.token_file))?;
    Ok(routes().with_state(HintsState {
        store,
        expected_auth: Arc::new(format!("Bearer {token}").into_bytes()),
    }))
}

fn routes() -> Router<HintsState> {
    Router::new().route("/api/internal/wallet-hints", get(handle_hints))
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

fn reply(status: StatusCode, body: serde_json::Value) -> Response {
    (status, [(header::CACHE_CONTROL, "no-store")], Json(body)).into_response()
}

#[derive(Deserialize)]
struct HintsQuery {
    after: Option<String>,
    limit: Option<String>,
}

async fn handle_hints(
    State(state): State<HintsState>,
    headers: HeaderMap,
    Query(query): Query<HintsQuery>,
) -> Response {
    let provided = headers
        .get(header::AUTHORIZATION)
        .map(|value| value.as_bytes())
        .unwrap_or_default();
    if !constant_time_eq(provided, &state.expected_auth) {
        return reply(StatusCode::UNAUTHORIZED, json!({"error": "unauthorized"}));
    }
    let after = match query.after.as_deref().map(str::parse::<i64>) {
        None => 0,
        Some(Ok(after)) if after >= 0 => after,
        _ => {
            return reply(
                StatusCode::BAD_REQUEST,
                json!({"error": "after must be a non-negative integer"}),
            )
        }
    };
    let limit = match query.limit.as_deref().map(str::parse::<i64>) {
        None => DEFAULT_LIMIT,
        Some(Ok(limit)) if (1..=MAX_LIMIT).contains(&limit) => limit,
        _ => {
            return reply(
                StatusCode::BAD_REQUEST,
                json!({"error": "limit must be 1..500"}),
            )
        }
    };
    let store = Arc::clone(&state.store);
    let hints =
        match tokio::task::spawn_blocking(move || store.wallet_payout_hints_after(after, limit))
            .await
        {
            Ok(Ok(hints)) => hints,
            Ok(Err(error)) => {
                tracing::warn!(%error, "wallet hints query failed");
                return reply(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    json!({"error": "hints unavailable"}),
                );
            }
            Err(error) => {
                tracing::warn!(%error, "wallet hints task failed");
                return reply(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    json!({"error": "hints unavailable"}),
                );
            }
        };
    let next_after = hints.last().map(|hint| hint.seq).unwrap_or(after);
    let hints = hints
        .into_iter()
        .map(|hint| {
            json!({
                "seq": hint.seq,
                "payout_id": hint.payout_id,
                "tx_id": hint.tx_hash,
                "address": hint.address,
                "amount_atomic": hint.amount.to_string(),
                "created_at_unix_ms": hint.created_at_ms,
            })
        })
        .collect::<Vec<_>>();
    reply(
        StatusCode::OK,
        json!({"hints": hints, "next_after": next_after}),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn disabled_unless_explicitly_enabled() {
        for raw in [r#"{}"#, r#"{"enabled":false,"token_file":"/missing"}"#] {
            let cfg: WalletHintsConfig = serde_json::from_str(raw).unwrap();
            assert!(!cfg.enabled);
        }
        assert!(serde_json::from_str::<WalletHintsConfig>(r#"{"enabled":"true"}"#).is_err());
        assert!(
            serde_json::from_str::<WalletHintsConfig>(r#"{"enabled":true,"extra":1}"#).is_err()
        );
    }

    #[test]
    fn token_file_rules() {
        let dir = std::env::temp_dir().join(format!("wallet-hints-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("token");
        std::fs::write(&path, "ab".repeat(32)).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
            assert!(load_token(&path).is_err(), "group-readable token accepted");
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
        }
        assert_eq!(load_token(&path).unwrap(), "ab".repeat(32));
        std::fs::write(&path, "short").unwrap();
        assert!(load_token(&path).is_err());
        std::fs::remove_dir_all(&dir).ok();
    }

    async fn call(app: Router, uri: &str, auth: Option<&str>) -> (StatusCode, serde_json::Value) {
        use tower::ServiceExt;
        let mut req = axum::http::Request::builder().uri(uri);
        if let Some(auth) = auth {
            req = req.header(header::AUTHORIZATION, auth);
        }
        let resp = app
            .oneshot(req.body(axum::body::Body::empty()).unwrap())
            .await
            .unwrap();
        let status = resp.status();
        assert_eq!(
            resp.headers()
                .get(header::CACHE_CONTROL)
                .map(|v| v.to_str().unwrap()),
            (status != StatusCode::NOT_FOUND).then_some("no-store")
        );
        let body = axum::body::to_bytes(resp.into_body(), 1 << 20)
            .await
            .unwrap();
        (
            status,
            serde_json::from_slice(&body).unwrap_or(serde_json::Value::Null),
        )
    }

    #[test]
    fn route_auth_params_and_disabled_postgres() {
        let Ok(url) = std::env::var("BLOCKNET_POOL_TEST_POSTGRES_URL") else {
            eprintln!("skipping postgres test: set BLOCKNET_POOL_TEST_POSTGRES_URL");
            return;
        };
        // The blocking Postgres client must be created outside a runtime.
        let store = PoolStore::open(&url, 2).expect("open store");
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(route_checks(Arc::clone(&store)));
        // Dropping the store's connections must also happen outside the runtime.
        drop(store);
    }

    async fn route_checks(store: Arc<PoolStore>) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("token");
        std::fs::write(&path, "cd".repeat(32)).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
        }
        let good = format!("Bearer {}", "cd".repeat(32));

        let disabled = router::<()>(None, Arc::clone(&store)).unwrap();
        let (status, _) = call(disabled, "/api/internal/wallet-hints", Some(&good)).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        let off = WalletHintsConfig {
            enabled: false,
            token_file: path.display().to_string(),
        };
        let (status, _) = call(
            router::<()>(Some(&off), Arc::clone(&store)).unwrap(),
            "/api/internal/wallet-hints",
            Some(&good),
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);

        let cfg = WalletHintsConfig {
            enabled: true,
            token_file: path.display().to_string(),
        };
        let app = router::<()>(Some(&cfg), Arc::clone(&store)).unwrap();
        for auth in [
            None,
            Some("Bearer nope"),
            Some(&format!("Bearer {}", "ab".repeat(32))[..]),
        ] {
            let (status, _) = call(app.clone(), "/api/internal/wallet-hints", auth).await;
            assert_eq!(status, StatusCode::UNAUTHORIZED);
        }
        for bad in [
            "?after=-1",
            "?after=x",
            "?limit=0",
            "?limit=501",
            "?limit=z",
        ] {
            let (status, _) = call(
                app.clone(),
                &format!("/api/internal/wallet-hints{bad}"),
                Some(&good),
            )
            .await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{bad}");
        }
        let far = i64::MAX - 1;
        let (status, body) = call(
            app.clone(),
            &format!("/api/internal/wallet-hints?after={far}&limit=5"),
            Some(&good),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["hints"], json!([]));
        assert_eq!(body["next_after"], json!(far));
        let (status, body) = call(
            app,
            "/api/internal/wallet-hints?after=0&limit=500",
            Some(&good),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        let seqs: Vec<i64> = body["hints"]
            .as_array()
            .unwrap()
            .iter()
            .map(|h| h["seq"].as_i64().unwrap())
            .collect();
        assert!(seqs.windows(2).all(|pair| pair[0] < pair[1]));
        for hint in body["hints"].as_array().unwrap() {
            assert!(hint["amount_atomic"].is_string());
        }
        if let Some(last) = seqs.last() {
            assert_eq!(body["next_after"], json!(last));
        }
    }

    #[test]
    fn constant_time_compare() {
        assert!(constant_time_eq(b"Bearer x", b"Bearer x"));
        assert!(!constant_time_eq(b"Bearer x", b"Bearer y"));
        assert!(!constant_time_eq(b"Bearer", b"Bearer x"));
    }
}
