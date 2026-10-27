//! Opt-in gateway for the testnet hosted-wallet prototype. Wallet keys and the
//! payout daemon cookie never enter this module. User authentication is enforced
//! by the wallet service; the gateway credential is not a user credential.
use std::{net::IpAddr, path::Path, sync::Arc, time::Duration};

use anyhow::{bail, Context, Result};
use axum::{
    body::{to_bytes, Body},
    extract::{Request, State},
    http::{header, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use serde::Deserialize;
use serde_json::json;
use tokio::sync::Semaphore;

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct HostedWalletConfig {
    pub backend_url: String,
    pub gateway_token_file: String,
}

impl HostedWalletConfig {
    pub fn validate(&self) -> Result<()> {
        let url = reqwest::Url::parse(&self.backend_url).context("hosted wallet backend URL")?;
        let loopback = url
            .host_str()
            .and_then(|host| host.trim_matches(['[', ']']).parse::<IpAddr>().ok())
            .is_some_and(|ip| ip.is_loopback());
        if url.scheme() != "http"
            || !loopback
            || url.port().is_none()
            || !url.username().is_empty()
            || url.password().is_some()
            || url.path() != "/"
            || url.query().is_some()
            || url.fragment().is_some()
            || self.gateway_token_file.is_empty()
        {
            bail!("hosted wallet requires a loopback HTTP origin with an explicit port and a gateway_token_file");
        }
        Ok(())
    }
}

struct Gateway {
    base: String,
    token: HeaderValue,
    client: reqwest::Client,
    slots: Semaphore,
}

pub(crate) fn router<S>(config: Option<&HostedWalletConfig>) -> Result<Router<S>>
where
    S: Clone + Send + Sync + 'static,
{
    let Some(config) = config else {
        return Ok(Router::new());
    };
    config.validate()?;
    let path = Path::new(&config.gateway_token_file);
    let metadata = std::fs::metadata(path).context("hosted wallet gateway token")?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if metadata.permissions().mode() & 0o077 != 0 {
            bail!("hosted wallet gateway token must have mode 0600");
        }
    }
    if !metadata.is_file() {
        bail!("hosted wallet gateway token must be a regular file");
    }
    let token = std::fs::read_to_string(path).context("read hosted wallet gateway token")?;
    let token = token.trim();
    if token.len() != 64 || !token.bytes().all(|c| c.is_ascii_hexdigit()) {
        bail!("hosted wallet gateway token must contain 32 random bytes encoded as hex");
    }
    let mut value = HeaderValue::from_str(&format!("Bearer {}", token.to_ascii_lowercase()))?;
    value.set_sensitive(true);
    let gateway = Arc::new(Gateway {
        base: config.backend_url.trim_end_matches('/').to_owned(),
        token: value,
        client: reqwest::Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(Duration::from_secs(3))
            .timeout(Duration::from_secs(240))
            .build()?,
        slots: Semaphore::new(4),
    });
    Ok(routes().with_state(gateway))
}

fn routes() -> Router<Arc<Gateway>> {
    Router::new()
        .route("/api/wallet/register", post(forward))
        .route("/api/wallet/login", post(forward))
        .route("/api/wallet/logout", post(forward))
        .route("/api/wallet/recover", post(forward))
        .route("/api/wallet/password", post(forward))
        .route("/api/wallet/session", get(forward))
        .route("/api/wallet/export", post(forward))
        .route("/api/wallet/backup/confirm", post(forward))
        .route("/api/wallet/balance", get(forward))
        .route("/api/wallet/history", get(forward))
        .route("/api/wallet/sends", get(forward))
        .route("/api/wallet/send", post(forward))
        .route("/api/wallet/send/status", get(forward))
        .route("/api/wallet/estimate", post(forward))
}

fn failure(status: StatusCode, message: &str) -> Response {
    (
        status,
        [(header::CACHE_CONTROL, "no-store")],
        Json(json!({"error": message})),
    )
        .into_response()
}

async fn forward(State(gateway): State<Arc<Gateway>>, request: Request) -> Response {
    let Ok(_permit) = gateway.slots.try_acquire() else {
        return failure(
            StatusCode::TOO_MANY_REQUESTS,
            "wallet service busy; retry shortly",
        );
    };
    let (parts, body) = request.into_parts();
    let body = match to_bytes(body, 16 * 1024).await {
        Ok(body) => body,
        Err(_) => return failure(StatusCode::PAYLOAD_TOO_LARGE, "wallet request too large"),
    };
    let mut upstream = gateway
        .client
        .request(
            parts.method,
            format!(
                "{}{}",
                gateway.base,
                parts.uri.path_and_query().map_or("", |p| p.as_str())
            ),
        )
        .header(header::AUTHORIZATION, gateway.token.clone())
        .body(body);
    for name in [
        header::COOKIE,
        header::ORIGIN,
        header::CONTENT_TYPE,
        header::HeaderName::from_static("idempotency-key"),
    ] {
        if let Some(value) = parts.headers.get(&name) {
            upstream = upstream.header(name, value.clone());
        }
    }
    let mut upstream = match upstream.send().await {
        Ok(response) => response,
        Err(_) => {
            return failure(
                StatusCode::BAD_GATEWAY,
                "wallet service unavailable; retry sends with the same idempotency key",
            )
        }
    };
    let status = upstream.status();
    let headers = upstream.headers().clone();
    let mut bytes = Vec::new();
    loop {
        match upstream.chunk().await {
            Ok(Some(chunk)) if bytes.len() + chunk.len() <= 8 * 1024 * 1024 => {
                bytes.extend_from_slice(&chunk)
            }
            Ok(None) => break,
            _ => return failure(StatusCode::BAD_GATEWAY, "wallet response unavailable"),
        }
    }
    let mut response = (status, Body::from(bytes)).into_response();
    for name in [
        header::CONTENT_TYPE,
        header::CONTENT_DISPOSITION,
        header::SET_COOKIE,
    ] {
        for value in headers.get_all(&name) {
            response.headers_mut().append(name.clone(), value.clone());
        }
    }
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response.headers_mut().insert(
        header::X_CONTENT_TYPE_OPTIONS,
        HeaderValue::from_static("nosniff"),
    );
    response
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_remote_and_ambiguous_backend_urls() {
        for url in [
            "https://example.com",
            "http://localhost:8335",
            "http://127.0.0.1:8335/api",
            "http://user@127.0.0.1:8335",
            "http://127.0.0.1:8335/?x=1",
            "http://0.0.0.0:8335",
        ] {
            assert!(
                HostedWalletConfig {
                    backend_url: url.into(),
                    gateway_token_file: "token".into()
                }
                .validate()
                .is_err(),
                "{url}"
            );
        }
        assert!(HostedWalletConfig {
            backend_url: "http://127.0.0.1:8335".into(),
            gateway_token_file: "token".into()
        }
        .validate()
        .is_ok());
    }

    #[tokio::test]
    async fn forwards_only_wallet_routes_and_preserves_session_and_no_store() {
        use axum::http::Request;
        use tower::ServiceExt;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let backend = Router::new().route("/api/wallet/export", post(|headers: axum::http::HeaderMap, body: String| async move {
            assert_eq!(headers[header::AUTHORIZATION], format!("Bearer {}", "ab".repeat(32)));
            assert_eq!(headers[header::COOKIE], "bnt_wallet_session=test-session");
            assert_eq!(headers[header::ORIGIN], "https://pool.example");
            assert!(headers.get("x-user-id").is_none());
            assert_eq!(body, r#"{"password":"example-password"}"#);
            ([(header::SET_COOKIE, "bnt_wallet_session=new; HttpOnly; Secure; SameSite=Strict; Path=/api/wallet")], Json(json!({"mnemonic":"fixture only"})))
        }));
        let task = tokio::spawn(async move { axum::serve(listener, backend).await.unwrap() });
        let dir = tempfile::tempdir().unwrap();
        let token = dir.path().join("gateway-token");
        std::fs::write(&token, "ab".repeat(32)).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&token, std::fs::Permissions::from_mode(0o600)).unwrap();
        }
        let app: Router = router(Some(&HostedWalletConfig {
            backend_url: format!("http://{addr}"),
            gateway_token_file: token.to_string_lossy().into(),
        }))
        .unwrap();
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/wallet/export")
                    .header(header::COOKIE, "bnt_wallet_session=test-session")
                    .header(header::ORIGIN, "https://pool.example")
                    .header(header::AUTHORIZATION, "Bearer attacker")
                    .header("x-user-id", "other-user")
                    .body(Body::from(r#"{"password":"example-password"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()[header::CACHE_CONTROL], "no-store");
        assert!(response.headers()[header::SET_COOKIE]
            .to_str()
            .unwrap()
            .contains("HttpOnly"));
        for path in [
            "/api/wallet/seed",
            "/api/wallet/load",
            "/api/purge",
            "/api/mining/start",
        ] {
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri(path)
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::NOT_FOUND);
        }
        task.abort();
    }
}
