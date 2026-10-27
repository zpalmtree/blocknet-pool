//! Release discovery is independent of miner requests and never blocks pool telemetry.
use std::{sync::Arc, time::Duration};

use anyhow::{bail, Context, Result};
use parking_lot::Mutex;
use serde::Deserialize;

const LATEST_RELEASE_URL: &str = "https://api.github.com/repos/zpalmtree/seine/releases/latest";
const REFRESH_INTERVAL: Duration = Duration::from_secs(15 * 60);
const RETRY_INTERVAL: Duration = Duration::from_secs(60);

#[derive(Clone)]
pub(crate) struct MinerRelease {
    version: Arc<Mutex<String>>,
}

#[derive(Deserialize)]
struct Release {
    tag_name: String,
    draft: bool,
    prerelease: bool,
}

fn stable_version(release: Release) -> Result<String> {
    let version = release
        .tag_name
        .strip_prefix('v')
        .unwrap_or(&release.tag_name);
    let parts: Vec<_> = version.split('.').collect();
    if release.draft
        || release.prerelease
        || parts.len() != 3
        || parts.iter().any(|part| {
            part.is_empty()
                || !part.bytes().all(|byte| byte.is_ascii_digit())
                || part.parse::<u64>().is_err()
        })
    {
        bail!("latest seine release is not a stable version");
    }
    Ok(version.to_string())
}

impl MinerRelease {
    pub(crate) fn new(fallback: &str) -> Self {
        Self {
            version: Arc::new(Mutex::new(fallback.trim().to_string())),
        }
    }

    pub(crate) fn version(&self) -> String {
        self.version.lock().clone()
    }

    async fn refresh(&self, url: &str) -> Result<()> {
        let client = reqwest::Client::builder()
            .user_agent("bntpool-miner-release-check")
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(10))
            .build()?;
        let release = client
            .get(url)
            .header("Accept", "application/vnd.github+json")
            .header("X-GitHub-Api-Version", "2022-11-28")
            .send()
            .await?
            .error_for_status()?
            .json::<Release>()
            .await
            .context("decode latest seine release")?;
        let version = stable_version(release)?;
        let mut current = self.version.lock();
        if *current != version {
            tracing::info!(%version, "latest seine release updated");
            *current = version;
        }
        Ok(())
    }

    pub(crate) async fn refresh_forever(&self) {
        loop {
            let delay = match self.refresh(LATEST_RELEASE_URL).await {
                Ok(()) => REFRESH_INTERVAL,
                Err(error) => {
                    tracing::warn!(%error, "seine release check failed; retaining last known version");
                    RETRY_INTERVAL
                }
            };
            tokio::time::sleep(delay).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{http::StatusCode, routing::get, Router};

    #[test]
    fn accepts_only_stable_release_tags() {
        for tag in ["v0.2.38", "0.2.38", "v1.10.0"] {
            assert_eq!(
                stable_version(Release {
                    tag_name: tag.into(),
                    draft: false,
                    prerelease: false
                })
                .unwrap(),
                tag.trim_start_matches('v')
            );
        }
        for (tag, draft, prerelease) in [
            ("v0.2.39", true, false),
            ("v0.2.39", false, true),
            ("v0.2.39-rc.1", false, false),
            ("garbage", false, false),
            ("0.2", false, false),
            ("0.2.38.1", false, false),
            ("0.2.+38", false, false),
            ("0.2.", false, false),
        ] {
            assert!(stable_version(Release {
                tag_name: tag.into(),
                draft,
                prerelease
            })
            .is_err());
        }
    }

    #[tokio::test]
    async fn refresh_updates_shared_state_and_preserves_it_on_failure() {
        let response = Arc::new(Mutex::new((StatusCode::OK, String::new())));
        let handler_response = response.clone();
        let app = Router::new().route(
            "/latest",
            get(move || {
                let response = handler_response.lock().clone();
                async move { response }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/latest", listener.local_addr().unwrap());
        let server = tokio::spawn(async { axum::serve(listener, app).await.unwrap() });
        let release = MinerRelease::new("");
        let reader = release.clone();
        assert_eq!(reader.version(), "");
        // A failed first check must not advertise an invented or obsolete release.
        assert!(release.refresh(&url).await.is_err());
        assert_eq!(reader.version(), "");
        for version in ["0.2.38", "0.2.39"] {
            *response.lock() = (
                StatusCode::OK,
                format!(r#"{{"tag_name":"v{version}","draft":false,"prerelease":false}}"#),
            );
            release.refresh(&url).await.unwrap();
            assert_eq!(reader.version(), version);
        }
        for (status, body) in [
            (StatusCode::TOO_MANY_REQUESTS, "rate limited"),
            (StatusCode::INTERNAL_SERVER_ERROR, "unavailable"),
            (StatusCode::OK, "malformed JSON"),
            (
                StatusCode::OK,
                r#"{"tag_name":"v0.2.40","draft":false,"prerelease":true}"#,
            ),
            (
                StatusCode::OK,
                r#"{"tag_name":"invalid","draft":false,"prerelease":false}"#,
            ),
        ] {
            *response.lock() = (status, body.into());
            assert!(release.refresh(&url).await.is_err());
            assert_eq!(reader.version(), "0.2.39");
        }
        server.abort();
        assert_eq!(MinerRelease::new(" 0.2.38 ").version(), "0.2.38");
    }
}
