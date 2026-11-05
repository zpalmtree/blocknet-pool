//! My Stats settings endpoints (rules in `pool_runtime::miner_settings`).
//!
//! An unlocked browser holds a random session token in an HttpOnly,
//! SameSite=Strict cookie. Sessions are kept in the database by token hash,
//! so they outlast API restarts; they end after [`SESSION_IDLE`] without use,
//! on sign-out, or once the account's password or owner changes. Every write is JSON, so a
//! cross-site form cannot send one. Password and take-back attempts are
//! limited per account and per client IP group, and those limits fail
//! closed when their tables are full.

use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use argon2::password_hash::{PasswordHash, PasswordHasher, PasswordVerifier, SaltString};
use argon2::Argon2;
use axum::extract::{ConnectInfo, Path, State};
use axum::http::{header, HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use parking_lot::Mutex;
use pool_runtime::config::Config as RuntimeConfig;
use pool_runtime::miner_settings::{
    discord_webhook_display, discord_webhook_valid, effective_min_payout, max_min_payout,
    plausible_email, start_difficulty_in_era, take_back_decision, validate_min_payout,
    AlertChoices as StoredAlertChoices, MinerAlert, MinerSettings, MinerSettingsRecord, TakeBack,
    ALERT_DROP_MINUTES, ALERT_OFFLINE_MINUTES, ATOMIC_PER_BNT, TAKE_BACK_WINDOW,
};
use pool_runtime::store::PoolStore;
use serde::Deserialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tokio::sync::Semaphore;

use crate::protect::{client_key, ClientIpPolicy};

pub(crate) const SESSION_COOKIE: &str = "pool_settings";
const SESSION_IDLE: Duration = Duration::from_secs(30 * 24 * 60 * 60);
/// How long the browser keeps the cookie; the server enforces [`SESSION_IDLE`].
const SESSION_COOKIE_MAX_AGE: Duration = Duration::from_secs(400 * 24 * 60 * 60);
/// How often a session's last use is written back while it is in use.
const SESSION_TOUCH_EVERY: Duration = Duration::from_secs(60 * 60);
/// How often expired sessions are deleted.
const SESSION_PRUNE_EVERY: Duration = Duration::from_secs(60 * 60);
const MAX_ACCOUNTS_PER_SESSION: usize = 16;

const PASSWORD_MIN_CHARS: usize = 8;
const PASSWORD_MAX_BYTES: usize = 256;

const ATTEMPT_WINDOW: Duration = Duration::from_secs(60 * 60);
const ATTEMPT_MAX_KEYS: usize = 100_000;
/// Wrong passwords per account per hour.
const UNLOCK_FAILURES_PER_ACCOUNT: u32 = 5;
/// Unlock attempts per client per hour, right or wrong, across accounts.
const UNLOCKS_PER_CLIENT: u32 = 20;
/// Take-backs per account per hour, passed or not.
const TAKE_BACKS_PER_ACCOUNT: u32 = 5;
/// Take-back checks per client per hour.
const TAKE_BACKS_PER_CLIENT: u32 = 20;
/// New passwords (claims and changes) per client per hour.
const PASSWORD_WRITES_PER_CLIENT: u32 = 10;
/// Concurrent Argon2 hashes; each costs ~19 MiB and tens of milliseconds.
const MAX_CONCURRENT_HASHES: usize = 4;

const HIDE_CACHE_TTL: Duration = Duration::from_secs(15);
const HIDE_CACHE_MAX_ENTRIES: usize = 65_536;

/// Per-key hourly counters that refuse new keys when full, so filling the
/// table cannot lift a limit.
struct AttemptLimiter {
    entries: HashMap<String, (Instant, u32)>,
}

impl AttemptLimiter {
    fn new() -> Self {
        Self {
            entries: HashMap::new(),
        }
    }

    fn current(&mut self, key: &str, now: Instant) -> u32 {
        match self.entries.get(key) {
            Some((started, count)) if now.saturating_duration_since(*started) < ATTEMPT_WINDOW => {
                *count
            }
            Some(_) => {
                self.entries.remove(key);
                0
            }
            None => 0,
        }
    }

    fn retry_after(&self, key: &str, now: Instant) -> u64 {
        self.entries
            .get(key)
            .map(|(started, _)| {
                ATTEMPT_WINDOW
                    .saturating_sub(now.saturating_duration_since(*started))
                    .as_secs()
                    .max(1)
            })
            .unwrap_or(1)
    }

    /// Counts one attempt. False when the table is full and `key` is new.
    fn record(&mut self, key: &str, now: Instant) -> bool {
        let count = self.current(key, now);
        if count == 0 {
            if self.entries.len() >= ATTEMPT_MAX_KEYS {
                self.entries.retain(|_, (started, _)| {
                    now.saturating_duration_since(*started) < ATTEMPT_WINDOW
                });
                if self.entries.len() >= ATTEMPT_MAX_KEYS {
                    return false;
                }
            }
            self.entries.insert(key.to_string(), (now, 1));
        } else if let Some(entry) = self.entries.get_mut(key) {
            entry.1 = entry.1.saturating_add(1);
        }
        true
    }

    /// Counts an attempt before it runs, so parallel requests cannot all
    /// pass one check. False when `key` is at `limit` or the table is full.
    fn try_take(&mut self, key: &str, limit: u32, now: Instant) -> bool {
        self.current(key, now) < limit && self.record(key, now)
    }

    /// Gives back an attempt taken with `try_take` (one that succeeded).
    fn release(&mut self, key: &str) {
        if let Some(entry) = self.entries.get_mut(key) {
            entry.1 = entry.1.saturating_sub(1);
        }
    }
}

struct Limits {
    unlock_by_account: AttemptLimiter,
    unlock_by_client: AttemptLimiter,
    take_back_by_account: AttemptLimiter,
    take_back_by_client: AttemptLimiter,
    password_writes_by_client: AttemptLimiter,
    alert_emails_by_account: AttemptLimiter,
    alert_emails_by_client: AttemptLimiter,
    alert_emails_total: AttemptLimiter,
    chat_links_by_account: AttemptLimiter,
    chat_links_by_client: AttemptLimiter,
}

/// Discord test posts and Telegram links per account per hour.
const CHAT_LINKS_PER_ACCOUNT: u32 = 5;
/// The same per client per hour, across accounts.
const CHAT_LINKS_PER_CLIENT: u32 = 20;

/// Confirmation emails per account per hour.
const ALERT_EMAILS_PER_ACCOUNT: u32 = 3;
/// Confirmation emails per client per hour, across accounts.
const ALERT_EMAILS_PER_CLIENT: u32 = 10;
/// Confirmation emails to one recipient per day, across accounts.
const ALERT_EMAILS_PER_RECIPIENT: u64 = 3;
/// Confirmation emails the pool sends per hour in all.
const ALERT_EMAILS_PER_HOUR: u32 = 200;

pub(crate) struct MinerSettingsService {
    store: Arc<PoolStore>,
    alerts: Option<Arc<crate::alerts::AlertService>>,
    runtime: RuntimeConfig,
    client_ip: Arc<ClientIpPolicy>,
    /// When expired sessions were last deleted.
    sessions_pruned: Mutex<Option<Instant>>,
    limits: Mutex<Limits>,
    hashing: Semaphore,
    hide_workers: Mutex<HashMap<String, (Instant, PublicView)>>,
}

/// What visitors without an unlocked session see of an account's stats.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PublicView {
    Full,
    /// Rigs are shown as "Rig 1", "Rig 2".
    HideNames,
    /// Nothing but that the stats are private.
    Private,
}

/// Changes whenever the password or the owner does, ending older sessions.
fn owner_stamp(record: &MinerSettingsRecord) -> String {
    let mut hasher = Sha256::new();
    hasher.update(record.password_hash.as_deref().unwrap_or("").as_bytes());
    hasher.update([0x1f]);
    hasher.update(record.owner_ip_group.as_bytes());
    hex::encode(&hasher.finalize()[..16])
}

fn token_hash(token: &str) -> String {
    hex::encode(Sha256::digest(token.as_bytes()))
}

fn session_token(headers: &HeaderMap) -> Option<String> {
    headers
        .get_all(header::COOKIE)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(';'))
        .filter_map(|pair| pair.trim().split_once('='))
        .find(|(name, _)| *name == SESSION_COOKIE)
        .map(|(_, value)| value.trim().to_string())
        .filter(|value| value.len() == 64 && value.bytes().all(|b| b.is_ascii_hexdigit()))
}

fn session_cookie(token: &str) -> HeaderValue {
    HeaderValue::from_str(&format!(
        "{SESSION_COOKIE}={token}; Path=/api; Max-Age={}; HttpOnly; Secure; SameSite=Strict",
        SESSION_COOKIE_MAX_AGE.as_secs()
    ))
    .expect("session cookie is ASCII")
}

/// Deletes the session cookie once no account is unlocked under it.
fn clear_session_cookie() -> HeaderValue {
    HeaderValue::from_str(&format!(
        "{SESSION_COOKIE}=; Path=/api; Max-Age=0; HttpOnly; Secure; SameSite=Strict"
    ))
    .expect("session cookie is ASCII")
}

fn to_unix(at: SystemTime) -> u64 {
    at.duration_since(UNIX_EPOCH).unwrap_or_default().as_secs()
}

fn coins(atomic: u64) -> f64 {
    atomic as f64 / ATOMIC_PER_BNT as f64
}

/// Accepts an IPv4 or IPv6 address, or an IPv6 /64 written as the pool
/// shows it (`2001:db8:1:2::/64`), and returns its IP group.
pub(crate) fn parse_rig_ip(raw: &str) -> Option<String> {
    let raw = raw.trim();
    let raw = raw.strip_suffix("/64").unwrap_or(raw);
    let ip = raw.parse::<IpAddr>().ok()?;
    let ip = match ip {
        IpAddr::V6(v6) => v6
            .to_ipv4_mapped()
            .map(IpAddr::V4)
            .unwrap_or(IpAddr::V6(v6)),
        v4 => v4,
    };
    if ip.is_unspecified() || ip.is_loopback() || ip.is_multicast() {
        return None;
    }
    Some(client_key(ip))
}

fn password_ok(password: &str) -> Result<(), ApiError> {
    if password.chars().count() < PASSWORD_MIN_CHARS {
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "invalid_password",
            "Use at least 8 characters.",
        ));
    }
    if password.len() > PASSWORD_MAX_BYTES {
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "invalid_password",
            "Use at most 256 characters.",
        ));
    }
    Ok(())
}

struct ApiError {
    status: StatusCode,
    body: Value,
}

impl ApiError {
    fn new(status: StatusCode, code: &str, message: &str) -> Self {
        Self {
            status,
            body: json!({ "error": code, "message": message }),
        }
    }

    fn with(mut self, key: &str, value: Value) -> Self {
        self.body[key] = value;
        self
    }

    fn internal(err: anyhow::Error) -> Self {
        tracing::warn!(error = %err, "miner settings request failed");
        Self::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal",
            "Settings are unavailable right now. Try again shortly.",
        )
    }

    fn rate_limited(retry_after: u64) -> Self {
        Self::new(
            StatusCode::TOO_MANY_REQUESTS,
            "rate_limited",
            "Too many tries. Wait and try again.",
        )
        .with("retry_after", json!(retry_after))
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let retry_after = self.body.get("retry_after").and_then(Value::as_u64);
        let mut response = (self.status, Json(self.body)).into_response();
        if let Some(secs) = retry_after {
            if let Ok(value) = HeaderValue::from_str(&secs.to_string()) {
                response.headers_mut().insert(header::RETRY_AFTER, value);
            }
        }
        response
    }
}

type ApiResult = Result<Response, ApiError>;

fn changed_elsewhere() -> ApiError {
    ApiError::new(
        StatusCode::CONFLICT,
        "locked",
        "These settings changed in another browser. Unlock them again.",
    )
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PasswordRequest {
    password: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TakeBackRequest {
    ip: String,
}

/// Tells a missing field (keep the saved value) from an explicit `null`.
fn present<'de, D, T>(deserializer: D) -> Result<Option<Option<T>>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer).map(Some)
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SettingsRequest {
    /// BNT; `null` keeps the pool's minimum.
    min_payout: Option<f64>,
    default_solo: bool,
    hide_workers: bool,
    /// Missing keeps the saved choice (older pages do not send it).
    #[serde(default)]
    private_stats: Option<bool>,
    /// Missing keeps the saved choice; `null` lets the pool choose.
    #[serde(default, deserialize_with = "present")]
    start_difficulty: Option<Option<u64>>,
    /// Alert choices; applied only once an alert channel is set.
    #[serde(default)]
    alerts: Option<AlertChoices>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct AlertChoices {
    /// 0 turns rig-stopped alerts off.
    offline_minutes: u32,
    notify_payouts: bool,
    notify_solo_blocks: bool,
    /// The rest are kept as saved when missing.
    #[serde(default)]
    hashrate_drop_minutes: Option<u32>,
    #[serde(default)]
    notify_updates: Option<bool>,
    #[serde(default)]
    weekly_summary: Option<bool>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DiscordRequest {
    webhook: String,
    #[serde(default)]
    lang: String,
}

#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct TelegramRequest {
    #[serde(default)]
    lang: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct AlertEmailRequest {
    email: String,
    #[serde(default)]
    lang: String,
}

/// Request context: account, client IP group and session token.
struct Caller {
    account: String,
    client: Option<String>,
    token: Option<String>,
}

impl MinerSettingsService {
    pub(crate) fn new(
        store: Arc<PoolStore>,
        runtime: RuntimeConfig,
        client_ip: Arc<ClientIpPolicy>,
        alerts: Option<Arc<crate::alerts::AlertService>>,
    ) -> Self {
        Self {
            store,
            alerts,
            runtime,
            client_ip,
            sessions_pruned: Mutex::new(None),
            limits: Mutex::new(Limits {
                unlock_by_account: AttemptLimiter::new(),
                unlock_by_client: AttemptLimiter::new(),
                take_back_by_account: AttemptLimiter::new(),
                take_back_by_client: AttemptLimiter::new(),
                password_writes_by_client: AttemptLimiter::new(),
                alert_emails_by_account: AttemptLimiter::new(),
                alert_emails_by_client: AttemptLimiter::new(),
                alert_emails_total: AttemptLimiter::new(),
                chat_links_by_account: AttemptLimiter::new(),
                chat_links_by_client: AttemptLimiter::new(),
            }),
            hashing: Semaphore::new(MAX_CONCURRENT_HASHES),
            hide_workers: Mutex::new(HashMap::new()),
        }
    }

    /// An account's minimum payout, and the lowest its miner may choose:
    /// exchange (memo) accounts keep the higher memo minimum so the exchange
    /// is not sent many small deposits.
    fn default_minimum(&self, account: &str) -> u64 {
        (self.runtime.payout_minimum(account) * ATOMIC_PER_BNT as f64).round() as u64
    }

    /// Highest minimum payout the account may choose: what one payout may send.
    fn max_min_payout(&self, account: &str) -> u64 {
        let atomic = |bnt: f64| (bnt.max(0.0) * ATOMIC_PER_BNT as f64).round() as u64;
        max_min_payout(
            self.default_minimum(account),
            atomic(self.runtime.payout_max_per_recipient),
            atomic(self.runtime.payout_max_total_per_tick),
        )
    }

    fn effective_minimum(&self, account: &str, chosen: Option<u64>) -> u64 {
        effective_min_payout(self.default_minimum(account), chosen)
    }

    /// The minimum payout that applies to an account, in BNT.
    pub(crate) fn effective_min_payout_coins(&self, account: &str, chosen: Option<u64>) -> f64 {
        coins(self.effective_minimum(account, chosen))
    }

    async fn blocking<T, F>(&self, operation: F) -> Result<T, ApiError>
    where
        F: FnOnce(&PoolStore) -> anyhow::Result<T> + Send + 'static,
        T: Send + 'static,
    {
        let store = Arc::clone(&self.store);
        tokio::task::spawn_blocking(move || operation(&store))
            .await
            .map_err(|err| ApiError::internal(anyhow::anyhow!("join error: {err}")))?
            .map_err(ApiError::internal)
    }

    async fn record(&self, account: &str) -> Result<Option<MinerSettingsRecord>, ApiError> {
        let account = account.to_string();
        self.blocking(move |store| store.miner_settings(&account))
            .await
    }

    async fn hash_password(&self, password: String) -> Result<String, ApiError> {
        let _permit = self
            .hashing
            .acquire()
            .await
            .map_err(|_| ApiError::internal(anyhow::anyhow!("hashing closed")))?;
        tokio::task::spawn_blocking(move || {
            let salt = SaltString::encode_b64(&rand::random::<[u8; 16]>())
                .map_err(|err| anyhow::anyhow!("salt: {err}"))?;
            Argon2::default()
                .hash_password(password.as_bytes(), &salt)
                .map(|hash| hash.to_string())
                .map_err(|err| anyhow::anyhow!("hash password: {err}"))
        })
        .await
        .map_err(|err| ApiError::internal(anyhow::anyhow!("join error: {err}")))?
        .map_err(ApiError::internal)
    }

    async fn verify_password(&self, password: String, stored: String) -> Result<bool, ApiError> {
        let _permit = self
            .hashing
            .acquire()
            .await
            .map_err(|_| ApiError::internal(anyhow::anyhow!("hashing closed")))?;
        tokio::task::spawn_blocking(move || {
            let Ok(parsed) = PasswordHash::new(&stored) else {
                return false;
            };
            Argon2::default()
                .verify_password(password.as_bytes(), &parsed)
                .is_ok()
        })
        .await
        .map_err(|err| ApiError::internal(anyhow::anyhow!("join error: {err}")))
    }

    /// Whether this caller's session has the account unlocked for `record`.
    async fn unlocked(
        &self,
        caller: &Caller,
        record: &MinerSettingsRecord,
    ) -> Result<bool, ApiError> {
        let Some(token) = &caller.token else {
            return Ok(false);
        };
        let hash = token_hash(token);
        let account = caller.account.clone();
        let stamp = owner_stamp(record);
        let now = SystemTime::now();
        self.blocking(move |store| {
            let Some((unlocked_stamp, last_used)) =
                store.miner_settings_session(&hash, &account)?
            else {
                return Ok(false);
            };
            let idle = to_unix(now).saturating_sub(u64::try_from(last_used).unwrap_or(0));
            if unlocked_stamp != stamp || idle >= SESSION_IDLE.as_secs() {
                store.remove_miner_settings_session(&hash, &account)?;
                return Ok(false);
            }
            if idle >= SESSION_TOUCH_EVERY.as_secs() {
                store.touch_miner_settings_session(&hash, &account, now)?;
            }
            Ok(true)
        })
        .await
    }

    /// Unlocks the account in the caller's session, creating one if needed.
    /// Returns a new token to set as the cookie, if one was created.
    async fn unlock_session(
        &self,
        caller: &Caller,
        record: &MinerSettingsRecord,
    ) -> Result<Option<String>, ApiError> {
        let stamp = owner_stamp(record);
        let account = caller.account.clone();
        let existing = caller.token.clone();
        let prune = {
            let mut pruned = self.sessions_pruned.lock();
            let due = pruned.is_none_or(|at| at.elapsed() >= SESSION_PRUNE_EVERY);
            if due {
                *pruned = Some(Instant::now());
            }
            due
        };
        self.blocking(move |store| {
            let now = SystemTime::now();
            if prune {
                store.prune_miner_settings_sessions(
                    now.checked_sub(SESSION_IDLE).unwrap_or(UNIX_EPOCH),
                )?;
            }
            // Reuse the browser's session when it still has one.
            let (hash, new_token) = match existing {
                Some(token) if store.miner_settings_session_exists(&token_hash(&token))? => {
                    (token_hash(&token), None)
                }
                _ => {
                    let token = hex::encode(rand::random::<[u8; 32]>());
                    (token_hash(&token), Some(token))
                }
            };
            store.put_miner_settings_session(
                &hash,
                &account,
                &stamp,
                now,
                MAX_ACCOUNTS_PER_SESSION,
            )?;
            Ok(new_token)
        })
        .await
    }

    /// Locks the account in this browser. Other accounts it unlocked stay so.
    async fn sign_out(&self, caller: Caller) -> ApiResult {
        self.validate_account(&caller.account).await?;
        let mut others = false;
        if let Some(token) = caller.token.clone() {
            let account = caller.account.clone();
            others = self
                .blocking(move |store| {
                    store.remove_miner_settings_session(&token_hash(&token), &account)
                })
                .await?;
        }
        let record = self.record(&caller.account).await?;
        let mut response = self.respond(&caller, record.as_ref(), false, None).await?;
        if !others {
            response
                .headers_mut()
                .append(header::SET_COOKIE, clear_session_cookie());
        }
        Ok(response)
    }

    fn forget_hide_cache(&self, account: &str) {
        self.hide_workers.lock().remove(account);
    }

    /// What this request may see of `account`'s public stats: everything,
    /// numbered rigs, or nothing. The unlocked owner sees everything.
    pub(crate) async fn public_view_for(&self, account: &str, headers: &HeaderMap) -> PublicView {
        let now = Instant::now();
        let cached = self
            .hide_workers
            .lock()
            .get(account)
            .filter(|(at, _)| now.saturating_duration_since(*at) < HIDE_CACHE_TTL)
            .map(|(_, view)| *view);
        let view = match cached {
            Some(view) => view,
            None => {
                let view = match self.record(account).await {
                    Ok(Some(record)) if record.settings.private_stats => PublicView::Private,
                    Ok(Some(record)) if record.settings.hide_workers => PublicView::HideNames,
                    Ok(_) => PublicView::Full,
                    // Fail closed: hiding is the safer mistake.
                    Err(_) => PublicView::Private,
                };
                let mut cache = self.hide_workers.lock();
                if cache.len() >= HIDE_CACHE_MAX_ENTRIES {
                    cache.clear();
                }
                cache.insert(account.to_string(), (now, view));
                view
            }
        };
        if view == PublicView::Full {
            return view;
        }
        // The owner sees everything while unlocked; visitors without a
        // session cost no further read.
        let Some(token) = session_token(headers) else {
            return view;
        };
        let caller = Caller {
            account: account.to_string(),
            client: None,
            token: Some(token),
        };
        match self.record(account).await {
            Ok(Some(record)) if self.unlocked(&caller, &record).await.unwrap_or(false) => {
                PublicView::Full
            }
            _ => view,
        }
    }

    fn caller(&self, account: String, headers: &HeaderMap, peer: Option<IpAddr>) -> Caller {
        Caller {
            account,
            client: self.client_ip.resolve(headers, peer).map(client_key),
            token: session_token(headers),
        }
    }

    fn state_body(
        &self,
        caller: &Caller,
        record: Option<&MinerSettingsRecord>,
        unlocked: bool,
        alert: Option<&MinerAlert>,
    ) -> Value {
        let mut body = json!({
            "account": caller.account,
            "claimed": record.is_some(),
            "has_password": record.is_some_and(|record| record.password_hash.is_some()),
            "unlocked": unlocked,
            "changed_at": record.map(|record| to_unix(record.changed_at)),
            "pool_min_payout": coins(self.default_minimum(&caller.account)),
            "default_min_payout": coins(self.default_minimum(&caller.account)),
            "max_min_payout": coins(self.max_min_payout(&caller.account)),
            "your_ip": caller.client,
            "alerts_available": self.alerts.is_some(),
            "telegram_available": self
                .alerts
                .as_ref()
                .is_some_and(|alerts| alerts.telegram_bot.is_some()),
        });
        if let (true, Some(record)) = (unlocked, record) {
            body["settings"] = json!({
                "min_payout": coins(self.effective_minimum(&caller.account, record.settings.min_payout)),
                "default_solo": record.settings.default_solo,
                "hide_workers": record.settings.hide_workers,
                "private_stats": record.settings.private_stats,
                "start_difficulty": record.settings.start_difficulty,
            });
            body["alerts"] = alert.map_or(Value::Null, |alert| {
                let telegram = match (&alert.telegram_chat_id, alert.telegram_pending) {
                    (Some(_), _) => {
                        json!({"name": alert.telegram_name.clone().unwrap_or_default()})
                    }
                    (None, true) => json!({"pending": ""}),
                    (None, false) => Value::Null,
                };
                json!({
                    "email": alert.email,
                    "confirmed": alert.confirmed_at.is_some() && !alert.email.is_empty(),
                    "offline_minutes": alert.offline_minutes,
                    "notify_payouts": alert.notify_payouts,
                    "notify_solo_blocks": alert.notify_solo_blocks,
                    "hashrate_drop_minutes": alert.hashrate_drop_minutes,
                    "notify_updates": alert.notify_updates,
                    "weekly_summary": alert.weekly_summary,
                    "discord": alert.discord_webhook.as_deref().map(discord_webhook_display),
                    "telegram": telegram,
                })
            });
        }
        body
    }

    /// Starting difficulties a miner may choose in the current era
    /// (`initial..=max`), and about where the account's rigs are now.
    async fn difficulty_range(&self, account: &str) -> Result<(u64, u64, Option<u64>), ApiError> {
        let account = account.to_string();
        let (min, max, initial) = (
            self.runtime.min_share_difficulty.max(1),
            self.runtime.max_share_difficulty,
            self.runtime.initial_share_difficulty,
        );
        self.blocking(move |store| {
            let height = store.highest_block_height()?;
            let (floor, initial, max) = if pool_common::faraday::is_faraday(height + 1) {
                (
                    pool_common::faraday::FARADAY_MIN_SHARE_DIFFICULTY,
                    pool_common::faraday::FARADAY_INITIAL_SHARE_DIFFICULTY,
                    pool_common::faraday::FARADAY_MAX_SHARE_DIFFICULTY,
                )
            } else {
                let max = max.max(min);
                (min, initial.clamp(min, max), max)
            };
            let fresh = SystemTime::now()
                .checked_sub(Duration::from_secs(86_400))
                .unwrap_or(UNIX_EPOCH);
            let mut hints: Vec<u64> = store
                .get_vardiff_hints_for_address(&account, 32)?
                .into_iter()
                .filter(|(difficulty, at)| *at >= fresh && *difficulty >= floor)
                .map(|(difficulty, _)| difficulty)
                .collect();
            hints.sort_unstable();
            let current = hints.get(hints.len() / 2).copied();
            Ok((initial, max, current))
        })
        .await
    }

    /// The account's alert email, read only for an unlocked caller.
    async fn alert_for(
        &self,
        account: &str,
        unlocked: bool,
    ) -> Result<Option<MinerAlert>, ApiError> {
        if !unlocked || self.alerts.is_none() {
            return Ok(None);
        }
        let account = account.to_string();
        self.blocking(move |store| store.miner_alert(&account))
            .await
    }

    async fn respond(
        &self,
        caller: &Caller,
        record: Option<&MinerSettingsRecord>,
        unlocked: bool,
        new_token: Option<String>,
    ) -> ApiResult {
        let alert = self.alert_for(&caller.account, unlocked).await?;
        let mut body = self.state_body(caller, record, unlocked, alert.as_ref());
        if let (true, Some(record)) = (unlocked, record) {
            let (min, max, current) = self.difficulty_range(&caller.account).await?;
            body["difficulty"] = json!({"min": min, "max": max, "current": current});
            // A choice from an earlier difficulty era no longer applies.
            if record
                .settings
                .start_difficulty
                .is_some_and(|chosen| start_difficulty_in_era(chosen, min, max).is_none())
            {
                body["settings"]["start_difficulty"] = Value::Null;
            }
        }
        Ok(respond_body(body, new_token))
    }
}

fn respond_body(body: Value, new_token: Option<String>) -> Response {
    let mut response = Json(body).into_response();
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    if let Some(token) = new_token {
        response
            .headers_mut()
            .append(header::SET_COOKIE, session_cookie(&token));
    }
    response
}

impl MinerSettingsService {
    async fn validate_account(&self, account: &str) -> Result<(), ApiError> {
        let invalid = || {
            ApiError::new(
                StatusCode::BAD_REQUEST,
                "invalid_account",
                "That is not a miner address.",
            )
        };
        if account.starts_with(pool_common::destination::MEMO_ACCOUNT_PREFIX) {
            let account = account.to_string();
            return self
                .blocking(move |store| Ok(store.payout_destination(&account).is_ok()))
                .await?
                .then_some(())
                .ok_or_else(invalid);
        }
        pool_common::protocol::validate_miner_address(account).map_err(|_| invalid())
    }

    async fn get_state(&self, caller: Caller) -> ApiResult {
        self.validate_account(&caller.account).await?;
        let record = self.record(&caller.account).await?;
        let unlocked = match &record {
            Some(record) => self.unlocked(&caller, record).await?,
            None => false,
        };
        self.respond(&caller, record.as_ref(), unlocked, None).await
    }

    fn require_client(&self, caller: &Caller) -> Result<String, ApiError> {
        caller.client.clone().ok_or_else(|| {
            ApiError::new(
                StatusCode::BAD_REQUEST,
                "unknown_client",
                "Your network address could not be read.",
            )
        })
    }

    fn take_password_write(&self, client: &str) -> Result<(), ApiError> {
        let now = Instant::now();
        let mut limits = self.limits.lock();
        let writes = &mut limits.password_writes_by_client;
        if writes.current(client, now) >= PASSWORD_WRITES_PER_CLIENT || !writes.record(client, now)
        {
            return Err(ApiError::rate_limited(writes.retry_after(client, now)));
        }
        Ok(())
    }

    async fn claim(&self, caller: Caller, request: PasswordRequest) -> ApiResult {
        self.validate_account(&caller.account).await?;
        password_ok(&request.password)?;
        let client = self.require_client(&caller)?;
        self.take_password_write(&client)?;
        let hash = self.hash_password(request.password).await?;
        let account = caller.account.clone();
        let group = client.clone();
        let claimed = self
            .blocking(move |store| {
                store.claim_miner_settings(&account, &hash, &group, SystemTime::now())
            })
            .await?;
        if !claimed {
            return Err(ApiError::new(
                StatusCode::CONFLICT,
                "already_claimed",
                "This address already has a password.",
            ));
        }
        tracing::info!(account = %caller.account, ip_group = %client, "miner settings password set");
        let record = self
            .record(&caller.account)
            .await?
            .ok_or_else(|| ApiError::internal(anyhow::anyhow!("claimed settings row missing")))?;
        let token = self.unlock_session(&caller, &record).await?;
        self.respond(&caller, Some(&record), true, token).await
    }

    async fn unlock(&self, caller: Caller, request: PasswordRequest) -> ApiResult {
        let client = self.require_client(&caller)?;
        if request.password.len() > PASSWORD_MAX_BYTES {
            return Err(ApiError::new(
                StatusCode::BAD_REQUEST,
                "invalid_password",
                "Use at most 256 characters.",
            ));
        }
        {
            let now = Instant::now();
            let mut limits = self.limits.lock();
            if !limits
                .unlock_by_client
                .try_take(&client, UNLOCKS_PER_CLIENT, now)
            {
                return Err(ApiError::rate_limited(
                    limits.unlock_by_client.retry_after(&client, now),
                ));
            }
            if !limits
                .unlock_by_account
                .try_take(&caller.account, UNLOCK_FAILURES_PER_ACCOUNT, now)
            {
                return Err(ApiError::rate_limited(
                    limits.unlock_by_account.retry_after(&caller.account, now),
                ));
            }
        }
        let record = self.record(&caller.account).await?;
        let verified = match record
            .as_ref()
            .and_then(|record| record.password_hash.clone())
        {
            Some(stored) => self.verify_password(request.password, stored).await?,
            None => false,
        };
        let Some(record) = record.filter(|_| verified) else {
            let now = Instant::now();
            let left = UNLOCK_FAILURES_PER_ACCOUNT.saturating_sub(
                self.limits
                    .lock()
                    .unlock_by_account
                    .current(&caller.account, now),
            );
            tracing::info!(account = %caller.account, ip_group = %client, tries_left = left, "miner settings wrong password");
            return Err(ApiError::new(
                StatusCode::UNAUTHORIZED,
                "wrong_password",
                "Wrong password.",
            )
            .with("tries_left", json!(left)));
        };
        // Only wrong passwords count against the account.
        self.limits
            .lock()
            .unlock_by_account
            .release(&caller.account);
        let token = self.unlock_session(&caller, &record).await?;
        self.respond(&caller, Some(&record), true, token).await
    }

    async fn take_back(&self, caller: Caller, request: TakeBackRequest) -> ApiResult {
        let client = self.require_client(&caller)?;
        let Some(rig_group) = parse_rig_ip(&request.ip) else {
            return Err(ApiError::new(
                StatusCode::BAD_REQUEST,
                "invalid_ip",
                "Enter an IP address, like 203.0.113.24.",
            ));
        };
        {
            // Counted up front, passed or not, so parallel requests cannot
            // all pass one check.
            let now = Instant::now();
            let mut limits = self.limits.lock();
            if !limits
                .take_back_by_client
                .try_take(&client, TAKE_BACKS_PER_CLIENT, now)
            {
                return Err(ApiError::rate_limited(
                    limits.take_back_by_client.retry_after(&client, now),
                ));
            }
            if !limits
                .take_back_by_account
                .try_take(&caller.account, TAKE_BACKS_PER_ACCOUNT, now)
            {
                return Err(ApiError::rate_limited(
                    limits
                        .take_back_by_account
                        .retry_after(&caller.account, now),
                ));
            }
        }
        let Some(record) = self.record(&caller.account).await? else {
            return Err(ApiError::new(
                StatusCode::NOT_FOUND,
                "not_claimed",
                "This address has no password yet; set one instead.",
            ));
        };
        let account = caller.account.clone();
        let work = self
            .blocking(move |store| {
                let since = SystemTime::now()
                    .checked_sub(TAKE_BACK_WINDOW)
                    .unwrap_or(UNIX_EPOCH);
                store.miner_source_work_since(&account, since)
            })
            .await?;
        // A refusal says nothing about the IP's work, so guesses cannot
        // find which networks mine an address.
        let fail = |status, code: &str, message: &str| {
            let now = Instant::now();
            let used = self
                .limits
                .lock()
                .take_back_by_account
                .current(&caller.account, now);
            let left = TAKE_BACKS_PER_ACCOUNT.saturating_sub(used);
            ApiError::new(status, code, message).with("tries_left", json!(left))
        };
        match take_back_decision(&record, &rig_group, &work, SystemTime::now()) {
            TakeBack::CoolingDown(left) => Err(ApiError::new(
                StatusCode::TOO_MANY_REQUESTS,
                "cooling_down",
                "This address was taken back recently. Try again later.",
            )
            .with("retry_after", json!(left.as_secs().max(1)))),
            TakeBack::NotEnoughWork { share } => {
                tracing::info!(account = %caller.account, ip_group = %client, rig_group = %rig_group, share, "miner settings take-back refused");
                Err(fail(
                    StatusCode::FORBIDDEN,
                    "not_enough_work",
                    "That IP did less work here than the one that set the password.",
                ))
            }
            TakeBack::Allowed { share } => {
                let account = caller.account.clone();
                let owner = record.owner_ip_group.clone();
                let group = rig_group.clone();
                let taken = self
                    .blocking(move |store| {
                        store.take_back_miner_settings(&account, &owner, &group, SystemTime::now())
                    })
                    .await?;
                if !taken {
                    return Err(fail(
                        StatusCode::CONFLICT,
                        "changed",
                        "This address changed while checking. Try again.",
                    ));
                }
                tracing::info!(account = %caller.account, ip_group = %client, rig_group = %rig_group, share, "miner settings taken back");
                let record = self.record(&caller.account).await?.ok_or_else(|| {
                    ApiError::internal(anyhow::anyhow!("taken back settings row missing"))
                })?;
                // The new owner sets its own alert email.
                let alert_account = caller.account.clone();
                self.blocking(move |store| store.remove_miner_alert(&alert_account))
                    .await?;
                let token = self.unlock_session(&caller, &record).await?;
                let mut body = self.state_body(&caller, Some(&record), true, None);
                body["share"] = json!(share);
                Ok(respond_body(body, token))
            }
        }
    }

    /// An unlocked caller's record, or a 401.
    async fn require_unlocked(&self, caller: &Caller) -> Result<MinerSettingsRecord, ApiError> {
        let locked = || ApiError::new(StatusCode::UNAUTHORIZED, "locked", "Unlock settings first.");
        let record = self.record(&caller.account).await?.ok_or_else(locked)?;
        if self.unlocked(caller, &record).await? {
            Ok(record)
        } else {
            Err(locked())
        }
    }

    async fn set_password(&self, caller: Caller, request: PasswordRequest) -> ApiResult {
        password_ok(&request.password)?;
        let client = self.require_client(&caller)?;
        let unlocked = self.require_unlocked(&caller).await?;
        self.take_password_write(&client)?;
        let hash = self.hash_password(request.password).await?;
        let account = caller.account.clone();
        let changed = self
            .blocking(move |store| {
                store.set_miner_settings_password(
                    &account,
                    (unlocked.password_hash.as_deref(), &unlocked.owner_ip_group),
                    &hash,
                    SystemTime::now(),
                )
            })
            .await?;
        if !changed {
            // Taken back or changed elsewhere since this session unlocked.
            return Err(changed_elsewhere());
        }
        tracing::info!(account = %caller.account, ip_group = %client, "miner settings password changed");
        let record = self
            .record(&caller.account)
            .await?
            .ok_or_else(|| ApiError::internal(anyhow::anyhow!("settings row missing")))?;
        // Other sessions end with the old password; this one stays open.
        let token = self.unlock_session(&caller, &record).await?;
        self.respond(&caller, Some(&record), true, token).await
    }

    async fn save(&self, caller: Caller, request: SettingsRequest) -> ApiResult {
        let record = self.require_unlocked(&caller).await?;
        let default_minimum = self.default_minimum(&caller.account);
        let min_payout = match request.min_payout {
            None => None,
            Some(value) if value.is_finite() && value > 0.0 => {
                let atomic = (value * ATOMIC_PER_BNT as f64).round();
                if atomic > u64::MAX as f64 {
                    return Err(ApiError::new(
                        StatusCode::BAD_REQUEST,
                        "invalid_settings",
                        "That minimum payout is too large.",
                    ));
                }
                let maximum = self.max_min_payout(&caller.account);
                let atomic = validate_min_payout(atomic as u64, default_minimum, maximum).map_err(
                    |message| ApiError::new(StatusCode::BAD_REQUEST, "invalid_settings", &message),
                )?;
                // The account's own minimum is stored as "no choice", so it
                // follows the pool if that changes.
                (atomic != default_minimum).then_some(atomic)
            }
            Some(_) => {
                return Err(ApiError::new(
                    StatusCode::BAD_REQUEST,
                    "invalid_settings",
                    "Enter a minimum payout in BNT.",
                ))
            }
        };
        let start_difficulty = match request.start_difficulty {
            None => record.settings.start_difficulty,
            Some(None) => None,
            Some(Some(chosen)) => {
                let (min, max, _) = self.difficulty_range(&caller.account).await?;
                Some(start_difficulty_in_era(chosen, min, max).ok_or_else(|| {
                    ApiError::new(
                        StatusCode::BAD_REQUEST,
                        "invalid_settings",
                        &format!("Choose a starting difficulty from {min} to {max}."),
                    )
                })?)
            }
        };
        let settings = MinerSettings {
            min_payout,
            default_solo: request.default_solo,
            hide_workers: request.hide_workers,
            private_stats: request
                .private_stats
                .unwrap_or(record.settings.private_stats),
            start_difficulty,
        };
        if settings != record.settings {
            let account = caller.account.clone();
            let saved = settings.clone();
            let expected = (record.password_hash.clone(), record.owner_ip_group.clone());
            let updated = self
                .blocking(move |store| {
                    store.update_miner_settings(
                        &account,
                        (expected.0.as_deref(), &expected.1),
                        &saved,
                        SystemTime::now(),
                    )
                })
                .await?;
            if !updated {
                // Taken back or changed elsewhere since this session unlocked.
                return Err(changed_elsewhere());
            }
            self.forget_hide_cache(&caller.account);
            tracing::info!(
                account = %caller.account,
                ip_group = ?caller.client,
                min_payout = ?settings.min_payout,
                default_solo = settings.default_solo,
                hide_workers = settings.hide_workers,
                private_stats = settings.private_stats,
                start_difficulty = ?settings.start_difficulty,
                "miner settings saved"
            );
        }
        if let (Some(choices), Some(_)) = (request.alerts, &self.alerts) {
            if choices.offline_minutes != 0
                && !ALERT_OFFLINE_MINUTES.contains(&choices.offline_minutes)
            {
                return Err(ApiError::new(
                    StatusCode::BAD_REQUEST,
                    "invalid_settings",
                    "Choose 10, 30 or 60 minutes.",
                ));
            }
            if choices
                .hashrate_drop_minutes
                .is_some_and(|minutes| minutes != 0 && !ALERT_DROP_MINUTES.contains(&minutes))
            {
                return Err(ApiError::new(
                    StatusCode::BAD_REQUEST,
                    "invalid_settings",
                    "Choose 30 minutes, 1 hour or 2 hours.",
                ));
            }
            let account = caller.account.clone();
            self.blocking(move |store| {
                let Some(saved) = store.miner_alert(&account)? else {
                    return Ok(false);
                };
                let choices = StoredAlertChoices {
                    offline_minutes: choices.offline_minutes,
                    notify_payouts: choices.notify_payouts,
                    notify_solo_blocks: choices.notify_solo_blocks,
                    hashrate_drop_minutes: choices
                        .hashrate_drop_minutes
                        .unwrap_or(saved.hashrate_drop_minutes),
                    notify_updates: choices.notify_updates.unwrap_or(saved.notify_updates),
                    weekly_summary: choices.weekly_summary.unwrap_or(saved.weekly_summary),
                };
                store.update_miner_alert_choices(&account, &choices, SystemTime::now())
            })
            .await?;
        }
        let record = self
            .record(&caller.account)
            .await?
            .ok_or_else(|| ApiError::internal(anyhow::anyhow!("settings row missing")))?;
        self.respond(&caller, Some(&record), true, None).await
    }

    fn alert_service(&self) -> Result<&Arc<crate::alerts::AlertService>, ApiError> {
        self.alerts.as_ref().ok_or_else(|| {
            ApiError::new(
                StatusCode::NOT_FOUND,
                "alerts_unavailable",
                "Alert emails are not available on this pool.",
            )
        })
    }

    /// Sets the alert email and sends its confirmation link.
    async fn set_alert_email(&self, caller: Caller, request: AlertEmailRequest) -> ApiResult {
        let service = Arc::clone(self.alert_service()?);
        let email = request.email.trim().to_string();
        if !plausible_email(&email) {
            return Err(ApiError::new(
                StatusCode::BAD_REQUEST,
                "invalid_email",
                "Enter an email address, like you@example.com.",
            ));
        }
        let client = self.require_client(&caller)?;
        let record = self.require_unlocked(&caller).await?;
        // Only addresses that mine here get alerts, and one inbox gets only a
        // few confirmation emails a day however many addresses ask.
        let account = caller.account.clone();
        let recipient = email.clone();
        let (active, recent) = self
            .blocking(move |store| {
                let since = SystemTime::now()
                    .checked_sub(Duration::from_secs(86_400))
                    .unwrap_or(UNIX_EPOCH);
                Ok((
                    store.miner_has_any_activity(&account)?,
                    store.alert_confirmations_to(&recipient, since)?,
                ))
            })
            .await?;
        if !active {
            return Err(ApiError::new(
                StatusCode::CONFLICT,
                "not_mining",
                "Alert emails are available once this address has mined on the pool.",
            ));
        }
        if recent >= ALERT_EMAILS_PER_RECIPIENT {
            return Err(ApiError::rate_limited(3_600));
        }
        {
            let now = Instant::now();
            let mut limits = self.limits.lock();
            if !limits
                .alert_emails_total
                .try_take("all", ALERT_EMAILS_PER_HOUR, now)
            {
                return Err(ApiError::rate_limited(
                    limits.alert_emails_total.retry_after("all", now),
                ));
            }
            if !limits
                .alert_emails_by_client
                .try_take(&client, ALERT_EMAILS_PER_CLIENT, now)
            {
                return Err(ApiError::rate_limited(
                    limits.alert_emails_by_client.retry_after(&client, now),
                ));
            }
            if !limits.alert_emails_by_account.try_take(
                &caller.account,
                ALERT_EMAILS_PER_ACCOUNT,
                now,
            ) {
                return Err(ApiError::rate_limited(
                    limits
                        .alert_emails_by_account
                        .retry_after(&caller.account, now),
                ));
            }
        }
        let lang = crate::alerts::supported_lang(request.lang.trim()).to_string();
        let confirm_token = crate::alerts::new_token();
        let unsubscribe_token = crate::alerts::new_token();
        let hash = crate::alerts::token_hash(&confirm_token);
        let account = caller.account.clone();
        let stored_email = email.clone();
        let stored_lang = lang.clone();
        self.blocking(move |store| {
            store.set_miner_alert_email(
                &account,
                &stored_email,
                &stored_lang,
                &hash,
                &unsubscribe_token,
                SystemTime::now(),
            )
        })
        .await?;
        let mail = crate::alerts::confirm_mail(
            &service.config,
            &email,
            &lang,
            &caller.account,
            &confirm_token,
        );
        if let Err(err) = service.mailer.send(&mail).await {
            tracing::warn!(account = %caller.account, error = %err, "alert confirmation email failed");
            return Err(ApiError::new(
                StatusCode::BAD_GATEWAY,
                "email_failed",
                "The confirmation email could not be sent. Try again shortly.",
            ));
        }
        tracing::info!(account = %caller.account, "alert confirmation email sent");
        self.respond(&caller, Some(&record), true, None).await
    }

    async fn remove_alert_email(&self, caller: Caller) -> ApiResult {
        self.alert_service()?;
        let record = self.require_unlocked(&caller).await?;
        let account = caller.account.clone();
        self.blocking(move |store| store.remove_miner_alert_email(&account))
            .await?;
        tracing::info!(account = %caller.account, "alert email removed");
        self.respond(&caller, Some(&record), true, None).await
    }

    /// Checks that a Discord or Telegram link may be made for this account
    /// now: unlocked, mined here, and within the hourly limits.
    async fn chat_link_allowed(&self, caller: &Caller) -> Result<MinerSettingsRecord, ApiError> {
        let client = self.require_client(caller)?;
        let record = self.require_unlocked(caller).await?;
        let account = caller.account.clone();
        if !self
            .blocking(move |store| store.miner_has_any_activity(&account))
            .await?
        {
            return Err(ApiError::new(
                StatusCode::CONFLICT,
                "not_mining",
                "Alerts are available once this address has mined on the pool.",
            ));
        }
        let now = Instant::now();
        let mut limits = self.limits.lock();
        if !limits
            .chat_links_by_client
            .try_take(&client, CHAT_LINKS_PER_CLIENT, now)
        {
            return Err(ApiError::rate_limited(
                limits.chat_links_by_client.retry_after(&client, now),
            ));
        }
        if !limits
            .chat_links_by_account
            .try_take(&caller.account, CHAT_LINKS_PER_ACCOUNT, now)
        {
            return Err(ApiError::rate_limited(
                limits
                    .chat_links_by_account
                    .retry_after(&caller.account, now),
            ));
        }
        Ok(record)
    }

    /// Saves a Discord webhook once a test message posted to it.
    async fn set_discord(&self, caller: Caller, request: DiscordRequest) -> ApiResult {
        let service = Arc::clone(self.alert_service()?);
        let webhook = request.webhook.trim().to_string();
        if !discord_webhook_valid(&webhook) {
            return Err(ApiError::new(
                StatusCode::BAD_REQUEST,
                "invalid_webhook",
                "Paste a Discord webhook link (https://discord.com/api/webhooks/…).",
            ));
        }
        let record = self.chat_link_allowed(&caller).await?;
        let lang = crate::alerts::supported_lang(request.lang.trim()).to_string();
        let text = crate::alerts::bot_text(
            &lang,
            "discordTest",
            &[&crate::alerts::short_account(&caller.account)],
        );
        if let Err(err) = service.chat.discord(&webhook, &text).await {
            tracing::info!(account = %caller.account, error = %err, "discord test message failed");
            return Err(ApiError::new(
                StatusCode::BAD_GATEWAY,
                "discord_failed",
                "Discord did not accept the test message. Check the webhook link.",
            ));
        }
        let account = caller.account.clone();
        self.blocking(move |store| {
            let was_active =
                store.ensure_miner_alert(&account, &lang, &crate::alerts::new_token())?;
            store.set_miner_alert_discord(&account, Some(&webhook), was_active, SystemTime::now())
        })
        .await?;
        tracing::info!(account = %caller.account, "discord alerts connected");
        self.respond(&caller, Some(&record), true, None).await
    }

    async fn remove_discord(&self, caller: Caller) -> ApiResult {
        self.alert_service()?;
        let record = self.require_unlocked(&caller).await?;
        let account = caller.account.clone();
        self.blocking(move |store| {
            store.set_miner_alert_discord(&account, None, true, SystemTime::now())
        })
        .await?;
        tracing::info!(account = %caller.account, "discord alerts removed");
        self.respond(&caller, Some(&record), true, None).await
    }

    /// Makes a Connect Telegram link; the bot links the chat on Start.
    async fn connect_telegram(&self, caller: Caller, request: TelegramRequest) -> ApiResult {
        let service = Arc::clone(self.alert_service()?);
        let Some(bot) = service.telegram_bot.clone() else {
            return Err(ApiError::new(
                StatusCode::NOT_FOUND,
                "telegram_unavailable",
                "Telegram alerts are not available on this pool.",
            ));
        };
        let record = self.chat_link_allowed(&caller).await?;
        let lang = crate::alerts::supported_lang(request.lang.trim()).to_string();
        let code = crate::alerts::new_link_code();
        let hash = crate::alerts::token_hash(&code);
        let account = caller.account.clone();
        self.blocking(move |store| {
            store.ensure_miner_alert(&account, &lang, &crate::alerts::new_token())?;
            store.start_miner_alert_telegram(&account, &hash, SystemTime::now())
        })
        .await?;
        let alert = self.alert_for(&caller.account, true).await?;
        let mut body = self.state_body(&caller, Some(&record), true, alert.as_ref());
        body["alerts"]["telegram"] =
            json!({"pending": crate::alert_chat::telegram_link(&bot, &code)});
        Ok(respond_body(body, None))
    }

    async fn remove_telegram(&self, caller: Caller) -> ApiResult {
        self.alert_service()?;
        let record = self.require_unlocked(&caller).await?;
        let account = caller.account.clone();
        self.blocking(move |store| store.remove_miner_alert_telegram(&account))
            .await?;
        tracing::info!(account = %caller.account, "telegram alerts removed");
        self.respond(&caller, Some(&record), true, None).await
    }
}

fn normalize_account(raw: &str) -> String {
    pool_common::solo::split_solo_login(raw)
        .1
        .trim()
        .to_string()
}

fn peer(connect: Option<ConnectInfo<SocketAddr>>) -> Option<IpAddr> {
    connect.map(|ConnectInfo(addr)| addr.ip())
}

fn finish(result: ApiResult) -> Response {
    result.unwrap_or_else(IntoResponse::into_response)
}

async fn handle_get(
    State(service): State<Arc<MinerSettingsService>>,
    Path(account): Path<String>,
    connect: Option<ConnectInfo<SocketAddr>>,
    headers: HeaderMap,
) -> Response {
    let caller = service.caller(normalize_account(&account), &headers, peer(connect));
    finish(service.get_state(caller).await)
}

async fn handle_save(
    State(service): State<Arc<MinerSettingsService>>,
    Path(account): Path<String>,
    connect: Option<ConnectInfo<SocketAddr>>,
    headers: HeaderMap,
    Json(request): Json<SettingsRequest>,
) -> Response {
    let caller = service.caller(normalize_account(&account), &headers, peer(connect));
    finish(service.save(caller, request).await)
}

async fn handle_claim(
    State(service): State<Arc<MinerSettingsService>>,
    Path(account): Path<String>,
    connect: Option<ConnectInfo<SocketAddr>>,
    headers: HeaderMap,
    Json(request): Json<PasswordRequest>,
) -> Response {
    let caller = service.caller(normalize_account(&account), &headers, peer(connect));
    finish(service.claim(caller, request).await)
}

async fn handle_unlock(
    State(service): State<Arc<MinerSettingsService>>,
    Path(account): Path<String>,
    connect: Option<ConnectInfo<SocketAddr>>,
    headers: HeaderMap,
    Json(request): Json<PasswordRequest>,
) -> Response {
    let caller = service.caller(normalize_account(&account), &headers, peer(connect));
    finish(service.unlock(caller, request).await)
}

async fn handle_sign_out(
    State(service): State<Arc<MinerSettingsService>>,
    Path(account): Path<String>,
    connect: Option<ConnectInfo<SocketAddr>>,
    headers: HeaderMap,
) -> Response {
    let caller = service.caller(normalize_account(&account), &headers, peer(connect));
    finish(service.sign_out(caller).await)
}

async fn handle_take_back(
    State(service): State<Arc<MinerSettingsService>>,
    Path(account): Path<String>,
    connect: Option<ConnectInfo<SocketAddr>>,
    headers: HeaderMap,
    Json(request): Json<TakeBackRequest>,
) -> Response {
    let caller = service.caller(normalize_account(&account), &headers, peer(connect));
    finish(service.take_back(caller, request).await)
}

async fn handle_password(
    State(service): State<Arc<MinerSettingsService>>,
    Path(account): Path<String>,
    connect: Option<ConnectInfo<SocketAddr>>,
    headers: HeaderMap,
    Json(request): Json<PasswordRequest>,
) -> Response {
    let caller = service.caller(normalize_account(&account), &headers, peer(connect));
    finish(service.set_password(caller, request).await)
}

async fn handle_alert_email(
    State(service): State<Arc<MinerSettingsService>>,
    Path(account): Path<String>,
    connect: Option<ConnectInfo<SocketAddr>>,
    headers: HeaderMap,
    Json(request): Json<AlertEmailRequest>,
) -> Response {
    let caller = service.caller(normalize_account(&account), &headers, peer(connect));
    finish(service.set_alert_email(caller, request).await)
}

async fn handle_alert_email_remove(
    State(service): State<Arc<MinerSettingsService>>,
    Path(account): Path<String>,
    connect: Option<ConnectInfo<SocketAddr>>,
    headers: HeaderMap,
) -> Response {
    let caller = service.caller(normalize_account(&account), &headers, peer(connect));
    finish(service.remove_alert_email(caller).await)
}

async fn handle_discord(
    State(service): State<Arc<MinerSettingsService>>,
    Path(account): Path<String>,
    connect: Option<ConnectInfo<SocketAddr>>,
    headers: HeaderMap,
    Json(request): Json<DiscordRequest>,
) -> Response {
    let caller = service.caller(normalize_account(&account), &headers, peer(connect));
    finish(service.set_discord(caller, request).await)
}

async fn handle_discord_remove(
    State(service): State<Arc<MinerSettingsService>>,
    Path(account): Path<String>,
    connect: Option<ConnectInfo<SocketAddr>>,
    headers: HeaderMap,
) -> Response {
    let caller = service.caller(normalize_account(&account), &headers, peer(connect));
    finish(service.remove_discord(caller).await)
}

async fn handle_telegram(
    State(service): State<Arc<MinerSettingsService>>,
    Path(account): Path<String>,
    connect: Option<ConnectInfo<SocketAddr>>,
    headers: HeaderMap,
    Json(request): Json<TelegramRequest>,
) -> Response {
    let caller = service.caller(normalize_account(&account), &headers, peer(connect));
    finish(service.connect_telegram(caller, request).await)
}

async fn handle_telegram_remove(
    State(service): State<Arc<MinerSettingsService>>,
    Path(account): Path<String>,
    connect: Option<ConnectInfo<SocketAddr>>,
    headers: HeaderMap,
) -> Response {
    let caller = service.caller(normalize_account(&account), &headers, peer(connect));
    finish(service.remove_telegram(caller).await)
}

pub(crate) fn router<S>(service: Arc<MinerSettingsService>) -> Router<S>
where
    S: Clone + Send + Sync + 'static,
{
    let body_limit = axum::extract::DefaultBodyLimit::max(2 * 1024);
    Router::new()
        .route(
            "/api/miner-settings/:account",
            get(handle_get).put(handle_save),
        )
        .route("/api/miner-settings/:account/claim", post(handle_claim))
        .route("/api/miner-settings/:account/unlock", post(handle_unlock))
        .route(
            "/api/miner-settings/:account/sign-out",
            post(handle_sign_out),
        )
        .route(
            "/api/miner-settings/:account/take-back",
            post(handle_take_back),
        )
        .route(
            "/api/miner-settings/:account/password",
            post(handle_password),
        )
        .route(
            "/api/miner-settings/:account/alert-email",
            post(handle_alert_email),
        )
        .route(
            "/api/miner-settings/:account/alert-email/remove",
            post(handle_alert_email_remove),
        )
        .route("/api/miner-settings/:account/discord", post(handle_discord))
        .route(
            "/api/miner-settings/:account/discord/remove",
            post(handle_discord_remove),
        )
        .route(
            "/api/miner-settings/:account/telegram",
            post(handle_telegram),
        )
        .route(
            "/api/miner-settings/:account/telegram/remove",
            post(handle_telegram_remove),
        )
        .layer(body_limit)
        .with_state(service)
}

/// Renames workers to "Rig 1", "Rig 2", … in name order, so the numbering is
/// stable between refreshes.
pub(crate) fn rig_names<'a>(names: impl Iterator<Item = &'a str>) -> HashMap<String, String> {
    let mut sorted = names.map(str::to_string).collect::<Vec<_>>();
    sorted.sort();
    sorted.dedup();
    sorted
        .into_iter()
        .enumerate()
        .map(|(index, name)| (name, format!("Rig {}", index + 1)))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rig_ips_parse_to_the_stratum_ip_group() {
        assert_eq!(
            parse_rig_ip(" 203.0.113.24 ").as_deref(),
            Some("203.0.113.24")
        );
        assert_eq!(
            parse_rig_ip("::ffff:203.0.113.24").as_deref(),
            Some("203.0.113.24")
        );
        assert_eq!(
            parse_rig_ip("2001:db8:1:2:aaaa::1").as_deref(),
            Some("2001:db8:1:2::/64")
        );
        assert_eq!(
            parse_rig_ip("2001:db8:1:2::/64").as_deref(),
            Some("2001:db8:1:2::/64")
        );
        assert_eq!(parse_rig_ip("not an ip"), None);
        assert_eq!(parse_rig_ip("127.0.0.1"), None);
        assert_eq!(parse_rig_ip("0.0.0.0"), None);
    }

    #[test]
    fn session_cookie_is_scoped_and_unreadable_by_scripts() {
        let cookie = session_cookie(&"a".repeat(64));
        let cookie = cookie.to_str().unwrap();
        assert!(cookie.contains("HttpOnly"));
        assert!(cookie.contains("SameSite=Strict"));
        assert!(cookie.contains("Secure"));
        assert!(cookie.contains("Max-Age=34560000"));
        assert!(clear_session_cookie()
            .to_str()
            .unwrap()
            .contains("Max-Age=0"));
        let mut headers = HeaderMap::new();
        headers.insert(
            header::COOKIE,
            HeaderValue::from_str(&format!("x=1; {SESSION_COOKIE}={}; y=2", "b".repeat(64)))
                .unwrap(),
        );
        assert_eq!(session_token(&headers), Some("b".repeat(64)));
        headers.insert(
            header::COOKIE,
            HeaderValue::from_static("pool_settings=short"),
        );
        assert_eq!(session_token(&headers), None);
    }

    #[test]
    fn attempt_limits_count_per_key_and_fail_closed_when_full() {
        let now = Instant::now();
        let mut limiter = AttemptLimiter::new();
        for _ in 0..3 {
            assert!(limiter.record("a", now));
        }
        assert_eq!(limiter.current("a", now), 3);
        assert_eq!(limiter.current("a", now + ATTEMPT_WINDOW), 0);
        // Attempts are taken before they run: a burst stops at the limit.
        let taken = (0..10)
            .filter(|_| limiter.try_take("burst", 5, now))
            .count();
        assert_eq!(taken, 5);
        limiter.release("burst");
        assert!(limiter.try_take("burst", 5, now));
        assert!(!limiter.try_take("burst", 5, now));
        for index in 0..ATTEMPT_MAX_KEYS {
            limiter.entries.insert(format!("k{index}"), (now, 1));
        }
        assert!(!limiter.try_take("new", 5, now));
        assert!(!limiter.record("new", now));
        // Expired entries make room again.
        assert!(limiter.try_take("new", 5, now + ATTEMPT_WINDOW));
    }

    #[test]
    fn owner_stamps_change_with_the_password_and_the_owner() {
        let record = MinerSettingsRecord {
            account: "a".into(),
            password_hash: Some("one".into()),
            owner_ip_group: "203.0.113.1".into(),
            claimed_at: UNIX_EPOCH,
            changed_at: UNIX_EPOCH,
            taken_back_at: None,
            settings: MinerSettings::default(),
        };
        let mut other = record.clone();
        other.password_hash = Some("two".into());
        assert_ne!(owner_stamp(&record), owner_stamp(&other));
        let mut moved = record.clone();
        moved.owner_ip_group = "203.0.113.2".into();
        assert_ne!(owner_stamp(&record), owner_stamp(&moved));
        let mut settings = record.clone();
        settings.settings.hide_workers = true;
        assert_eq!(owner_stamp(&record), owner_stamp(&settings));
    }

    #[test]
    fn rigs_are_numbered_in_name_order() {
        let names = rig_names(["laptop", "rig-1", "laptop", "basement"].into_iter());
        assert_eq!(names["basement"], "Rig 1");
        assert_eq!(names["laptop"], "Rig 2");
        assert_eq!(names["rig-1"], "Rig 3");
    }

    fn test_address() -> String {
        use curve25519_dalek::{RistrettoPoint, Scalar};
        use sha3::{Digest as _, Sha3_256};
        let seed = rand::random::<u64>() >> 2;
        let spend = RistrettoPoint::mul_base(&Scalar::from(seed * 2 + 1));
        let view = RistrettoPoint::mul_base(&Scalar::from(seed * 2 + 2));
        let mut encoded = Vec::with_capacity(68);
        encoded.extend_from_slice(spend.compress().as_bytes());
        encoded.extend_from_slice(view.compress().as_bytes());
        let mut hasher = Sha3_256::new();
        hasher.update(b"blocknet_stealth_address_checksum");
        hasher.update(b"blocknet_mainnet");
        hasher.update(&encoded);
        let checksum = hasher.finalize();
        encoded.extend_from_slice(&checksum[..4]);
        bs58::encode(encoded).into_string()
    }

    struct Client {
        app: Router,
        ip: &'static str,
        cookie: Option<String>,
    }

    impl Client {
        async fn send(
            &mut self,
            method: &str,
            path: &str,
            body: Option<Value>,
        ) -> (StatusCode, Value) {
            use tower::ServiceExt;
            let mut request = axum::http::Request::builder()
                .method(method)
                .uri(path)
                .header("x-real-ip", self.ip);
            if let Some(cookie) = &self.cookie {
                request = request.header(header::COOKIE, cookie);
            }
            let request = match body {
                Some(body) => request
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(axum::body::Body::from(body.to_string())),
                None => request.body(axum::body::Body::empty()),
            }
            .unwrap();
            let response = self.app.clone().oneshot(request).await.unwrap();
            if let Some(cookie) = response.headers().get(header::SET_COOKIE) {
                let cookie = cookie.to_str().unwrap();
                self.cookie = Some(cookie.split(';').next().unwrap().to_string());
            }
            let status = response.status();
            let bytes = axum::body::to_bytes(response.into_body(), 1 << 20)
                .await
                .unwrap();
            let value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
            (status, value)
        }
    }

    #[tokio::test]
    async fn sessions_outlast_restarts_until_sign_out_or_idle() {
        let Ok(url) = std::env::var("BLOCKNET_POOL_TEST_POSTGRES_URL") else {
            eprintln!("skipping postgres test: set BLOCKNET_POOL_TEST_POSTGRES_URL");
            return;
        };
        let store = tokio::task::spawn_blocking(move || PoolStore::open(&url, 2))
            .await
            .unwrap()
            .expect("open test store");
        let service = |store: &Arc<PoolStore>| {
            Arc::new(MinerSettingsService::new(
                Arc::clone(store),
                RuntimeConfig::default(),
                Arc::new(ClientIpPolicy::default()),
                None,
            ))
        };
        let first = service(&store);
        let account = test_address();
        let second_account = test_address();
        let path = format!("/api/miner-settings/{account}");
        let mut owner = Client {
            app: router(Arc::clone(&first)),
            ip: "198.51.100.7",
            cookie: None,
        };
        let (status, state) = owner
            .send(
                "POST",
                &format!("{path}/claim"),
                Some(json!({"password": "correct horse"})),
            )
            .await;
        assert_eq!(status, StatusCode::OK, "{state}");
        let (status, _) = owner
            .send(
                "POST",
                &format!("/api/miner-settings/{second_account}/claim"),
                Some(json!({"password": "correct horse"})),
            )
            .await;
        assert_eq!(status, StatusCode::OK);
        let cookie = owner.cookie.clone().unwrap();
        let token = cookie.strip_prefix("pool_settings=").unwrap().to_string();

        // A new API process still knows the browser.
        let restarted = service(&store);
        owner.app = router(Arc::clone(&restarted));
        let (_, state) = owner.send("GET", &path, None).await;
        assert_eq!(state["unlocked"], true);
        assert_eq!(owner.cookie.as_deref(), Some(cookie.as_str()));

        // Signing out of one account keeps the other and the cookie.
        let (status, state) = owner.send("POST", &format!("{path}/sign-out"), None).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(state["unlocked"], false);
        assert_eq!(owner.cookie.as_deref(), Some(cookie.as_str()));
        let (_, state) = owner.send("GET", &path, None).await;
        assert_eq!(state["unlocked"], false);
        let second_path = format!("/api/miner-settings/{second_account}");
        let (_, state) = owner.send("GET", &second_path, None).await;
        assert_eq!(state["unlocked"], true);

        // A session idle past the limit ends.
        let hash = token_hash(&token);
        let idle_account = second_account.clone();
        let stale = SystemTime::now() - SESSION_IDLE - Duration::from_secs(60);
        let reset = Arc::clone(&store);
        tokio::task::spawn_blocking(move || {
            reset.put_miner_settings_session(
                &hash,
                &idle_account,
                &owner_stamp(&reset.miner_settings(&idle_account)?.unwrap()),
                stale,
                MAX_ACCOUNTS_PER_SESSION,
            )
        })
        .await
        .unwrap()
        .unwrap();
        let (_, state) = owner.send("GET", &second_path, None).await;
        assert_eq!(state["unlocked"], false);

        // Signing out of the last account clears the cookie.
        let (status, _) = owner
            .send("POST", &format!("{second_path}/sign-out"), None)
            .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(owner.cookie.as_deref(), Some("pool_settings="));
    }

    #[tokio::test]
    async fn claim_unlock_save_and_take_back_over_http() {
        let Ok(url) = std::env::var("BLOCKNET_POOL_TEST_POSTGRES_URL") else {
            eprintln!("skipping postgres test: set BLOCKNET_POOL_TEST_POSTGRES_URL");
            return;
        };
        let store = tokio::task::spawn_blocking(move || PoolStore::open(&url, 2))
            .await
            .unwrap()
            .expect("open test store");
        let runtime = RuntimeConfig {
            min_payout_amount: 1.0,
            ..Default::default()
        };
        let service = Arc::new(MinerSettingsService::new(
            Arc::clone(&store),
            runtime,
            Arc::new(ClientIpPolicy::default()),
            None,
        ));
        let app: Router = router(Arc::clone(&service));
        let account = test_address();
        let path = format!("/api/miner-settings/{account}");
        let mut owner = Client {
            app: app.clone(),
            ip: "198.51.100.7",
            cookie: None,
        };
        let mut stranger = Client {
            app: app.clone(),
            ip: "192.0.2.50",
            cookie: None,
        };

        let (status, state) = stranger.send("GET", &path, None).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(state["claimed"], false);
        assert_eq!(state["your_ip"], "192.0.2.50");
        assert_eq!(
            stranger
                .send("GET", "/api/miner-settings/not-an-address", None)
                .await
                .0,
            StatusCode::BAD_REQUEST
        );

        // Short passwords are refused; the first real one claims the address.
        let (status, _) = owner
            .send(
                "POST",
                &format!("{path}/claim"),
                Some(json!({"password": "short"})),
            )
            .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        let (status, state) = owner
            .send(
                "POST",
                &format!("{path}/claim"),
                Some(json!({"password": "correct horse"})),
            )
            .await;
        assert_eq!(status, StatusCode::OK, "{state}");
        assert_eq!(state["unlocked"], true);
        assert_eq!(state["settings"]["min_payout"], 1.0);
        assert_eq!(state["pool_min_payout"], 1.0);
        assert_eq!(state["default_min_payout"], 1.0);
        assert!(owner.cookie.is_some());
        let (status, _) = stranger
            .send(
                "POST",
                &format!("{path}/claim"),
                Some(json!({"password": "another one"})),
            )
            .await;
        assert_eq!(status, StatusCode::CONFLICT);

        // Saving needs an unlocked session and valid values.
        let save = json!({"min_payout": 5.0, "default_solo": true, "hide_workers": true});
        assert_eq!(
            stranger.send("PUT", &path, Some(save.clone())).await.0,
            StatusCode::UNAUTHORIZED
        );
        let (status, _) = owner
            .send(
                "PUT",
                &path,
                Some(json!({"min_payout": 0.5, "default_solo": false, "hide_workers": false})),
            )
            .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        let (status, state) = owner.send("PUT", &path, Some(save)).await;
        assert_eq!(status, StatusCode::OK, "{state}");
        assert_eq!(state["settings"]["min_payout"], 5.0);
        assert_eq!(state["settings"]["default_solo"], true);
        assert_eq!(
            service.effective_min_payout_coins(&account, Some(5 * ATOMIC_PER_BNT)),
            5.0
        );

        // Hidden worker names stay visible to the unlocked owner only.
        let owner_headers = {
            let mut headers = HeaderMap::new();
            headers.insert(
                header::COOKIE,
                HeaderValue::from_str(owner.cookie.as_ref().unwrap()).unwrap(),
            );
            headers
        };
        assert_eq!(
            service.public_view_for(&account, &HeaderMap::new()).await,
            PublicView::HideNames
        );
        assert_eq!(
            service.public_view_for(&account, &owner_headers).await,
            PublicView::Full
        );

        // A stranger guessing the password runs out of tries.
        for left in (0..UNLOCK_FAILURES_PER_ACCOUNT).rev() {
            let (status, body) = stranger
                .send(
                    "POST",
                    &format!("{path}/unlock"),
                    Some(json!({"password": "guess guess"})),
                )
                .await;
            assert_eq!(status, StatusCode::UNAUTHORIZED);
            assert_eq!(body["tries_left"], left);
        }
        let (status, _) = stranger
            .send(
                "POST",
                &format!("{path}/unlock"),
                Some(json!({"password": "correct horse"})),
            )
            .await;
        assert_eq!(
            status,
            StatusCode::TOO_MANY_REQUESTS,
            "even the right password waits"
        );

        // Take-backs: no work, then more work than the owner's network.
        let (status, body) = stranger
            .send(
                "POST",
                &format!("{path}/take-back"),
                Some(json!({"ip": "203.0.113.24"})),
            )
            .await;
        assert_eq!(status, StatusCode::FORBIDDEN);
        assert_eq!(body["error"], "not_enough_work");
        let hour = pool_runtime::miner_settings::source_work_hour(SystemTime::now());
        let rows = vec![
            (account.clone(), "198.51.100.7".to_string(), hour, 10),
            (account.clone(), "203.0.113.24".to_string(), hour, 40),
        ];
        let work_store = Arc::clone(&store);
        tokio::task::spawn_blocking(move || work_store.add_miner_source_work(&rows))
            .await
            .unwrap()
            .unwrap();
        let (status, body) = stranger
            .send(
                "POST",
                &format!("{path}/take-back"),
                Some(json!({"ip": "203.0.113.24"})),
            )
            .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["share"], 0.8);
        assert_eq!(body["has_password"], false);
        assert_eq!(body["unlocked"], true);
        // The old password and the owner's session no longer work.
        let (_, state) = owner.send("GET", &path, None).await;
        assert_eq!(state["unlocked"], false);
        let (status, state) = stranger
            .send(
                "POST",
                &format!("{path}/password"),
                Some(json!({"password": "new password"})),
            )
            .await;
        assert_eq!(status, StatusCode::OK, "{state}");
        assert_eq!(state["has_password"], true);
        assert_eq!(
            state["settings"]["hide_workers"], true,
            "settings survive a take-back"
        );
        // The old owner's network cannot take it straight back.
        let (status, body) = owner
            .send(
                "POST",
                &format!("{path}/take-back"),
                Some(json!({"ip": "198.51.100.7"})),
            )
            .await;
        assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(body["error"], "cooling_down");
    }

    #[tokio::test]
    async fn alert_emails_confirm_then_report_stopped_and_returning_rigs() {
        use crate::alert_mail::{AlertsConfig, MemoryMailer};
        use crate::alerts::{run_cycle, AlertService, WorkerContext};
        let Ok(url) = std::env::var("BLOCKNET_POOL_TEST_POSTGRES_URL") else {
            eprintln!("skipping postgres test: set BLOCKNET_POOL_TEST_POSTGRES_URL");
            return;
        };
        let store = tokio::task::spawn_blocking(move || PoolStore::open(&url, 2))
            .await
            .unwrap()
            .expect("open test store");
        let mailer = Arc::new(MemoryMailer::default());
        let chat = Arc::new(crate::alert_chat::MemoryChat::default());
        let alerts = Arc::new(AlertService {
            config: AlertsConfig::default(),
            mailer: Arc::clone(&mailer) as Arc<dyn crate::alert_mail::Mailer>,
            chat: Arc::clone(&chat) as Arc<dyn crate::alert_chat::ChatSender>,
            telegram_bot: Some("bntpool_alerts_bot".into()),
            telegram_poller: None,
        });
        let service = Arc::new(MinerSettingsService::new(
            Arc::clone(&store),
            RuntimeConfig {
                min_payout_amount: 1.0,
                ..Default::default()
            },
            Arc::new(ClientIpPolicy::default()),
            Some(Arc::clone(&alerts)),
        ));
        let app: Router =
            router(Arc::clone(&service)).merge(crate::alerts::router(Arc::clone(&store)));
        let account = test_address();
        let path = format!("/api/miner-settings/{account}");
        // A fresh inbox each run: the shared test database remembers how many
        // confirmations an address got today.
        let inbox = format!("miner-{}@example.com", rand::random::<u32>());
        let mut owner = Client {
            app: app.clone(),
            ip: "198.51.100.7",
            cookie: None,
        };
        let (status, _) = owner
            .send(
                "POST",
                &format!("{path}/claim"),
                Some(json!({"password": "correct horse"})),
            )
            .await;
        assert_eq!(status, StatusCode::OK);

        // Only an address that has mined may set one.
        let (status, body) = owner
            .send(
                "POST",
                &format!("{path}/alert-email"),
                Some(json!({"email": inbox, "lang": "en"})),
            )
            .await;
        assert_eq!(status, StatusCode::CONFLICT, "{body}");
        assert_eq!(body["error"], "not_mining");
        let seed = Arc::clone(&store);
        let first = pool_runtime::engine::ShareRecord {
            job_id: format!("alert-first-{}", rand::random::<u64>()),
            miner: account.clone(),
            worker: "old-rig".into(),
            difficulty: 6_000,
            nonce: rand::random::<u32>() as u64,
            status: "verified",
            was_sampled: true,
            block_hash: None,
            claimed_hash: None,
            reject_reason: None,
            created_at: SystemTime::now() - Duration::from_secs(5 * 3600),
            solo: false,
        };
        tokio::task::spawn_blocking(move || seed.add_share(first).unwrap())
            .await
            .unwrap();

        // A bad address is refused; a good one gets a confirmation email.
        let (status, _) = owner
            .send(
                "POST",
                &format!("{path}/alert-email"),
                Some(json!({"email": "not an email"})),
            )
            .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        let (status, state) = owner
            .send(
                "POST",
                &format!("{path}/alert-email"),
                Some(json!({"email": inbox, "lang": "en"})),
            )
            .await;
        assert_eq!(status, StatusCode::OK, "{state}");
        assert_eq!(state["alerts"]["email"], inbox);
        assert_eq!(state["alerts"]["confirmed"], false);
        let confirm = mailer.sent.lock().pop().expect("confirmation email");
        assert_eq!(confirm.to, inbox);
        assert!(confirm.unsubscribe_url.is_none());
        let token = confirm
            .text
            .split("confirm=")
            .nth(1)
            .map(|rest| rest[..64].to_string())
            .expect("token in link");

        // Nothing is sent before the link is opened.
        let context = WorkerContext {
            store: Arc::clone(&store),
            service: Arc::clone(&alerts),
            pool_fee_pct: 0.0,
            confirmations: 60,
            // The pool has been up long enough to judge rigs.
            pool_up_since: parking_lot::Mutex::new(Some(
                SystemTime::now() - Duration::from_secs(3_600),
            )),
            miner_release: pool_runtime::miner_release::MinerRelease::new("0.3.1"),
        };
        let now = SystemTime::now();
        let share = |worker: &str, at: SystemTime, nonce: u64| pool_runtime::engine::ShareRecord {
            job_id: format!("alert-test-{nonce}"),
            miner: account.clone(),
            worker: worker.to_string(),
            difficulty: 6_000,
            nonce,
            status: "verified",
            was_sampled: true,
            block_hash: None,
            claimed_hash: None,
            reject_reason: None,
            created_at: at,
            solo: false,
        };
        let base = rand::random::<u32>() as u64 * 1_000;
        let seed = Arc::clone(&store);
        let shares = vec![
            share("rig-1", now - Duration::from_secs(20 * 60), base + 1),
            share("laptop", now - Duration::from_secs(30), base + 2),
        ];
        tokio::task::spawn_blocking(move || {
            for share in shares {
                seed.add_share(share).unwrap();
            }
        })
        .await
        .unwrap();
        run_cycle(&context, now).await.unwrap();
        assert!(mailer.sent.lock().is_empty(), "unconfirmed");

        let mut stranger = Client {
            app: app.clone(),
            ip: "192.0.2.50",
            cookie: None,
        };
        let (status, _) = stranger
            .send(
                "POST",
                "/api/alerts/confirm",
                Some(json!({"token": "f".repeat(64)})),
            )
            .await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        let (status, body) = stranger
            .send("POST", "/api/alerts/confirm", Some(json!({"token": token})))
            .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let (_, state) = owner.send("GET", &path, None).await;
        assert_eq!(state["alerts"]["confirmed"], true);
        // Strangers never see the alert email.
        let (_, public) = stranger.send("GET", &path, None).await;
        assert!(public.get("alerts").is_none());

        let (status, state) = owner
            .send("PUT", &path, Some(json!({
                "min_payout": null, "default_solo": false, "hide_workers": false,
                "alerts": {"offline_minutes": 10, "notify_payouts": true, "notify_solo_blocks": true}
            })))
            .await;
        assert_eq!(status, StatusCode::OK, "{state}");
        assert_eq!(state["alerts"]["offline_minutes"], 10);
        let (status, _) = owner
            .send("PUT", &path, Some(json!({
                "min_payout": null, "default_solo": false, "hide_workers": false,
                "alerts": {"offline_minutes": 7, "notify_payouts": true, "notify_solo_blocks": true}
            })))
            .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);

        // rig-1 has been quiet for 20 minutes: one email, not repeated.
        run_cycle(&context, now).await.unwrap();
        let sent = std::mem::take(&mut *mailer.sent.lock());
        assert_eq!(sent.len(), 1, "{sent:?}");
        assert_eq!(sent[0].subject, "rig-1 stopped mining");
        assert!(sent[0].text.contains("laptop"));
        assert!(sent[0].unsubscribe_url.is_some());
        run_cycle(&context, now).await.unwrap();
        assert!(mailer.sent.lock().is_empty(), "not repeated");

        // It mines again.
        let seed = Arc::clone(&store);
        let back = share("rig-1", now, base + 3);
        tokio::task::spawn_blocking(move || seed.add_share(back).unwrap())
            .await
            .unwrap();
        run_cycle(&context, now + Duration::from_secs(60))
            .await
            .unwrap();
        let sent = std::mem::take(&mut *mailer.sent.lock());
        assert_eq!(sent.len(), 1, "{sent:?}");
        assert_eq!(sent[0].subject, "rig-1 is mining again");

        // A solo block found after confirming.
        let seed = Arc::clone(&store);
        let block_account = account.clone();
        // Above any block earlier runs left in the shared test database.
        let height = 3_000_000_000
            + SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64;
        tokio::task::spawn_blocking(move || {
            seed.add_block(&pool_common::db::DbBlock {
                height,
                hash: format!("alert-test-{height}"),
                difficulty: 1,
                finder: block_account,
                finder_worker: "rig-1".into(),
                reward: 43_946_080_260,
                timestamp: SystemTime::now(),
                confirmed: false,
                orphaned: false,
                paid_out: false,
                effort_pct: None,
                solo: true,
            })
            .unwrap()
        })
        .await
        .unwrap();
        run_cycle(&context, now + Duration::from_secs(120))
            .await
            .unwrap();
        let sent = std::mem::take(&mut *mailer.sent.lock());
        assert_eq!(sent.len(), 1, "{sent:?}");
        assert_eq!(sent[0].subject, "You found a block: 439.4608026 BNT");

        // One-click unsubscribe from the email header.
        let one_click = sent[0].unsubscribe_url.clone().unwrap();
        let query = one_click.split("/api/alerts/unsubscribe").nth(1).unwrap();
        let (status, _) = stranger
            .send("POST", &format!("/api/alerts/unsubscribe{query}"), None)
            .await;
        assert_eq!(status, StatusCode::OK);
        let (_, state) = owner.send("GET", &path, None).await;
        assert_eq!(state["alerts"], Value::Null);
        run_cycle(&context, now + Duration::from_secs(180))
            .await
            .unwrap();
        assert!(mailer.sent.lock().is_empty());
    }

    #[tokio::test]
    async fn chat_alerts_private_stats_and_starting_difficulty() {
        use crate::alert_mail::{AlertsConfig, MemoryMailer};
        use crate::alerts::{run_cycle, AlertService, WorkerContext};
        let Ok(url) = std::env::var("BLOCKNET_POOL_TEST_POSTGRES_URL") else {
            eprintln!("skipping postgres test: set BLOCKNET_POOL_TEST_POSTGRES_URL");
            return;
        };
        let store = tokio::task::spawn_blocking(move || PoolStore::open(&url, 2))
            .await
            .unwrap()
            .expect("open test store");
        let mailer = Arc::new(MemoryMailer::default());
        let chat = Arc::new(crate::alert_chat::MemoryChat::default());
        let alerts = Arc::new(AlertService {
            config: AlertsConfig::default(),
            mailer: Arc::clone(&mailer) as Arc<dyn crate::alert_mail::Mailer>,
            chat: Arc::clone(&chat) as Arc<dyn crate::alert_chat::ChatSender>,
            telegram_bot: Some("bntpool_alerts_bot".into()),
            telegram_poller: None,
        });
        let service = Arc::new(MinerSettingsService::new(
            Arc::clone(&store),
            RuntimeConfig {
                min_payout_amount: 1.0,
                ..Default::default()
            },
            Arc::new(ClientIpPolicy::default()),
            Some(Arc::clone(&alerts)),
        ));
        let app: Router = router(Arc::clone(&service));
        let account = test_address();
        let path = format!("/api/miner-settings/{account}");
        let mut owner = Client {
            app: app.clone(),
            ip: "198.51.100.8",
            cookie: None,
        };
        let (status, _) = owner
            .send(
                "POST",
                &format!("{path}/claim"),
                Some(json!({"password": "correct horse"})),
            )
            .await;
        assert_eq!(status, StatusCode::OK);
        let now = SystemTime::now();
        let base = rand::random::<u32>() as u64 * 1_000;
        let share = |worker: &str, at: SystemTime, nonce: u64| pool_runtime::engine::ShareRecord {
            job_id: format!("chat-test-{nonce}"),
            miner: account.clone(),
            worker: worker.to_string(),
            difficulty: 6_000,
            nonce,
            status: "verified",
            was_sampled: true,
            block_hash: None,
            claimed_hash: None,
            reject_reason: None,
            created_at: at,
            solo: false,
        };
        let seed = Arc::clone(&store);
        let shares = vec![
            share("rig-1", now - Duration::from_secs(20 * 60), base + 1),
            share("laptop", now - Duration::from_secs(30), base + 2),
        ];
        tokio::task::spawn_blocking(move || {
            for share in shares {
                seed.add_share(share).unwrap();
            }
        })
        .await
        .unwrap();

        // Discord: only channel webhooks, saved once the test message posts.
        let webhook = "https://discord.com/api/webhooks/1204567890/AbCdEfGhIjKlMnOpQrStUvWx3qXw";
        let (status, body) = owner
            .send("POST", &format!("{path}/discord"),
                Some(json!({"webhook": "https://example.com/api/webhooks/1/abcdefghijklmnopqrstuvwx"})))
            .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["error"], "invalid_webhook");
        let (status, state) = owner
            .send(
                "POST",
                &format!("{path}/discord"),
                Some(json!({"webhook": webhook, "lang": "en"})),
            )
            .await;
        assert_eq!(status, StatusCode::OK, "{state}");
        assert_eq!(
            state["alerts"]["discord"],
            "discord.com/api/webhooks/1204…3qXw"
        );
        assert_eq!(state["alerts"]["email"], "");
        let test_post = chat.sent.lock().pop().expect("test message");
        assert_eq!(test_post.0, webhook);
        assert!(test_post.1.contains("will be posted in this channel"));

        // Telegram: a one-time link, linked when the bot sees Start.
        let (status, state) = owner
            .send(
                "POST",
                &format!("{path}/telegram"),
                Some(json!({"lang": "en"})),
            )
            .await;
        assert_eq!(status, StatusCode::OK, "{state}");
        let link = state["alerts"]["telegram"]["pending"]
            .as_str()
            .unwrap()
            .to_string();
        let code = link
            .strip_prefix("https://t.me/bntpool_alerts_bot?start=")
            .expect("bot link")
            .to_string();
        let (_, state) = owner.send("GET", &path, None).await;
        assert_eq!(state["alerts"]["telegram"]["pending"], "");
        let chat_id = -(rand::random::<u32>() as i64) - 1;
        let link_store = Arc::clone(&store);
        let hash = crate::alerts::token_hash(&code);
        let linked = tokio::task::spawn_blocking(move || {
            let first = link_store.finish_miner_alert_telegram(
                &hash,
                chat_id,
                "@ada",
                SystemTime::now(),
            )?;
            let again = link_store.finish_miner_alert_telegram(
                &hash,
                chat_id,
                "@ada",
                SystemTime::now(),
            )?;
            anyhow::Ok((first, again))
        })
        .await
        .unwrap()
        .unwrap();
        assert_eq!(linked.0.map(|(linked, _)| linked), Some(account.clone()));
        assert!(linked.1.is_none(), "links work once");
        let (_, state) = owner.send("GET", &path, None).await;
        assert_eq!(state["alerts"]["telegram"]["name"], "@ada");

        // New alert choices are validated and saved.
        let settings = |alerts: Value| {
            json!({
                "min_payout": null, "default_solo": false, "hide_workers": false, "alerts": alerts
            })
        };
        let (status, _) = owner
            .send(
                "PUT",
                &path,
                Some(settings(
                    json!({"offline_minutes": 10, "notify_payouts": true,
                "notify_solo_blocks": false, "hashrate_drop_minutes": 45}),
                )),
            )
            .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        let (status, state) = owner
            .send(
                "PUT",
                &path,
                Some(settings(
                    json!({"offline_minutes": 10, "notify_payouts": true,
                "notify_solo_blocks": false, "hashrate_drop_minutes": 60, "weekly_summary": true}),
                )),
            )
            .await;
        assert_eq!(status, StatusCode::OK, "{state}");
        assert_eq!(state["alerts"]["hashrate_drop_minutes"], 60);
        assert_eq!(state["alerts"]["weekly_summary"], true);
        assert_eq!(state["alerts"]["notify_updates"], false);

        // Without an email, alerts go to Discord and Telegram.
        let context = WorkerContext {
            store: Arc::clone(&store),
            service: Arc::clone(&alerts),
            pool_fee_pct: 0.0,
            confirmations: 60,
            pool_up_since: parking_lot::Mutex::new(Some(now - Duration::from_secs(3_600))),
            miner_release: pool_runtime::miner_release::MinerRelease::new("0.3.1"),
        };
        chat.sent.lock().clear();
        run_cycle(&context, now).await.unwrap();
        let telegram_to = format!("telegram:{chat_id}");
        let sent: Vec<(String, String)> = std::mem::take(&mut *chat.sent.lock())
            .into_iter()
            .filter(|(to, _)| to == webhook || to == &telegram_to)
            .collect();
        assert_eq!(sent.len(), 2, "{sent:?}");
        assert!(sent
            .iter()
            .all(|(_, text)| text.starts_with("rig-1 stopped mining\n")));

        // A deleted webhook is dropped; Telegram keeps working.
        chat.gone.lock().push(webhook.to_string());
        let seed = Arc::clone(&store);
        let back = share("rig-1", now, base + 3);
        tokio::task::spawn_blocking(move || seed.add_share(back).unwrap())
            .await
            .unwrap();
        run_cycle(&context, now + Duration::from_secs(60))
            .await
            .unwrap();
        let sent: Vec<(String, String)> = std::mem::take(&mut *chat.sent.lock())
            .into_iter()
            .filter(|(to, _)| to == &telegram_to)
            .collect();
        assert_eq!(sent.len(), 1, "{sent:?}");
        assert!(sent[0].1.starts_with("rig-1 is mining again"));
        let (_, state) = owner.send("GET", &path, None).await;
        assert_eq!(state["alerts"]["discord"], Value::Null);

        // /stop in Telegram leaves nowhere to send to.
        let stop_store = Arc::clone(&store);
        tokio::task::spawn_blocking(move || stop_store.clear_miner_alert_telegram_chat(chat_id))
            .await
            .unwrap()
            .unwrap();
        let (_, state) = owner.send("GET", &path, None).await;
        assert_eq!(state["alerts"], Value::Null);

        // Private stats hide everything from visitors, not from the owner.
        let owner_headers = {
            let mut headers = HeaderMap::new();
            headers.insert(
                header::COOKIE,
                HeaderValue::from_str(owner.cookie.as_ref().unwrap()).unwrap(),
            );
            headers
        };
        let (status, state) = owner
            .send(
                "PUT",
                &path,
                Some(json!({"min_payout": null, "default_solo": false,
                "hide_workers": false, "private_stats": true})),
            )
            .await;
        assert_eq!(status, StatusCode::OK, "{state}");
        assert_eq!(state["settings"]["private_stats"], true);
        assert_eq!(
            service.public_view_for(&account, &HeaderMap::new()).await,
            PublicView::Private
        );
        assert_eq!(
            service.public_view_for(&account, &owner_headers).await,
            PublicView::Full
        );
        // An older page that does not send it keeps the choice.
        let (_, state) = owner
            .send(
                "PUT",
                &path,
                Some(json!({"min_payout": null, "default_solo": false, "hide_workers": false})),
            )
            .await;
        assert_eq!(state["settings"]["private_stats"], true);

        // Starting difficulty: within the era's range, or back to the pool's pick.
        let min = state["difficulty"]["min"].as_u64().unwrap();
        let difficulty = |value: Value| {
            json!({"min_payout": null, "default_solo": false,
            "hide_workers": false, "start_difficulty": value})
        };
        let (status, _) = owner
            .send("PUT", &path, Some(difficulty(json!(min - 1))))
            .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        let (status, state) = owner
            .send("PUT", &path, Some(difficulty(json!(min * 2))))
            .await;
        assert_eq!(status, StatusCode::OK, "{state}");
        assert_eq!(state["settings"]["start_difficulty"], min * 2);
        let (_, state) = owner
            .send(
                "PUT",
                &path,
                Some(json!({"min_payout": null, "default_solo": false, "hide_workers": false})),
            )
            .await;
        assert_eq!(
            state["settings"]["start_difficulty"],
            min * 2,
            "kept when not sent"
        );
        let (_, state) = owner
            .send("PUT", &path, Some(difficulty(Value::Null)))
            .await;
        assert_eq!(state["settings"]["start_difficulty"], Value::Null);
    }

    #[test]
    fn passwords_need_eight_characters() {
        assert!(password_ok("1234567").is_err());
        assert!(password_ok("12345678").is_ok());
        assert!(password_ok(&"x".repeat(257)).is_err());
    }
}
