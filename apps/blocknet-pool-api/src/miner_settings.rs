//! My Stats settings endpoints (rules in `pool_runtime::miner_settings`).
//!
//! An unlocked browser holds a random session token in an HttpOnly,
//! SameSite=Strict cookie; sessions live in memory, end after
//! [`SESSION_IDLE`] without use or when the browser closes, and stop working
//! once the account's password or owner changes. Every write is JSON, so a
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
    effective_min_payout, max_min_payout, take_back_decision, validate_min_payout, MinerSettings,
    MinerSettingsRecord, TakeBack, ATOMIC_PER_BNT, TAKE_BACK_WINDOW,
};
use pool_runtime::store::PoolStore;
use serde::Deserialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tokio::sync::Semaphore;

use crate::protect::{client_key, ClientIpPolicy};

pub(crate) const SESSION_COOKIE: &str = "pool_settings";
const SESSION_IDLE: Duration = Duration::from_secs(30 * 60);
const MAX_SESSIONS: usize = 50_000;
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

struct Session {
    /// Account → (owner stamp at unlock, last use).
    accounts: HashMap<String, (String, Instant)>,
    last_used: Instant,
}

#[derive(Default)]
struct Sessions {
    by_token_hash: HashMap<String, Session>,
}

impl Sessions {
    fn prune(&mut self, now: Instant) {
        self.by_token_hash.retain(|_, session| {
            session
                .accounts
                .retain(|_, (_, used)| now.saturating_duration_since(*used) < SESSION_IDLE);
            !session.accounts.is_empty()
                && now.saturating_duration_since(session.last_used) < SESSION_IDLE
        });
    }
}

struct Limits {
    unlock_by_account: AttemptLimiter,
    unlock_by_client: AttemptLimiter,
    take_back_by_account: AttemptLimiter,
    take_back_by_client: AttemptLimiter,
    password_writes_by_client: AttemptLimiter,
}

pub(crate) struct MinerSettingsService {
    store: Arc<PoolStore>,
    runtime: RuntimeConfig,
    client_ip: Arc<ClientIpPolicy>,
    sessions: Mutex<Sessions>,
    limits: Mutex<Limits>,
    hashing: Semaphore,
    hide_workers: Mutex<HashMap<String, (Instant, bool)>>,
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
    // No Max-Age: the browser drops it when it closes.
    HeaderValue::from_str(&format!(
        "{SESSION_COOKIE}={token}; Path=/api; HttpOnly; Secure; SameSite=Strict"
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

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SettingsRequest {
    /// BNT; `null` keeps the pool's minimum.
    min_payout: Option<f64>,
    default_solo: bool,
    hide_workers: bool,
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
    ) -> Self {
        Self {
            store,
            runtime,
            client_ip,
            sessions: Mutex::new(Sessions::default()),
            limits: Mutex::new(Limits {
                unlock_by_account: AttemptLimiter::new(),
                unlock_by_client: AttemptLimiter::new(),
                take_back_by_account: AttemptLimiter::new(),
                take_back_by_client: AttemptLimiter::new(),
                password_writes_by_client: AttemptLimiter::new(),
            }),
            hashing: Semaphore::new(MAX_CONCURRENT_HASHES),
            hide_workers: Mutex::new(HashMap::new()),
        }
    }

    fn pool_minimum(&self, account: &str) -> u64 {
        (self.runtime.payout_minimum(account) * ATOMIC_PER_BNT as f64).round() as u64
    }

    /// Highest minimum payout the account may choose: what one payout may send.
    fn max_min_payout(&self, account: &str) -> u64 {
        let atomic = |bnt: f64| (bnt.max(0.0) * ATOMIC_PER_BNT as f64).round() as u64;
        max_min_payout(
            self.pool_minimum(account),
            atomic(self.runtime.payout_max_per_recipient),
            atomic(self.runtime.payout_max_total_per_tick),
        )
    }

    /// The minimum payout that applies to an account, in BNT.
    pub(crate) fn effective_min_payout_coins(&self, account: &str, chosen: Option<u64>) -> f64 {
        coins(effective_min_payout(self.pool_minimum(account), chosen))
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
    fn unlocked(&self, caller: &Caller, record: &MinerSettingsRecord) -> bool {
        let Some(token) = &caller.token else {
            return false;
        };
        let now = Instant::now();
        let stamp = owner_stamp(record);
        let mut sessions = self.sessions.lock();
        let Some(session) = sessions.by_token_hash.get_mut(&token_hash(token)) else {
            return false;
        };
        let Some((unlocked_stamp, used)) = session.accounts.get_mut(&caller.account) else {
            return false;
        };
        if *unlocked_stamp != stamp || now.saturating_duration_since(*used) >= SESSION_IDLE {
            session.accounts.remove(&caller.account);
            return false;
        }
        *used = now;
        session.last_used = now;
        true
    }

    /// Unlocks the account in the caller's session, creating one if needed.
    /// Returns a new token to set as the cookie, if one was created.
    fn unlock_session(&self, caller: &Caller, record: &MinerSettingsRecord) -> Option<String> {
        let now = Instant::now();
        let stamp = owner_stamp(record);
        let mut sessions = self.sessions.lock();
        if let Some(token) = &caller.token {
            if let Some(session) = sessions.by_token_hash.get_mut(&token_hash(token)) {
                if session.accounts.len() >= MAX_ACCOUNTS_PER_SESSION
                    && !session.accounts.contains_key(&caller.account)
                {
                    if let Some(oldest) = session
                        .accounts
                        .iter()
                        .min_by_key(|(_, (_, used))| *used)
                        .map(|(account, _)| account.clone())
                    {
                        session.accounts.remove(&oldest);
                    }
                }
                session
                    .accounts
                    .insert(caller.account.clone(), (stamp, now));
                session.last_used = now;
                return None;
            }
        }
        if sessions.by_token_hash.len() >= MAX_SESSIONS {
            sessions.prune(now);
            if sessions.by_token_hash.len() >= MAX_SESSIONS {
                // Drop the least recently used session.
                if let Some(oldest) = sessions
                    .by_token_hash
                    .iter()
                    .min_by_key(|(_, session)| session.last_used)
                    .map(|(hash, _)| hash.clone())
                {
                    sessions.by_token_hash.remove(&oldest);
                }
            }
        }
        let token = hex::encode(rand::random::<[u8; 32]>());
        sessions.by_token_hash.insert(
            token_hash(&token),
            Session {
                accounts: HashMap::from([(caller.account.clone(), (stamp, now))]),
                last_used: now,
            },
        );
        Some(token)
    }

    fn forget_hide_cache(&self, account: &str) {
        self.hide_workers.lock().remove(account);
    }

    /// Whether public miner stats for `account` should show "Rig N" instead
    /// of worker names for this request.
    pub(crate) async fn hides_workers_for(&self, account: &str, headers: &HeaderMap) -> bool {
        let now = Instant::now();
        let cached = self
            .hide_workers
            .lock()
            .get(account)
            .filter(|(at, _)| now.saturating_duration_since(*at) < HIDE_CACHE_TTL)
            .map(|(_, hide)| *hide);
        let hide = match cached {
            Some(hide) => hide,
            None => {
                let hide = match self.record(account).await {
                    Ok(record) => record.is_some_and(|record| record.settings.hide_workers),
                    // Fail closed: hiding names is the safer mistake.
                    Err(_) => true,
                };
                let mut cache = self.hide_workers.lock();
                if cache.len() >= HIDE_CACHE_MAX_ENTRIES {
                    cache.clear();
                }
                cache.insert(account.to_string(), (now, hide));
                hide
            }
        };
        if !hide {
            return false;
        }
        // The owner sees real names while unlocked; visitors without a
        // session cost no further read.
        let Some(token) = session_token(headers) else {
            return true;
        };
        let caller = Caller {
            account: account.to_string(),
            client: None,
            token: Some(token),
        };
        match self.record(account).await {
            Ok(Some(record)) => !self.unlocked(&caller, &record),
            _ => true,
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
    ) -> Value {
        let pool_minimum = self.pool_minimum(&caller.account);
        let mut body = json!({
            "account": caller.account,
            "claimed": record.is_some(),
            "has_password": record.is_some_and(|record| record.password_hash.is_some()),
            "unlocked": unlocked,
            "changed_at": record.map(|record| to_unix(record.changed_at)),
            "pool_min_payout": coins(pool_minimum),
            "max_min_payout": coins(self.max_min_payout(&caller.account)),
            "your_ip": caller.client,
        });
        if let (true, Some(record)) = (unlocked, record) {
            body["settings"] = json!({
                "min_payout": coins(effective_min_payout(pool_minimum, record.settings.min_payout)),
                "default_solo": record.settings.default_solo,
                "hide_workers": record.settings.hide_workers,
            });
        }
        body
    }

    fn respond(
        &self,
        caller: &Caller,
        record: Option<&MinerSettingsRecord>,
        unlocked: bool,
        new_token: Option<String>,
    ) -> Response {
        respond_body(self.state_body(caller, record, unlocked), new_token)
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
        let unlocked = record
            .as_ref()
            .is_some_and(|record| self.unlocked(&caller, record));
        Ok(self.respond(&caller, record.as_ref(), unlocked, None))
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
        let token = self.unlock_session(&caller, &record);
        Ok(self.respond(&caller, Some(&record), true, token))
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
        let token = self.unlock_session(&caller, &record);
        Ok(self.respond(&caller, Some(&record), true, token))
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
                let token = self.unlock_session(&caller, &record);
                let mut body = self.state_body(&caller, Some(&record), true);
                body["share"] = json!(share);
                Ok(respond_body(body, token))
            }
        }
    }

    /// An unlocked caller's record, or a 401.
    async fn require_unlocked(&self, caller: &Caller) -> Result<MinerSettingsRecord, ApiError> {
        let locked = || ApiError::new(StatusCode::UNAUTHORIZED, "locked", "Unlock settings first.");
        let record = self.record(&caller.account).await?.ok_or_else(locked)?;
        if self.unlocked(caller, &record) {
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
        let token = self.unlock_session(&caller, &record);
        Ok(self.respond(&caller, Some(&record), true, token))
    }

    async fn save(&self, caller: Caller, request: SettingsRequest) -> ApiResult {
        let record = self.require_unlocked(&caller).await?;
        let pool_minimum = self.pool_minimum(&caller.account);
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
                let atomic = validate_min_payout(atomic as u64, pool_minimum, maximum).map_err(
                    |message| ApiError::new(StatusCode::BAD_REQUEST, "invalid_settings", &message),
                )?;
                // The pool's own minimum is stored as "no choice", so it
                // follows the pool if that changes.
                (atomic != pool_minimum).then_some(atomic)
            }
            Some(_) => {
                return Err(ApiError::new(
                    StatusCode::BAD_REQUEST,
                    "invalid_settings",
                    "Enter a minimum payout in BNT.",
                ))
            }
        };
        let settings = MinerSettings {
            min_payout,
            default_solo: request.default_solo,
            hide_workers: request.hide_workers,
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
                "miner settings saved"
            );
        }
        let record = self
            .record(&caller.account)
            .await?
            .ok_or_else(|| ApiError::internal(anyhow::anyhow!("settings row missing")))?;
        Ok(self.respond(&caller, Some(&record), true, None))
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
            "/api/miner-settings/:account/take-back",
            post(handle_take_back),
        )
        .route(
            "/api/miner-settings/:account/password",
            post(handle_password),
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
        assert!(!cookie.contains("Max-Age"));
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
        assert!(service.hides_workers_for(&account, &HeaderMap::new()).await);
        assert!(!service.hides_workers_for(&account, &owner_headers).await);

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

    #[test]
    fn passwords_need_eight_characters() {
        assert!(password_ok("1234567").is_err());
        assert!(password_ok("12345678").is_ok());
        assert!(password_ok(&"x".repeat(257)).is_err());
    }
}
