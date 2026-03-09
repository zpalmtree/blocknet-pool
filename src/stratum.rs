use std::collections::{HashMap, VecDeque};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Result};
use parking_lot::Mutex;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, mpsc, Notify};

use crate::dev_fee::should_defer_submit_ack_difficulty;
use crate::engine::{canonical_share_reject_reason, PoolEngine};
use crate::jobs::JobManager;
use crate::protocol::{
    normalize_worker_name, LoginParams, StratumNotify, StratumRequest, StratumResponse,
    SubmitParams, METHOD_LOGIN, METHOD_NOTIFICATION, METHOD_SUBMIT, NOTIFY_MINER_BLOCK_FOUND,
    NOTIFY_POOL_BLOCK_SOLVED,
};
use crate::stats::PoolStats;

const MAX_CONNS_PER_IP: usize = 16;
const MAX_CONNS_TOTAL: usize = 4096;
const LOGIN_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_STRATUM_REQUEST_BYTES: usize = 8 * 1024;
const NOTIFICATION_CHANNEL_CAPACITY: usize = 256;
const OUTBOUND_QUEUE_CAPACITY: usize = 256;
const OUTBOUND_WRITE_TIMEOUT: Duration = Duration::from_secs(2);

fn client_submit_ack_difficulty(
    address: &str,
    current_difficulty: u64,
    next_difficulty: u64,
) -> u64 {
    if should_defer_submit_ack_difficulty(address) {
        current_difficulty.max(1)
    } else {
        next_difficulty.max(1)
    }
}

#[derive(Debug)]
struct ConnState {
    counts: HashMap<String, usize>,
    total: usize,
}

#[derive(Clone)]
struct OutboundHandle {
    normal_tx: mpsc::Sender<Vec<u8>>,
    latest_job: Arc<Mutex<Option<Vec<u8>>>>,
    latest_job_notify: Arc<Notify>,
}

impl OutboundHandle {
    fn new(capacity: usize) -> (Self, mpsc::Receiver<Vec<u8>>) {
        let (normal_tx, normal_rx) = mpsc::channel(capacity);
        (
            Self {
                normal_tx,
                latest_job: Arc::new(Mutex::new(None)),
                latest_job_notify: Arc::new(Notify::new()),
            },
            normal_rx,
        )
    }
}

pub struct StratumServer {
    listen_addr: SocketAddr,
    engine: Arc<PoolEngine>,
    jobs: Arc<JobManager>,
    stats: Arc<PoolStats>,
    conn_state: Arc<Mutex<ConnState>>,
    notifications: broadcast::Sender<StratumNotify>,
    post_login_idle_timeout: Duration,
    submit_rate_limit_window: Duration,
    submit_rate_limit_max: usize,
}

impl StratumServer {
    pub fn new(
        listen_addr: SocketAddr,
        engine: Arc<PoolEngine>,
        jobs: Arc<JobManager>,
        stats: Arc<PoolStats>,
        post_login_idle_timeout: Duration,
        submit_rate_limit_window: Duration,
        submit_rate_limit_max: usize,
    ) -> Arc<Self> {
        let (notifications, _) = broadcast::channel(NOTIFICATION_CHANNEL_CAPACITY);
        Arc::new(Self {
            listen_addr,
            engine,
            jobs,
            stats,
            conn_state: Arc::new(Mutex::new(ConnState {
                counts: HashMap::new(),
                total: 0,
            })),
            notifications,
            post_login_idle_timeout,
            submit_rate_limit_window,
            submit_rate_limit_max: submit_rate_limit_max.max(1),
        })
    }

    pub async fn run(self: Arc<Self>) -> Result<()> {
        let listener = TcpListener::bind(self.listen_addr).await?;
        tracing::info!(addr = %self.listen_addr, "stratum listening");

        loop {
            let (stream, peer) = listener.accept().await?;
            let ip = peer.ip().to_string();

            if !self.try_track_conn(&ip) {
                tracing::warn!(ip = %ip, "rejecting stratum connection due to limits");
                continue;
            }

            let this = Arc::clone(&self);
            tokio::spawn(async move {
                if let Err(err) = this.handle_conn(stream, peer).await {
                    tracing::warn!(peer = %peer, error = %err, "stratum connection ended with error");
                }
                this.untrack_conn(&ip);
            });
        }
    }

    fn try_track_conn(&self, ip: &str) -> bool {
        let mut state = self.conn_state.lock();
        if state.total >= MAX_CONNS_TOTAL {
            return false;
        }
        let count = state.counts.entry(ip.to_string()).or_default();
        if *count >= MAX_CONNS_PER_IP {
            return false;
        }
        *count += 1;
        state.total += 1;
        true
    }

    fn untrack_conn(&self, ip: &str) {
        let mut state = self.conn_state.lock();
        if let Some(count) = state.counts.get_mut(ip) {
            if *count > 1 {
                *count -= 1;
            } else {
                state.counts.remove(ip);
            }
        }
        if state.total > 0 {
            state.total -= 1;
        }
    }

    async fn handle_conn(&self, stream: TcpStream, peer: SocketAddr) -> Result<()> {
        let conn_id = peer.to_string();
        let (reader_half, writer_half) = stream.into_split();
        let (outbound, outbound_rx) = OutboundHandle::new(OUTBOUND_QUEUE_CAPACITY);
        let latest_job = Arc::clone(&outbound.latest_job);
        let latest_job_notify = Arc::clone(&outbound.latest_job_notify);
        let mut writer_task = tokio::spawn(async move {
            run_outbound_writer(writer_half, outbound_rx, latest_job, latest_job_notify).await
        });
        let mut writer_task_finished = false;

        let mut logged_in: Option<(String, String, u64)> = None; // address, worker, difficulty
        let mut reader = BufReader::new(reader_half);
        let mut rx_jobs = self.jobs.subscribe();
        let mut rx_notifications = self.notifications.subscribe();
        let post_login_idle_timeout = self.post_login_idle_timeout;
        let submit_rate_limit_window = self.submit_rate_limit_window;
        let submit_rate_limit_max = self.submit_rate_limit_max;
        let mut submit_timestamps = VecDeque::<Instant>::new();

        let login_deadline = tokio::time::sleep(LOGIN_TIMEOUT);
        tokio::pin!(login_deadline);
        let idle_deadline = tokio::time::sleep(post_login_idle_timeout);
        tokio::pin!(idle_deadline);

        let mut run_result = Ok(());
        loop {
            tokio::select! {
                writer_result = &mut writer_task => {
                    writer_task_finished = true;
                    run_result = match writer_result {
                        Ok(Ok(())) => Ok(()),
                        Ok(Err(err)) => Err(err),
                        Err(err) => Err(anyhow!("stratum writer task failed: {err}")),
                    };
                    break;
                }
                _ = &mut login_deadline, if logged_in.is_none() => {
                    tracing::warn!(peer = %peer, "stratum login timeout");
                    break;
                }
                _ = &mut idle_deadline, if logged_in.is_some() => {
                    tracing::warn!(peer = %peer, "stratum idle timeout");
                    break;
                }
                maybe_job = rx_jobs.recv(), if logged_in.is_some() => {
                    if let Ok(job) = maybe_job {
                        if let Some((address, worker, difficulty)) = logged_in.as_mut() {
                            let next_difficulty = self
                                .engine
                                .retarget_on_job_if_needed(&conn_id)
                                .unwrap_or(*difficulty);
                            if next_difficulty != *difficulty {
                                *difficulty = next_difficulty;
                                tracing::debug!(
                                    peer = %peer,
                                    address = %address,
                                    worker = %worker,
                                    difficulty = next_difficulty,
                                    "stratum difficulty updated on job tick"
                                );
                            }
                            if let Some(miner_job) =
                                self.jobs.build_miner_job(&conn_id, *difficulty, address)
                            {
                                let notify = StratumNotify {
                                    method: "job".to_string(),
                                    params: serde_json::to_value(miner_job)?,
                                };
                                if let Err(err) = queue_job_json(&outbound, &notify) {
                                    run_result = Err(err);
                                    break;
                                }
                            }
                        }
                        let _ = job;
                    }
                }
                maybe_notification = rx_notifications.recv(), if logged_in.is_some() => {
                    match maybe_notification {
                        Ok(notification) => {
                            if let Err(err) = queue_json(&outbound, &notification) {
                                run_result = Err(err);
                                break;
                            }
                        }
                        Err(broadcast::error::RecvError::Lagged(skipped)) => {
                            tracing::debug!(peer = %peer, skipped, "stratum notification receiver lagged");
                        }
                        Err(broadcast::error::RecvError::Closed) => {
                            break;
                        }
                    }
                }
                line = read_line_limited(&mut reader, MAX_STRATUM_REQUEST_BYTES) => {
                    let line = match line {
                        Ok(Some(v)) => v,
                        Ok(None) => break,
                        Err(err) => {
                            let _ = queue_error(&outbound, 0, &err.to_string());
                            break;
                        }
                    };
                    if logged_in.is_some() {
                        idle_deadline
                            .as_mut()
                            .reset(tokio::time::Instant::now() + post_login_idle_timeout);
                    }
                    let trimmed = line.trim();
                    if trimmed.is_empty() {
                        continue;
                    }

                    let req: StratumRequest = match serde_json::from_str(trimmed) {
                        Ok(v) => v,
                        Err(_) => {
                            if let Err(err) = queue_error(&outbound, 0, "invalid JSON") {
                                run_result = Err(err);
                                break;
                            }
                            continue;
                        }
                    };

                    match req.method.as_str() {
                        METHOD_LOGIN => {
                            let params: LoginParams = match serde_json::from_value(req.params.clone()) {
                                Ok(v) => v,
                                Err(_) => {
                                    if let Err(err) =
                                        queue_error(&outbound, req.id, "invalid login params")
                                    {
                                        run_result = Err(err);
                                        break;
                                    }
                                    continue;
                                }
                            };

                            let login_engine = Arc::clone(&self.engine);
                            let login_conn_id = conn_id.clone();
                            let login_address = params.address.trim().to_string();
                            let login_worker = params.worker.clone();
                            let login_protocol_version = params.protocol_version;
                            let login_capabilities = params.capabilities.clone();
                            let login_hint = params.difficulty_hint;
                            let login = tokio::task::spawn_blocking(move || {
                                login_engine.login_with_hint(
                                    &login_conn_id,
                                    login_address,
                                    Some(login_worker),
                                    login_protocol_version,
                                    login_capabilities,
                                    login_hint,
                                )
                            })
                            .await;

                            match login {
                                Ok(Ok(login_result)) => {
                                    let worker =
                                        normalize_worker_name(Some(params.worker.as_str()));
                                    let address = params.address.trim().to_string();
                                    let difficulty =
                                        self.engine.session_difficulty(&conn_id).unwrap_or(1);

                                    logged_in =
                                        Some((address.clone(), worker.clone(), difficulty));
                                    self.stats.add_miner(&conn_id, &address, &worker);

                                    let response = StratumResponse {
                                        id: req.id,
                                        status: Some("ok".to_string()),
                                        error: None,
                                        result: Some(serde_json::to_value(login_result)?),
                                    };
                                    if let Err(err) = queue_json(&outbound, &response) {
                                        run_result = Err(err);
                                        break;
                                    }

                                    if let Some(miner_job) = self.jobs.build_miner_job(
                                        &conn_id,
                                        difficulty,
                                        &address,
                                    ) {
                                        let notify = StratumNotify {
                                            method: "job".to_string(),
                                            params: serde_json::to_value(miner_job)?,
                                        };
                                        if let Err(err) = queue_json(&outbound, &notify) {
                                            run_result = Err(err);
                                            break;
                                        }
                                    }
                                }
                                Ok(Err(err)) => {
                                    let _ = queue_error(&outbound, req.id, &err.to_string());
                                    break;
                                }
                                Err(err) => {
                                    let _ = queue_error(
                                        &outbound,
                                        req.id,
                                        &format!("login worker failure: {err}"),
                                    );
                                    break;
                                }
                            }
                        }
                        METHOD_SUBMIT => {
                            let params: SubmitParams = match serde_json::from_value(req.params.clone()) {
                                Ok(v) => v,
                                Err(_) => {
                                    if let Err(err) =
                                        queue_error(&outbound, req.id, "invalid submit params")
                                    {
                                        run_result = Err(err);
                                        break;
                                    }
                                    continue;
                                }
                            };
                            let now = Instant::now();
                            let cutoff = now.checked_sub(submit_rate_limit_window).unwrap_or(now);
                            while submit_timestamps
                                .front()
                                .is_some_and(|ts| *ts < cutoff)
                            {
                                submit_timestamps.pop_front();
                            }
                            if submit_timestamps.len() >= submit_rate_limit_max {
                                if let Some((address, _, _)) = logged_in.as_ref() {
                                    self.stats.record_rejected_share(address, "rate limited");
                                }
                                if let Err(err) =
                                    queue_error(&outbound, req.id, "rate limited, retry")
                                {
                                    run_result = Err(err);
                                    break;
                                }
                                continue;
                            }
                            submit_timestamps.push_back(now);
                            let submit_job_id = params.job_id.clone();
                            let submit_nonce = params.nonce;

                            let engine = Arc::clone(&self.engine);
                            let submit_conn_id = conn_id.clone();
                            let received_at = Instant::now();
                            let submit = tokio::task::spawn_blocking(move || {
                                engine.submit_with_received_at(
                                    &submit_conn_id,
                                    params.job_id,
                                    params.nonce,
                                    params.claimed_hash,
                                    received_at,
                                )
                            })
                            .await;

                            match submit {
                                Ok(Ok(ack)) => {
                                    let mut finder_address = None::<String>;
                                    let mut finder_worker = None::<String>;
                                    let mut response_difficulty = ack.next_difficulty;
                                    if let Some((address, worker, difficulty)) = logged_in.as_mut() {
                                        self.stats
                                            .record_accepted_share(address, ack.share_difficulty);
                                        if ack.block_accepted {
                                            self.stats.record_block_found(address);
                                            finder_address = Some(address.clone());
                                            finder_worker = Some(worker.clone());
                                        }
                                        response_difficulty = client_submit_ack_difficulty(
                                            address,
                                            *difficulty,
                                            ack.next_difficulty,
                                        );
                                        if ack.next_difficulty != *difficulty {
                                            if should_defer_submit_ack_difficulty(address) {
                                                tracing::debug!(
                                                    peer = %peer,
                                                    address = %address,
                                                    worker = %worker,
                                                    active_difficulty = *difficulty,
                                                    deferred_difficulty = ack.next_difficulty,
                                                    "stratum deferred difficulty update until next template"
                                                );
                                            } else {
                                                *difficulty = ack.next_difficulty;
                                                if let Some(miner_job) =
                                                    self.jobs
                                                        .build_miner_job(
                                                            &conn_id,
                                                            ack.next_difficulty,
                                                            address,
                                                        )
                                                {
                                                    let notify = StratumNotify {
                                                        method: "job".to_string(),
                                                        params: serde_json::to_value(miner_job)?,
                                                    };
                                                    if let Err(err) = queue_job_json(&outbound, &notify) {
                                                        run_result = Err(err);
                                                        break;
                                                    }
                                                }
                                                tracing::debug!(
                                                    peer = %peer,
                                                    address = %address,
                                                    worker = %worker,
                                                    difficulty = ack.next_difficulty,
                                                    "stratum difficulty updated"
                                                );
                                            }
                                        }
                                    }
                                    let response = StratumResponse {
                                        id: req.id,
                                        status: Some("ok".to_string()),
                                        error: None,
                                        result: Some(serde_json::json!({
                                            "accepted": ack.accepted,
                                            "verified": ack.verified,
                                            "status": ack.status,
                                            "difficulty": response_difficulty,
                                        })),
                                    };
                                    if let Err(err) = queue_json(&outbound, &response) {
                                        run_result = Err(err);
                                        break;
                                    }
                                    if ack.block_accepted {
                                        let miner_notification = block_notification(
                                            NOTIFY_MINER_BLOCK_FOUND,
                                            "great success: you found a block for the pool",
                                        );
                                        if let Err(err) = queue_json(&outbound, &miner_notification) {
                                            run_result = Err(err);
                                            break;
                                        }

                                        let pool_notification = block_notification(
                                            NOTIFY_POOL_BLOCK_SOLVED,
                                            "pool solved a block: share rewards are now pending confirmation",
                                        );
                                        let _ = self.notifications.send(pool_notification);
                                        tracing::info!(
                                            peer = %peer,
                                            finder = finder_address.unwrap_or_default(),
                                            worker = finder_worker.unwrap_or_default(),
                                            "broadcasted pool block solved notification"
                                        );
                                    }
                                }
                                Ok(Err(err)) => {
                                    let err_text = err.to_string();
                                    let reason_code = canonical_share_reject_reason(&err_text);
                                    if let Some((address, _, _)) = logged_in.as_ref() {
                                        self.stats
                                            .record_rejected_share(address, reason_code);
                                        if log_rejection_at_info(reason_code) {
                                            tracing::info!(
                                                peer = %peer,
                                                address = %address,
                                                job_id = %submit_job_id,
                                                nonce = submit_nonce,
                                                reason_code,
                                                error = %err_text,
                                                "share rejected"
                                            );
                                        } else {
                                            tracing::debug!(
                                                peer = %peer,
                                                address = %address,
                                                job_id = %submit_job_id,
                                                nonce = submit_nonce,
                                                reason_code,
                                                error = %err_text,
                                                "share rejected"
                                            );
                                        }
                                    }
                                    if let Err(err) = queue_error(&outbound, req.id, &err_text) {
                                        run_result = Err(err);
                                        break;
                                    }
                                }
                                Err(err) => {
                                    if let Some((address, _, _)) = logged_in.as_ref() {
                                        self.stats
                                            .record_rejected_share(address, "submit worker failure");
                                        tracing::warn!(
                                            peer = %peer,
                                            address = %address,
                                            job_id = %submit_job_id,
                                            nonce = submit_nonce,
                                            error = %err,
                                            "submit worker failure"
                                        );
                                    }
                                    if let Err(err) = queue_error(
                                        &outbound,
                                        req.id,
                                        &format!("submit worker failure: {err}"),
                                    ) {
                                        run_result = Err(err);
                                        break;
                                    }
                                }
                            }
                        }
                        _ => {
                            if let Err(err) = queue_error(&outbound, req.id, "unknown method") {
                                run_result = Err(err);
                                break;
                            }
                        }
                    }
                }
            }
        }

        drop(outbound);
        if !writer_task_finished {
            let writer_result = writer_task.await;
            if run_result.is_ok() {
                run_result = match writer_result {
                    Ok(Ok(())) => Ok(()),
                    Ok(Err(err)) => Err(err),
                    Err(err) => Err(anyhow!("stratum writer task failed: {err}")),
                };
            }
        }

        if let Some((address, _, _)) = logged_in.take() {
            let engine = Arc::clone(&self.engine);
            let disconnect_conn_id = conn_id.clone();
            let _ = tokio::task::spawn_blocking(move || {
                engine.disconnect(&disconnect_conn_id);
            })
            .await;
            self.stats.remove_miner(&conn_id);
            tracing::debug!(peer = %peer, address = %address, "stratum miner disconnected");
        }

        run_result
    }
}

fn queue_error(outbound: &OutboundHandle, id: u64, msg: &str) -> Result<()> {
    let response = StratumResponse {
        id,
        status: None,
        error: Some(msg.to_string()),
        result: None,
    };
    queue_json(outbound, &response)
}

async fn read_line_limited(
    reader: &mut BufReader<tokio::net::tcp::OwnedReadHalf>,
    max_bytes: usize,
) -> Result<Option<String>> {
    let mut data = Vec::<u8>::with_capacity(256);
    loop {
        let byte = match reader.read_u8().await {
            Ok(v) => v,
            Err(err) if err.kind() == std::io::ErrorKind::UnexpectedEof => {
                if data.is_empty() {
                    return Ok(None);
                }
                break;
            }
            Err(err) => return Err(err.into()),
        };

        if byte == b'\n' {
            break;
        }
        if data.len() >= max_bytes {
            return Err(anyhow!("request exceeds {max_bytes} bytes"));
        }
        if byte != b'\r' {
            data.push(byte);
        }
    }

    String::from_utf8(data)
        .map(Some)
        .map_err(|_| anyhow!("request is not valid UTF-8"))
}

fn queue_json<T: serde::Serialize>(outbound: &OutboundHandle, value: &T) -> Result<()> {
    let payload = serialize_json_line(value)?;
    outbound
        .normal_tx
        .try_send(payload)
        .map_err(|err| anyhow!("stratum outbound queue saturated: {err}"))
}

fn queue_job_json<T: serde::Serialize>(outbound: &OutboundHandle, value: &T) -> Result<()> {
    let payload = serialize_json_line(value)?;
    *outbound.latest_job.lock() = Some(payload);
    outbound.latest_job_notify.notify_one();
    Ok(())
}

fn serialize_json_line<T: serde::Serialize>(value: &T) -> Result<Vec<u8>> {
    let mut data = serde_json::to_vec(value)?;
    data.push(b'\n');
    Ok(data)
}

async fn run_outbound_writer<W>(
    mut writer: W,
    mut rx: mpsc::Receiver<Vec<u8>>,
    latest_job: Arc<Mutex<Option<Vec<u8>>>>,
    latest_job_notify: Arc<Notify>,
) -> Result<()>
where
    W: tokio::io::AsyncWrite + Unpin,
{
    loop {
        if let Some(data) = take_priority_job(&latest_job) {
            write_outbound_payload(&mut writer, &data).await?;
            continue;
        }

        tokio::select! {
            biased;
            _ = latest_job_notify.notified() => {
                continue;
            }
            maybe = rx.recv() => {
                let Some(data) = maybe else {
                    if let Some(data) = take_priority_job(&latest_job) {
                        write_outbound_payload(&mut writer, &data).await?;
                        continue;
                    }
                    break;
                };
                write_outbound_payload(&mut writer, &data).await?;
            }
        }
    }
    Ok(())
}

fn take_priority_job(latest_job: &Mutex<Option<Vec<u8>>>) -> Option<Vec<u8>> {
    latest_job.lock().take()
}

async fn write_outbound_payload<W>(writer: &mut W, data: &[u8]) -> Result<()>
where
    W: tokio::io::AsyncWrite + Unpin,
{
    match tokio::time::timeout(OUTBOUND_WRITE_TIMEOUT, writer.write_all(data)).await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(err)) => Err(err.into()),
        Err(_) => Err(anyhow!(
            "stratum outbound write timed out after {}ms",
            OUTBOUND_WRITE_TIMEOUT.as_millis()
        )),
    }
}

fn block_notification(kind: &str, message: &str) -> StratumNotify {
    StratumNotify {
        method: METHOD_NOTIFICATION.to_string(),
        params: serde_json::json!({
            "kind": kind,
            "message": message,
        }),
    }
}

fn log_rejection_at_info(reason_code: &str) -> bool {
    matches!(
        reason_code,
        "stale job"
            | "duplicate share"
            | "nonce out of assigned range"
            | "job not assigned"
            | "rate limited"
    )
}

#[cfg(test)]
mod tests {
    use super::{
        client_submit_ack_difficulty, log_rejection_at_info, queue_job_json, queue_json,
        run_outbound_writer, OutboundHandle, StratumResponse,
    };
    use crate::engine::canonical_share_reject_reason;
    use crate::protocol::StratumNotify;
    use serde_json::Value;
    use std::sync::Arc;
    use tokio::io::{duplex, AsyncReadExt};

    #[test]
    fn stale_and_duplicate_share_reasons_are_classified() {
        assert_eq!(
            canonical_share_reject_reason("stale job: assignment not found"),
            "stale job"
        );
        assert_eq!(
            canonical_share_reject_reason("duplicate share"),
            "duplicate share"
        );
        assert_eq!(
            canonical_share_reject_reason("nonce out of assigned range"),
            "nonce out of assigned range"
        );
        assert_eq!(
            canonical_share_reject_reason("claimed hash required"),
            "claimed hash required"
        );
        assert_eq!(
            canonical_share_reject_reason("invalid hex"),
            "invalid claimed hash"
        );
        assert_eq!(
            canonical_share_reject_reason("rate limited, retry"),
            "rate limited"
        );
    }

    #[test]
    fn only_high_signal_rejections_are_logged_at_info() {
        assert!(log_rejection_at_info("stale job"));
        assert!(log_rejection_at_info("duplicate share"));
        assert!(log_rejection_at_info("rate limited"));
        assert!(!log_rejection_at_info("claimed hash required"));
        assert!(!log_rejection_at_info("other"));
    }

    #[test]
    fn dev_fee_submit_ack_keeps_client_on_current_difficulty_until_next_template() {
        assert_eq!(
            client_submit_ack_difficulty(crate::dev_fee::SEINE_DEV_FEE_ADDRESS, 60, 240),
            60
        );
    }

    #[test]
    fn regular_submit_ack_applies_next_difficulty_immediately() {
        assert_eq!(client_submit_ack_difficulty("addr1", 60, 240), 240);
    }

    #[test]
    fn queue_json_rejects_when_outbound_queue_is_full() {
        let (outbound, _rx) = OutboundHandle::new(1);
        outbound
            .normal_tx
            .try_send(b"occupied\n".to_vec())
            .expect("seeded payload should fit");

        let err = queue_json(
            &outbound,
            &StratumResponse {
                id: 1,
                status: Some("ok".to_string()),
                error: None,
                result: None,
            },
        )
        .expect_err("full outbound queue should fail fast");

        assert!(err.to_string().contains("outbound queue saturated"));
    }

    #[tokio::test]
    async fn outbound_writer_times_out_when_peer_stops_reading() {
        let (writer, _reader) = duplex(1);
        let (outbound, rx) = OutboundHandle::new(1);
        let latest_job = Arc::clone(&outbound.latest_job);
        let latest_job_notify = Arc::clone(&outbound.latest_job_notify);
        outbound
            .normal_tx
            .send(vec![b'a'; 64])
            .await
            .expect("seed payload should enqueue");
        drop(outbound);

        let err = run_outbound_writer(writer, rx, latest_job, latest_job_notify)
            .await
            .expect_err("writer should time out when peer stops reading");

        assert!(err.to_string().contains("timed out"));
    }

    #[tokio::test]
    async fn outbound_writer_prioritizes_latest_job_and_coalesces_stale_jobs() {
        let (writer, mut reader) = duplex(4096);
        let (outbound, rx) = OutboundHandle::new(8);
        let latest_job = Arc::clone(&outbound.latest_job);
        let latest_job_notify = Arc::clone(&outbound.latest_job_notify);

        queue_json(
            &outbound,
            &StratumResponse {
                id: 7,
                status: Some("ok".to_string()),
                error: None,
                result: Some(serde_json::json!({"accepted": true})),
            },
        )
        .expect("response should enqueue");
        queue_job_json(
            &outbound,
            &StratumNotify {
                method: "job".to_string(),
                params: serde_json::json!({"job_id": "old"}),
            },
        )
        .expect("old job should enqueue");
        queue_job_json(
            &outbound,
            &StratumNotify {
                method: "job".to_string(),
                params: serde_json::json!({"job_id": "new"}),
            },
        )
        .expect("new job should replace the stale one");
        drop(outbound);

        run_outbound_writer(writer, rx, latest_job, latest_job_notify)
            .await
            .expect("writer should flush queued payloads");

        let mut out = Vec::new();
        reader
            .read_to_end(&mut out)
            .await
            .expect("reader should collect writer output");
        let lines = String::from_utf8(out).expect("output should be utf8");
        let messages = lines
            .lines()
            .map(|line| serde_json::from_str::<Value>(line).expect("line should decode as json"))
            .collect::<Vec<_>>();

        assert_eq!(messages.len(), 2, "stale job should be coalesced away");
        assert_eq!(messages[0]["method"], "job");
        assert_eq!(messages[0]["params"]["job_id"], "new");
        assert_eq!(messages[1]["id"], 7);
    }
}
