use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Result};
use parking_lot::Mutex;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex as AsyncMutex;

use crate::engine::PoolEngine;
use crate::jobs::JobManager;
use crate::protocol::{
    normalize_worker_name, LoginParams, StratumNotify, StratumRequest, StratumResponse,
    SubmitParams, METHOD_LOGIN, METHOD_SUBMIT,
};
use crate::stats::PoolStats;

const MAX_CONNS_PER_IP: usize = 16;
const MAX_CONNS_TOTAL: usize = 4096;
const LOGIN_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_STRATUM_REQUEST_BYTES: usize = 8 * 1024;

#[derive(Debug)]
struct ConnState {
    counts: HashMap<String, usize>,
    total: usize,
}

pub struct StratumServer {
    listen_addr: SocketAddr,
    engine: Arc<PoolEngine>,
    jobs: Arc<JobManager>,
    stats: Arc<PoolStats>,
    conn_state: Arc<Mutex<ConnState>>,
    post_login_idle_timeout: Duration,
}

impl StratumServer {
    pub fn new(
        listen_addr: SocketAddr,
        engine: Arc<PoolEngine>,
        jobs: Arc<JobManager>,
        stats: Arc<PoolStats>,
        post_login_idle_timeout: Duration,
    ) -> Arc<Self> {
        Arc::new(Self {
            listen_addr,
            engine,
            jobs,
            stats,
            conn_state: Arc::new(Mutex::new(ConnState {
                counts: HashMap::new(),
                total: 0,
            })),
            post_login_idle_timeout,
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
        let writer = Arc::new(AsyncMutex::new(writer_half));

        let mut logged_in: Option<(String, String, u64)> = None; // address, worker, difficulty
        let mut reader = BufReader::new(reader_half);
        let mut rx_jobs = self.jobs.subscribe();
        let post_login_idle_timeout = self.post_login_idle_timeout;

        let login_deadline = tokio::time::sleep(LOGIN_TIMEOUT);
        tokio::pin!(login_deadline);
        let idle_deadline = tokio::time::sleep(post_login_idle_timeout);
        tokio::pin!(idle_deadline);

        let run_result: Result<()> = async {
            loop {
                tokio::select! {
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
                                if let Some(miner_job) = self.jobs.build_miner_job(*difficulty, address) {
                                    let notify = StratumNotify {
                                        method: "job".to_string(),
                                        params: serde_json::to_value(miner_job)?,
                                    };
                                    send_json(&writer, &notify).await?;
                                }
                            }
                            let _ = job;
                        }
                    }
                    line = read_line_limited(&mut reader, MAX_STRATUM_REQUEST_BYTES) => {
                        let line = match line {
                            Ok(Some(v)) => v,
                            Ok(None) => break,
                            Err(err) => {
                                send_error(&writer, 0, &err.to_string()).await?;
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
                                send_error(&writer, 0, "invalid JSON").await?;
                                continue;
                            }
                        };

                        match req.method.as_str() {
                            METHOD_LOGIN => {
                                let params: LoginParams = match serde_json::from_value(req.params.clone()) {
                                    Ok(v) => v,
                                    Err(_) => {
                                        send_error(&writer, req.id, "invalid login params").await?;
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
                                        send_json(&writer, &response).await?;

                                        if let Some(miner_job) = self.jobs.build_miner_job(
                                            difficulty,
                                            &address,
                                        ) {
                                            let notify = StratumNotify {
                                                method: "job".to_string(),
                                                params: serde_json::to_value(miner_job)?,
                                            };
                                            send_json(&writer, &notify).await?;
                                        }
                                    }
                                    Ok(Err(err)) => {
                                        send_error(&writer, req.id, &err.to_string()).await?;
                                        break;
                                    }
                                    Err(err) => {
                                        send_error(
                                            &writer,
                                            req.id,
                                            &format!("login worker failure: {err}"),
                                        )
                                        .await?;
                                        break;
                                    }
                                }
                            }
                            METHOD_SUBMIT => {
                                let params: SubmitParams = match serde_json::from_value(req.params.clone()) {
                                    Ok(v) => v,
                                    Err(_) => {
                                        send_error(&writer, req.id, "invalid submit params").await?;
                                        continue;
                                    }
                                };
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
                                        let response = StratumResponse {
                                            id: req.id,
                                            status: Some("ok".to_string()),
                                            error: None,
                                            result: Some(serde_json::json!({
                                                "accepted": ack.accepted,
                                                "verified": ack.verified,
                                                "status": ack.status,
                                                "difficulty": ack.next_difficulty,
                                            })),
                                        };
                                        if let Some((address, worker, difficulty)) = logged_in.as_mut() {
                                            self.stats
                                                .record_accepted_share(address, ack.share_difficulty);
                                            if ack.block_accepted {
                                                self.stats.record_block_found(address);
                                            }
                                            if ack.next_difficulty != *difficulty {
                                                *difficulty = ack.next_difficulty;
                                                if let Some(miner_job) =
                                                    self.jobs
                                                        .build_miner_job(ack.next_difficulty, address)
                                                {
                                                    let notify = StratumNotify {
                                                        method: "job".to_string(),
                                                        params: serde_json::to_value(miner_job)?,
                                                    };
                                                    send_json(&writer, &notify).await?;
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
                                        send_json(&writer, &response).await?;
                                    }
                                    Ok(Err(err)) => {
                                        let err_text = err.to_string();
                                        let reason_code = share_reject_reason_code(&err_text);
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
                                        send_error(&writer, req.id, &err_text).await?;
                                    }
                                    Err(err) => {
                                        if let Some((address, _, _)) = logged_in.as_ref() {
                                            self.stats
                                                .record_rejected_share(address, "submit_worker_failure");
                                            tracing::warn!(
                                                peer = %peer,
                                                address = %address,
                                                job_id = %submit_job_id,
                                                nonce = submit_nonce,
                                                error = %err,
                                                "submit worker failure"
                                            );
                                        }
                                        send_error(
                                            &writer,
                                            req.id,
                                            &format!("submit worker failure: {err}"),
                                        )
                                        .await?;
                                    }
                                }
                            }
                            _ => {
                                send_error(&writer, req.id, "unknown method").await?;
                            }
                        }
                    }
                }
            }
            Ok(())
        }
        .await;

        if let Some((address, _, _)) = logged_in.take() {
            self.engine.disconnect(&conn_id);
            self.stats.remove_miner(&conn_id);
            tracing::debug!(peer = %peer, address = %address, "stratum miner disconnected");
        }

        run_result
    }
}

async fn send_error(
    writer: &Arc<AsyncMutex<tokio::net::tcp::OwnedWriteHalf>>,
    id: u64,
    msg: &str,
) -> Result<()> {
    let response = StratumResponse {
        id,
        status: None,
        error: Some(msg.to_string()),
        result: None,
    };
    send_json(writer, &response).await
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

async fn send_json<T: serde::Serialize>(
    writer: &Arc<AsyncMutex<tokio::net::tcp::OwnedWriteHalf>>,
    value: &T,
) -> Result<()> {
    let mut data = serde_json::to_vec(value)?;
    data.push(b'\n');

    let mut guard = writer.lock().await;
    guard.write_all(&data).await?;
    Ok(())
}

fn share_reject_reason_code(error: &str) -> &'static str {
    let trimmed = error.trim();
    if trimmed.starts_with("stale job:") || trimmed == "stale job" {
        return "stale_job";
    }
    if trimmed.starts_with("duplicate share") {
        return "duplicate_share";
    }
    if trimmed.starts_with("nonce out of assigned range") {
        return "nonce_out_of_range";
    }
    if trimmed.starts_with("job not assigned to this miner") {
        return "job_not_assigned";
    }
    if trimmed.starts_with("claimed hash required") {
        return "claimed_hash_required";
    }
    "other"
}

fn log_rejection_at_info(reason_code: &str) -> bool {
    matches!(
        reason_code,
        "stale_job" | "duplicate_share" | "nonce_out_of_range" | "job_not_assigned"
    )
}

#[cfg(test)]
mod tests {
    use super::{log_rejection_at_info, share_reject_reason_code};

    #[test]
    fn stale_and_duplicate_share_reasons_are_classified() {
        assert_eq!(
            share_reject_reason_code("stale job: assignment not found"),
            "stale_job"
        );
        assert_eq!(
            share_reject_reason_code("duplicate share"),
            "duplicate_share"
        );
        assert_eq!(
            share_reject_reason_code("nonce out of assigned range"),
            "nonce_out_of_range"
        );
    }

    #[test]
    fn only_high_signal_rejections_are_logged_at_info() {
        assert!(log_rejection_at_info("stale_job"));
        assert!(log_rejection_at_info("duplicate_share"));
        assert!(!log_rejection_at_info("claimed_hash_required"));
        assert!(!log_rejection_at_info("other"));
    }
}
