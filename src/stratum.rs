use std::collections::{HashMap, VecDeque};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Result};
use futures_util::{Sink, SinkExt, Stream, StreamExt};
use parking_lot::Mutex;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, mpsc, Notify};
use tokio_tungstenite::accept_async;
use tokio_tungstenite::tungstenite::Message;

use crate::dev_fee::should_defer_submit_ack_difficulty;
use crate::engine::{canonical_share_reject_reason, PoolEngine, SubmitAck};
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
const INBOUND_QUEUE_CAPACITY: usize = 256;
const OUTBOUND_QUEUE_CAPACITY: usize = 256;
const SUBMIT_QUEUE_CAPACITY: usize = 256;
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
    normal_tx: mpsc::Sender<String>,
    latest_job: Arc<Mutex<Option<String>>>,
    latest_job_notify: Arc<Notify>,
}

impl OutboundHandle {
    fn new(capacity: usize) -> (Self, mpsc::Receiver<String>) {
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

#[derive(Debug)]
enum InboundFrame {
    Text(String),
    ReadError(String),
}

#[derive(Debug)]
struct QueuedSubmit {
    req_id: u64,
    params: SubmitParams,
    received_at: Instant,
}

#[derive(Debug)]
enum SubmitCompletionOutcome {
    Finished(Result<SubmitAck>),
    WorkerFailure(String),
}

#[derive(Debug)]
struct SubmitCompletion {
    req_id: u64,
    job_id: String,
    nonce: u64,
    outcome: SubmitCompletionOutcome,
}

pub struct StratumServer {
    listen_addr: SocketAddr,
    ws_listen_addr: Option<SocketAddr>,
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
        ws_listen_addr: Option<SocketAddr>,
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
            ws_listen_addr,
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
        if let Some(ws_addr) = self.ws_listen_addr {
            let ws_listener = TcpListener::bind(ws_addr).await?;
            tracing::info!(addr = %ws_addr, "stratum websocket listening");
            let tcp_server = Arc::clone(&self);
            let ws_server = Arc::clone(&self);
            tokio::try_join!(
                async move { tcp_server.run_tcp_listener(listener).await },
                async move { ws_server.run_ws_listener(ws_listener).await }
            )?;
            Ok(())
        } else {
            self.run_tcp_listener(listener).await
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

    async fn run_tcp_listener(self: Arc<Self>, listener: TcpListener) -> Result<()> {
        loop {
            let (stream, peer) = listener.accept().await?;
            let ip = peer.ip().to_string();
            if !self.try_track_conn(&ip) {
                tracing::warn!(ip = %ip, "rejecting stratum connection due to limits");
                continue;
            }

            let this = Arc::clone(&self);
            tokio::spawn(async move {
                if let Err(err) = this.handle_tcp_conn(stream, peer).await {
                    tracing::warn!(peer = %peer, error = %err, "stratum tcp connection ended with error");
                }
                this.untrack_conn(&ip);
            });
        }
    }

    async fn run_ws_listener(self: Arc<Self>, listener: TcpListener) -> Result<()> {
        loop {
            let (stream, peer) = listener.accept().await?;
            let ip = peer.ip().to_string();
            if !self.try_track_conn(&ip) {
                tracing::warn!(ip = %ip, "rejecting stratum websocket connection due to limits");
                continue;
            }

            let this = Arc::clone(&self);
            tokio::spawn(async move {
                if let Err(err) = this.handle_ws_conn(stream, peer).await {
                    tracing::warn!(peer = %peer, error = %err, "stratum websocket connection ended with error");
                }
                this.untrack_conn(&ip);
            });
        }
    }

    async fn handle_tcp_conn(&self, stream: TcpStream, peer: SocketAddr) -> Result<()> {
        let conn_id = peer.to_string();
        let (reader_half, writer_half) = stream.into_split();
        let (inbound_tx, inbound_rx) = mpsc::channel(INBOUND_QUEUE_CAPACITY);
        let reader_task =
            tokio::spawn(
                async move { run_tcp_reader(BufReader::new(reader_half), inbound_tx).await },
            );
        let (outbound, outbound_rx) = OutboundHandle::new(OUTBOUND_QUEUE_CAPACITY);
        let latest_job = Arc::clone(&outbound.latest_job);
        let latest_job_notify = Arc::clone(&outbound.latest_job_notify);
        let writer_task = tokio::spawn(async move {
            run_tcp_outbound_writer(writer_half, outbound_rx, latest_job, latest_job_notify).await
        });
        self.handle_conn_loop(
            peer,
            conn_id,
            inbound_rx,
            reader_task,
            writer_task,
            outbound,
        )
        .await
    }

    async fn handle_ws_conn(&self, stream: TcpStream, peer: SocketAddr) -> Result<()> {
        let conn_id = peer.to_string();
        let ws = accept_async(stream).await?;
        let (writer, reader) = ws.split();
        let (inbound_tx, inbound_rx) = mpsc::channel(INBOUND_QUEUE_CAPACITY);
        let reader_task = tokio::spawn(async move { run_ws_reader(reader, inbound_tx).await });
        let (outbound, outbound_rx) = OutboundHandle::new(OUTBOUND_QUEUE_CAPACITY);
        let latest_job = Arc::clone(&outbound.latest_job);
        let latest_job_notify = Arc::clone(&outbound.latest_job_notify);
        let writer_task = tokio::spawn(async move {
            run_ws_outbound_writer(writer, outbound_rx, latest_job, latest_job_notify).await
        });
        self.handle_conn_loop(
            peer,
            conn_id,
            inbound_rx,
            reader_task,
            writer_task,
            outbound,
        )
        .await
    }

    async fn handle_conn_loop(
        &self,
        peer: SocketAddr,
        conn_id: String,
        mut inbound_rx: mpsc::Receiver<InboundFrame>,
        reader_task: tokio::task::JoinHandle<Result<()>>,
        mut writer_task: tokio::task::JoinHandle<Result<()>>,
        outbound: OutboundHandle,
    ) -> Result<()> {
        let mut writer_task_finished = false;
        let mut logged_in: Option<(String, String, u64)> = None;
        let mut rx_jobs = self.jobs.subscribe();
        let mut rx_notifications = self.notifications.subscribe();
        let post_login_idle_timeout = self.post_login_idle_timeout;
        let submit_rate_limit_window = self.submit_rate_limit_window;
        let submit_rate_limit_max = self.submit_rate_limit_max;
        let mut submit_timestamps = VecDeque::<Instant>::new();
        let (submit_tx, submit_rx) =
            mpsc::channel(submit_rate_limit_max.min(SUBMIT_QUEUE_CAPACITY).max(1));
        let (submit_result_tx, mut submit_result_rx) = mpsc::unbounded_channel();
        let submit_engine = Arc::clone(&self.engine);
        let submit_conn_id = conn_id.clone();
        let submit_task = tokio::spawn(async move {
            run_submit_worker(submit_engine, submit_conn_id, submit_rx, submit_result_tx).await;
        });
        let mut submit_results_open = true;

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
                maybe_submit_result = submit_result_rx.recv(), if submit_results_open => {
                    match maybe_submit_result {
                        Some(completion) => {
                            if let Err(err) = self.handle_submit_completion(
                                peer,
                                &conn_id,
                                Some(&outbound),
                                &mut logged_in,
                                completion,
                            ) {
                                run_result = Err(err);
                                break;
                            }
                        }
                        None => {
                            submit_results_open = false;
                        }
                    }
                }
                maybe_job = rx_jobs.recv(), if logged_in.is_some() => {
                    if maybe_job.is_ok() {
                        if let Some((address, worker, difficulty)) = logged_in.as_mut() {
                            let next_difficulty = match retarget_on_job_tick(
                                Arc::clone(&self.engine),
                                conn_id.clone(),
                                *difficulty,
                            )
                            .await
                            {
                                Ok(next_difficulty) => next_difficulty,
                                Err(err) => {
                                    run_result = Err(err);
                                    break;
                                }
                            };
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
                maybe_inbound = inbound_rx.recv() => {
                    let inbound = match maybe_inbound {
                        Some(v) => v,
                        None => break,
                    };
                    let line = match inbound {
                        InboundFrame::Text(line) => line,
                        InboundFrame::ReadError(err) => {
                            let _ = queue_error(&outbound, 0, &err);
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
                            let queued_submit = QueuedSubmit {
                                req_id: req.id,
                                params,
                                received_at: Instant::now(),
                            };
                            match submit_tx.try_send(queued_submit) {
                                Ok(()) => {}
                                Err(mpsc::error::TrySendError::Full(_)) => {
                                    if let Some((address, _, _)) = logged_in.as_ref() {
                                        self.stats.record_rejected_share(address, "server busy");
                                        tracing::warn!(
                                            peer = %peer,
                                            address = %address,
                                            job_id = %submit_job_id,
                                            nonce = submit_nonce,
                                            "submit queue saturated"
                                        );
                                    }
                                    if let Err(err) = queue_error(&outbound, req.id, "server busy, retry") {
                                        run_result = Err(err);
                                        break;
                                    }
                                }
                                Err(mpsc::error::TrySendError::Closed(_)) => {
                                    run_result = Err(anyhow!("submit worker queue closed"));
                                    break;
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

        drop(submit_tx);
        let mut drain_outbound = if run_result.is_ok() && !writer_task_finished {
            Some(&outbound)
        } else {
            None
        };
        while submit_results_open {
            match submit_result_rx.recv().await {
                Some(completion) => {
                    if let Err(err) = self.handle_submit_completion(
                        peer,
                        &conn_id,
                        drain_outbound,
                        &mut logged_in,
                        completion,
                    ) {
                        if run_result.is_ok() {
                            run_result = Err(err);
                        }
                        drain_outbound = None;
                    }
                }
                None => {
                    submit_results_open = false;
                }
            }
        }
        if let Err(err) = submit_task.await {
            if run_result.is_ok() {
                run_result = Err(anyhow!("stratum submit task failed: {err}"));
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
        match reader_task.await {
            Ok(Ok(())) => {}
            Ok(Err(err)) => {
                if run_result.is_ok() {
                    run_result = Err(err);
                }
            }
            Err(err) => {
                if run_result.is_ok() {
                    run_result = Err(anyhow!("stratum reader task failed: {err}"));
                }
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

    fn handle_submit_completion(
        &self,
        peer: SocketAddr,
        conn_id: &str,
        outbound: Option<&OutboundHandle>,
        logged_in: &mut Option<(String, String, u64)>,
        completion: SubmitCompletion,
    ) -> Result<()> {
        match completion.outcome {
            SubmitCompletionOutcome::Finished(Ok(ack)) => {
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
                    response_difficulty =
                        client_submit_ack_difficulty(address, *difficulty, ack.next_difficulty);
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
                                    .build_miner_job(conn_id, ack.next_difficulty, address)
                            {
                                if let Some(outbound) = outbound {
                                    let notify = StratumNotify {
                                        method: "job".to_string(),
                                        params: serde_json::to_value(miner_job)?,
                                    };
                                    queue_job_json(outbound, &notify)?;
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
                if let Some(outbound) = outbound {
                    let response = StratumResponse {
                        id: completion.req_id,
                        status: Some("ok".to_string()),
                        error: None,
                        result: Some(serde_json::json!({
                            "accepted": ack.accepted,
                            "verified": ack.verified,
                            "status": ack.status,
                            "difficulty": response_difficulty,
                        })),
                    };
                    queue_json(outbound, &response)?;
                    if ack.block_accepted {
                        let miner_notification = block_notification(
                            NOTIFY_MINER_BLOCK_FOUND,
                            "great success: you found a block for the pool",
                        );
                        queue_json(outbound, &miner_notification)?;
                    }
                }
                if ack.block_accepted {
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
            SubmitCompletionOutcome::Finished(Err(err)) => {
                let err_text = err.to_string();
                let reason_code = canonical_share_reject_reason(&err_text);
                if let Some((address, _, _)) = logged_in.as_ref() {
                    self.stats.record_rejected_share(address, reason_code);
                    if log_rejection_at_info(reason_code) {
                        tracing::info!(
                            peer = %peer,
                            address = %address,
                            job_id = %completion.job_id,
                            nonce = completion.nonce,
                            reason_code,
                            error = %err_text,
                            "share rejected"
                        );
                    } else {
                        tracing::debug!(
                            peer = %peer,
                            address = %address,
                            job_id = %completion.job_id,
                            nonce = completion.nonce,
                            reason_code,
                            error = %err_text,
                            "share rejected"
                        );
                    }
                }
                if let Some(outbound) = outbound {
                    queue_error(outbound, completion.req_id, &err_text)?;
                }
            }
            SubmitCompletionOutcome::WorkerFailure(err_text) => {
                if let Some((address, _, _)) = logged_in.as_ref() {
                    self.stats
                        .record_rejected_share(address, "submit worker failure");
                    tracing::warn!(
                        peer = %peer,
                        address = %address,
                        job_id = %completion.job_id,
                        nonce = completion.nonce,
                        error = %err_text,
                        "submit worker failure"
                    );
                }
                if let Some(outbound) = outbound {
                    queue_error(
                        outbound,
                        completion.req_id,
                        &format!("submit worker failure: {err_text}"),
                    )?;
                }
            }
        }
        Ok(())
    }
}

async fn run_submit_worker(
    engine: Arc<PoolEngine>,
    conn_id: String,
    mut submit_rx: mpsc::Receiver<QueuedSubmit>,
    submit_result_tx: mpsc::UnboundedSender<SubmitCompletion>,
) {
    while let Some(queued) = submit_rx.recv().await {
        let engine = Arc::clone(&engine);
        let submit_conn_id = conn_id.clone();
        let job_id = queued.params.job_id.clone();
        let nonce = queued.params.nonce;
        let outcome = match tokio::task::spawn_blocking(move || {
            engine.submit_with_received_at(
                &submit_conn_id,
                queued.params.job_id,
                queued.params.nonce,
                queued.params.claimed_hash,
                queued.received_at,
            )
        })
        .await
        {
            Ok(result) => SubmitCompletionOutcome::Finished(result),
            Err(err) => SubmitCompletionOutcome::WorkerFailure(err.to_string()),
        };
        if submit_result_tx
            .send(SubmitCompletion {
                req_id: queued.req_id,
                job_id,
                nonce,
                outcome,
            })
            .is_err()
        {
            break;
        }
    }
}

async fn retarget_on_job_tick(
    engine: Arc<PoolEngine>,
    conn_id: String,
    fallback_difficulty: u64,
) -> Result<u64> {
    tokio::task::spawn_blocking(move || {
        engine
            .retarget_on_job_if_needed(&conn_id)
            .unwrap_or(fallback_difficulty)
    })
    .await
    .map_err(|err| anyhow!("stratum job retarget task failed: {err}"))
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
    let payload = serialize_json_text(value)?;
    outbound
        .normal_tx
        .try_send(payload)
        .map_err(|err| anyhow!("stratum outbound queue saturated: {err}"))
}

fn queue_job_json<T: serde::Serialize>(outbound: &OutboundHandle, value: &T) -> Result<()> {
    let payload = serialize_json_text(value)?;
    *outbound.latest_job.lock() = Some(payload);
    outbound.latest_job_notify.notify_one();
    Ok(())
}

fn serialize_json_text<T: serde::Serialize>(value: &T) -> Result<String> {
    serde_json::to_string(value).map_err(Into::into)
}

async fn run_tcp_reader(
    mut reader: BufReader<tokio::net::tcp::OwnedReadHalf>,
    inbound_tx: mpsc::Sender<InboundFrame>,
) -> Result<()> {
    loop {
        match read_line_limited(&mut reader, MAX_STRATUM_REQUEST_BYTES).await {
            Ok(Some(line)) => {
                if inbound_tx.send(InboundFrame::Text(line)).await.is_err() {
                    break;
                }
            }
            Ok(None) => break,
            Err(err) => {
                let _ = inbound_tx
                    .send(InboundFrame::ReadError(err.to_string()))
                    .await;
                break;
            }
        }
    }
    Ok(())
}

async fn run_ws_reader<S>(mut reader: S, inbound_tx: mpsc::Sender<InboundFrame>) -> Result<()>
where
    S: Stream<Item = std::result::Result<Message, tokio_tungstenite::tungstenite::Error>> + Unpin,
{
    while let Some(message) = reader.next().await {
        match message {
            Ok(Message::Text(text)) => {
                if inbound_tx
                    .send(InboundFrame::Text(text.to_string()))
                    .await
                    .is_err()
                {
                    break;
                }
            }
            Ok(Message::Binary(_)) => {
                let _ = inbound_tx
                    .send(InboundFrame::ReadError(
                        "binary websocket frames are not supported".to_string(),
                    ))
                    .await;
                break;
            }
            Ok(Message::Close(_)) => break,
            Ok(Message::Ping(_)) | Ok(Message::Pong(_)) => {}
            Ok(_) => {}
            Err(err) => {
                let _ = inbound_tx
                    .send(InboundFrame::ReadError(err.to_string()))
                    .await;
                break;
            }
        }
    }
    Ok(())
}

async fn run_tcp_outbound_writer<W>(
    mut writer: W,
    mut rx: mpsc::Receiver<String>,
    latest_job: Arc<Mutex<Option<String>>>,
    latest_job_notify: Arc<Notify>,
) -> Result<()>
where
    W: tokio::io::AsyncWrite + Unpin,
{
    while let Some(data) =
        next_outbound_payload(&mut rx, latest_job.as_ref(), &latest_job_notify).await
    {
        write_tcp_outbound_payload(&mut writer, &data).await?;
    }
    Ok(())
}

async fn run_ws_outbound_writer<W>(
    mut writer: W,
    mut rx: mpsc::Receiver<String>,
    latest_job: Arc<Mutex<Option<String>>>,
    latest_job_notify: Arc<Notify>,
) -> Result<()>
where
    W: Sink<Message, Error = tokio_tungstenite::tungstenite::Error> + Unpin,
{
    while let Some(data) =
        next_outbound_payload(&mut rx, latest_job.as_ref(), &latest_job_notify).await
    {
        write_ws_outbound_payload(&mut writer, &data).await?;
    }
    Ok(())
}

async fn next_outbound_payload(
    rx: &mut mpsc::Receiver<String>,
    latest_job: &Mutex<Option<String>>,
    latest_job_notify: &Notify,
) -> Option<String> {
    loop {
        if let Some(data) = take_priority_job(latest_job) {
            return Some(data);
        }

        tokio::select! {
            biased;
            _ = latest_job_notify.notified() => {}
            maybe = rx.recv() => {
                let Some(data) = maybe else {
                    return take_priority_job(latest_job);
                };
                return Some(data);
            }
        }
    }
}

fn take_priority_job(latest_job: &Mutex<Option<String>>) -> Option<String> {
    latest_job.lock().take()
}

async fn write_tcp_outbound_payload<W>(writer: &mut W, data: &str) -> Result<()>
where
    W: tokio::io::AsyncWrite + Unpin,
{
    let mut line = data.as_bytes().to_vec();
    line.push(b'\n');
    match tokio::time::timeout(OUTBOUND_WRITE_TIMEOUT, writer.write_all(&line)).await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(err)) => Err(err.into()),
        Err(_) => Err(anyhow!(
            "stratum outbound write timed out after {}ms",
            OUTBOUND_WRITE_TIMEOUT.as_millis()
        )),
    }
}

async fn write_ws_outbound_payload<W>(writer: &mut W, data: &str) -> Result<()>
where
    W: Sink<Message, Error = tokio_tungstenite::tungstenite::Error> + Unpin,
{
    match tokio::time::timeout(
        OUTBOUND_WRITE_TIMEOUT,
        writer.send(Message::Text(data.to_string().into())),
    )
    .await
    {
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
        retarget_on_job_tick, run_submit_worker, run_tcp_outbound_writer, OutboundHandle,
        QueuedSubmit, StratumResponse, SubmitCompletionOutcome,
    };
    use crate::config::Config;
    use crate::engine::canonical_share_reject_reason;
    use crate::engine::{
        InMemoryJobs, InMemoryNode, InMemoryStore, Job, PoolEngine, ShareRecord, ShareStore,
    };
    use crate::pow::PowHasher;
    use crate::protocol::StratumNotify;
    use crate::validation::ValidationEngine;
    use serde_json::Value;
    use std::sync::Arc;
    use std::time::{Duration, Instant};
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
            .try_send("occupied".to_string())
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
            .send("a".repeat(64))
            .await
            .expect("seed payload should enqueue");
        drop(outbound);

        let err = run_tcp_outbound_writer(writer, rx, latest_job, latest_job_notify)
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

        run_tcp_outbound_writer(writer, rx, latest_job, latest_job_notify)
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

    #[tokio::test]
    async fn submit_worker_preserves_submit_order() {
        struct SlowHasher;

        impl PowHasher for SlowHasher {
            fn hash(&self, _header_base: &[u8], _nonce: u64) -> anyhow::Result<[u8; 32]> {
                std::thread::sleep(Duration::from_millis(60));
                Ok([0x01; 32])
            }
        }

        let mut cfg = Config::default();
        cfg.validation_mode = "full".to_string();
        cfg.max_verifiers = 1;
        cfg.max_validation_queue = 8;
        cfg.job_timeout = "10s".to_string();
        let validation = Arc::new(ValidationEngine::new(cfg.clone(), Arc::new(SlowHasher)));
        let jobs = Arc::new(InMemoryJobs::default());
        jobs.insert(Job {
            id: "job1".to_string(),
            height: 100,
            header_base: vec![1, 2, 3],
            network_target: [0u8; 32],
            network_difficulty: 1_000_000,
            template_id: Some("tmpl1".to_string()),
            full_block: None,
        });
        let address = bs58::encode([0x11; 64]).into_string();
        jobs.insert_assignment("assign1", "job1", 1, Some(address.clone()), 0, u64::MAX);
        let engine = Arc::new(PoolEngine::new(
            cfg,
            validation,
            jobs,
            Arc::new(InMemoryStore::default()),
            Arc::new(InMemoryNode::default()),
        ));
        engine
            .login(
                "conn1",
                address,
                None,
                2,
                vec!["submit_claimed_hash".to_string()],
            )
            .expect("login should succeed");

        let (submit_tx, submit_rx) = tokio::sync::mpsc::channel(2);
        let (result_tx, mut result_rx) = tokio::sync::mpsc::unbounded_channel();
        let worker = tokio::spawn(run_submit_worker(
            Arc::clone(&engine),
            "conn1".to_string(),
            submit_rx,
            result_tx,
        ));
        let submitted_at = Instant::now();
        submit_tx
            .send(QueuedSubmit {
                req_id: 1,
                params: crate::protocol::SubmitParams {
                    job_id: "assign1".to_string(),
                    nonce: 1,
                    claimed_hash: Some(hex::encode([0x01; 32])),
                },
                received_at: submitted_at,
            })
            .await
            .expect("first submit should enqueue");
        submit_tx
            .send(QueuedSubmit {
                req_id: 2,
                params: crate::protocol::SubmitParams {
                    job_id: "assign1".to_string(),
                    nonce: 2,
                    claimed_hash: Some(hex::encode([0x01; 32])),
                },
                received_at: submitted_at + Duration::from_millis(5),
            })
            .await
            .expect("second submit should enqueue");
        drop(submit_tx);

        let first = result_rx.recv().await.expect("first result");
        let second = result_rx.recv().await.expect("second result");
        worker.await.expect("worker should exit cleanly");

        assert_eq!(first.req_id, 1);
        assert_eq!(second.req_id, 2);
        assert!(matches!(
            first.outcome,
            SubmitCompletionOutcome::Finished(Ok(_))
        ));
        assert!(matches!(
            second.outcome,
            SubmitCompletionOutcome::Finished(Ok(_))
        ));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn job_tick_retarget_runs_on_blocking_pool() {
        struct RuntimeStartingStore;

        impl ShareStore for RuntimeStartingStore {
            fn is_share_seen(&self, _job_id: &str, _nonce: u64) -> anyhow::Result<bool> {
                Ok(false)
            }

            fn mark_share_seen(&self, _job_id: &str, _nonce: u64) -> anyhow::Result<()> {
                Ok(())
            }

            fn add_share(&self, _share: ShareRecord) -> anyhow::Result<()> {
                Ok(())
            }

            fn upsert_vardiff_hint(
                &self,
                _address: &str,
                _worker: &str,
                _difficulty: u64,
                _updated_at: std::time::SystemTime,
            ) -> anyhow::Result<()> {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("runtime");
                runtime.block_on(async {});
                Ok(())
            }
        }

        struct StaticHasher;

        impl PowHasher for StaticHasher {
            fn hash(&self, _header_base: &[u8], _nonce: u64) -> anyhow::Result<[u8; 32]> {
                Ok([0x01; 32])
            }
        }

        let cfg = Config::default();
        let validation = Arc::new(ValidationEngine::new(cfg.clone(), Arc::new(StaticHasher)));
        let jobs = Arc::new(InMemoryJobs::default());
        let engine = Arc::new(PoolEngine::new(
            cfg.clone(),
            validation,
            jobs,
            Arc::new(RuntimeStartingStore),
            Arc::new(InMemoryNode::default()),
        ));
        let address = bs58::encode([0x44; 64]).into_string();
        engine
            .login(
                "conn1",
                address,
                None,
                2,
                vec!["submit_claimed_hash".to_string()],
            )
            .expect("login should succeed");

        let difficulty = retarget_on_job_tick(
            Arc::clone(&engine),
            "conn1".to_string(),
            cfg.initial_share_difficulty,
        )
        .await
        .expect("job tick should not panic on runtime-backed store");

        assert_eq!(difficulty, cfg.initial_share_difficulty);
    }
}
