use std::collections::{HashMap, VecDeque};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Result};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, mpsc, Notify};

use crate::config::Config;
use crate::dev_fee::should_defer_submit_ack_difficulty;
use crate::engine::{canonical_share_reject_reason, PoolEngine, SubmitAck, SubmitQueueRoute};
use crate::jobs::{AssignmentRangeMode, JobManager};
use crate::protocol::{
    normalize_worker_name, LoginParams, StratumNotify, StratumRequest, StratumResponse,
    SubmitParams, CAP_SAME_TEMPLATE_REBIND_V1, METHOD_LOGIN, METHOD_NOTIFICATION, METHOD_SUBMIT,
    NOTIFY_MINER_BLOCK_FOUND, NOTIFY_POOL_BLOCK_SOLVED,
};
use crate::stats::PoolStats;
use crate::telemetry::{PercentileSummary, QueueTracker};

const MAX_CONNS_PER_IP: usize = 256;
const MAX_CONNS_TOTAL: usize = 4096;
const LOGIN_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_STRATUM_REQUEST_BYTES: usize = 8 * 1024;
const NOTIFICATION_CHANNEL_CAPACITY: usize = 256;
const INBOUND_QUEUE_CAPACITY: usize = 256;
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

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SubmitRuntimeSnapshot {
    #[serde(default)]
    pub candidate_queue_depth: usize,
    #[serde(default)]
    pub regular_queue_depth: usize,
    #[serde(default)]
    pub candidate_oldest_age_millis: Option<u64>,
    #[serde(default)]
    pub regular_oldest_age_millis: Option<u64>,
    #[serde(default)]
    pub candidate_wait: PercentileSummary,
    #[serde(default)]
    pub regular_wait: PercentileSummary,
}

#[derive(Debug)]
struct SubmitWorkItem {
    conn_id: String,
    req_id: u64,
    params: SubmitParams,
    received_at: Instant,
    completion_tx: mpsc::UnboundedSender<SubmitCompletion>,
    candidate_permit: Option<CandidateClaimPermit>,
}

#[derive(Debug, Default)]
struct CandidateClaimState {
    recent: VecDeque<Instant>,
    in_flight: usize,
}

#[derive(Debug)]
struct CandidateClaimTracker {
    window: Duration,
    max_per_window: usize,
    max_inflight: usize,
    state: Mutex<HashMap<String, CandidateClaimState>>,
}

#[derive(Debug, Clone)]
struct CandidateClaimPermit {
    tracker: Arc<CandidateClaimTracker>,
    key: String,
}

#[derive(Debug)]
struct SubmitDispatcher {
    candidate_tx: flume::Sender<SubmitWorkItem>,
    regular_tx: flume::Sender<SubmitWorkItem>,
    candidate_queue: Arc<QueueTracker>,
    regular_queue: Arc<QueueTracker>,
    candidate_tracker: Arc<CandidateClaimTracker>,
}

impl CandidateClaimTracker {
    fn new(window: Duration, max_per_window: usize, max_inflight: usize) -> Arc<Self> {
        Arc::new(Self {
            window: window.max(Duration::from_secs(1)),
            max_per_window: max_per_window.max(1),
            max_inflight: max_inflight.max(1),
            state: Mutex::new(HashMap::new()),
        })
    }

    fn try_acquire(self: &Arc<Self>, key: String, now: Instant) -> Result<CandidateClaimPermit> {
        let mut state = self.state.lock();
        let entry = state.entry(key.clone()).or_default();
        while entry
            .recent
            .front()
            .is_some_and(|seen_at| now.saturating_duration_since(*seen_at) >= self.window)
        {
            entry.recent.pop_front();
        }
        if entry.in_flight >= self.max_inflight {
            return Err(anyhow!("candidate claim busy, retry"));
        }
        if entry.recent.len() >= self.max_per_window {
            return Err(anyhow!("candidate claim rate limited, retry"));
        }
        entry.recent.push_back(now);
        entry.in_flight += 1;
        Ok(CandidateClaimPermit {
            tracker: Arc::clone(self),
            key,
        })
    }

    fn release(&self, key: &str) {
        let mut state = self.state.lock();
        let Some(entry) = state.get_mut(key) else {
            return;
        };
        entry.in_flight = entry.in_flight.saturating_sub(1);
        if entry.in_flight == 0 && entry.recent.is_empty() {
            state.remove(key);
        }
    }
}

impl CandidateClaimPermit {
    fn release(self) {
        self.tracker.release(&self.key);
    }
}

impl SubmitDispatcher {
    fn new(engine: Arc<PoolEngine>, cfg: &Config) -> Arc<Self> {
        let (candidate_tx, candidate_rx) =
            flume::bounded::<SubmitWorkItem>(cfg.candidate_submit_queue_size());
        let (regular_tx, regular_rx) =
            flume::bounded::<SubmitWorkItem>(cfg.regular_submit_queue_size());
        let candidate_queue = Arc::new(QueueTracker::new(512));
        let regular_queue = Arc::new(QueueTracker::new(512));
        engine.attach_submit_regular_queue(Arc::clone(&regular_queue));
        let dispatcher = Arc::new(Self {
            candidate_tx,
            regular_tx,
            candidate_queue,
            regular_queue,
            candidate_tracker: CandidateClaimTracker::new(
                cfg.candidate_claim_window_duration(),
                cfg.candidate_claim_max_per_window as usize,
                cfg.candidate_claim_max_inflight as usize,
            ),
        });

        for _ in 0..cfg.candidate_submit_workers() {
            let engine = Arc::clone(&engine);
            let dispatcher = Arc::clone(&dispatcher);
            let candidate_rx = candidate_rx.clone();
            tokio::spawn(async move {
                run_submit_worker(engine, dispatcher, candidate_rx, true).await;
            });
        }

        for _ in 0..cfg.regular_submit_workers() {
            let engine = Arc::clone(&engine);
            let dispatcher = Arc::clone(&dispatcher);
            let regular_rx = regular_rx.clone();
            tokio::spawn(async move {
                run_submit_worker(engine, dispatcher, regular_rx, false).await;
            });
        }

        dispatcher
    }

    fn try_acquire_candidate_permit(
        &self,
        address: &str,
        peer: SocketAddr,
        now: Instant,
    ) -> Result<CandidateClaimPermit> {
        let key = format!("{}|{}", address.trim(), peer.ip());
        self.candidate_tracker.try_acquire(key, now)
    }

    fn try_enqueue(&self, item: SubmitWorkItem, route: SubmitQueueRoute) -> Result<()> {
        let queued_at = Instant::now();
        let tracker_id = match route {
            SubmitQueueRoute::Candidate => self.candidate_queue.push(queued_at),
            SubmitQueueRoute::Regular => self.regular_queue.push(queued_at),
        };
        let result = match route {
            SubmitQueueRoute::Candidate => self.candidate_tx.try_send(item),
            SubmitQueueRoute::Regular => self.regular_tx.try_send(item),
        };
        match result {
            Ok(()) => Ok(()),
            Err(flume::TrySendError::Full(item)) | Err(flume::TrySendError::Disconnected(item)) => {
                match route {
                    SubmitQueueRoute::Candidate => self.candidate_queue.remove(tracker_id),
                    SubmitQueueRoute::Regular => self.regular_queue.remove(tracker_id),
                }
                if let Some(permit) = item.candidate_permit {
                    permit.release();
                }
                Err(anyhow!("server busy, retry"))
            }
        }
    }

    fn snapshot(&self) -> SubmitRuntimeSnapshot {
        let now = Instant::now();
        let candidate = self.candidate_queue.snapshot(now);
        let regular = self.regular_queue.snapshot(now);
        SubmitRuntimeSnapshot {
            candidate_queue_depth: candidate.depth,
            regular_queue_depth: regular.depth,
            candidate_oldest_age_millis: candidate.oldest_age_millis,
            regular_oldest_age_millis: regular.oldest_age_millis,
            candidate_wait: candidate.wait,
            regular_wait: regular.wait,
        }
    }
}

pub struct StratumServer {
    listen_addr: SocketAddr,
    engine: Arc<PoolEngine>,
    jobs: Arc<JobManager>,
    stats: Arc<PoolStats>,
    submit_dispatcher: Arc<SubmitDispatcher>,
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
        cfg: Config,
    ) -> Arc<Self> {
        let (notifications, _) = broadcast::channel(NOTIFICATION_CHANNEL_CAPACITY);
        let submit_dispatcher = SubmitDispatcher::new(Arc::clone(&engine), &cfg);
        Arc::new(Self {
            listen_addr,
            engine,
            jobs,
            stats,
            submit_dispatcher,
            conn_state: Arc::new(Mutex::new(ConnState {
                counts: HashMap::new(),
                total: 0,
            })),
            notifications,
            post_login_idle_timeout: cfg.stratum_idle_timeout_duration(),
            submit_rate_limit_window: cfg.stratum_submit_rate_limit_window_duration(),
            submit_rate_limit_max: cfg.stratum_submit_rate_limit_max.max(1) as usize,
        })
    }

    pub fn submit_snapshot(&self) -> SubmitRuntimeSnapshot {
        self.submit_dispatcher.snapshot()
    }

    pub async fn run(self: Arc<Self>) -> Result<()> {
        let listener = TcpListener::bind(self.listen_addr).await?;
        tracing::info!(addr = %self.listen_addr, "stratum listening");
        self.run_tcp_listener(listener).await
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
        let (submit_result_tx, mut submit_result_rx) = mpsc::unbounded_channel();

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
                maybe_submit_result = submit_result_rx.recv() => {
                    if let Some(completion) = maybe_submit_result {
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
                            let received_at = Instant::now();
                            let submit_job_id = params.job_id.clone();
                            let submit_nonce = params.nonce;
                            let route = match self
                                .engine
                                .preclassify_submit_route(&params.job_id, params.claimed_hash.as_deref(), received_at)
                            {
                                Ok(route) => route,
                                Err(err) => {
                                    if let Some((address, _, _)) = logged_in.as_ref() {
                                        self.stats
                                            .record_rejected_share(address, canonical_share_reject_reason(&err.to_string()));
                                    }
                                    self.persist_prequeue_reject_async(&conn_id, &params.job_id, params.nonce, received_at, &err.to_string());
                                    if let Err(queue_err) = queue_error(&outbound, req.id, &err.to_string()) {
                                        run_result = Err(queue_err);
                                        break;
                                    }
                                    continue;
                                }
                            };
                            let candidate_permit = if route == SubmitQueueRoute::Candidate {
                                let Some((address, _, _)) = logged_in.as_ref() else {
                                    if let Err(err) = queue_error(&outbound, req.id, "not logged in") {
                                        run_result = Err(err);
                                        break;
                                    }
                                    continue;
                                };
                                match self
                                    .submit_dispatcher
                                    .try_acquire_candidate_permit(address, peer, received_at)
                                {
                                    Ok(permit) => Some(permit),
                                    Err(err) => {
                                        self.stats
                                            .record_rejected_share(address, canonical_share_reject_reason(&err.to_string()));
                                        self.persist_prequeue_reject_async(&conn_id, &params.job_id, params.nonce, received_at, &err.to_string());
                                        if let Err(queue_err) = queue_error(&outbound, req.id, &err.to_string()) {
                                            run_result = Err(queue_err);
                                            break;
                                        }
                                        continue;
                                    }
                                }
                            } else {
                                None
                            };
                            let work = SubmitWorkItem {
                                conn_id: conn_id.clone(),
                                req_id: req.id,
                                params,
                                received_at,
                                completion_tx: submit_result_tx.clone(),
                                candidate_permit,
                            };
                            if let Err(err) = self.submit_dispatcher.try_enqueue(work, route) {
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
                                self.persist_prequeue_reject_async(&conn_id, &submit_job_id, submit_nonce, received_at, &err.to_string());
                                if let Err(queue_err) = queue_error(&outbound, req.id, &err.to_string()) {
                                    run_result = Err(queue_err);
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
                            let range_mode = if self
                                .engine
                                .session_supports_capability(conn_id, CAP_SAME_TEMPLATE_REBIND_V1)
                            {
                                AssignmentRangeMode::PreserveCurrent
                            } else {
                                AssignmentRangeMode::Fresh
                            };
                            if let Some(miner_job) = self.jobs.build_miner_job_with_range_mode(
                                conn_id,
                                ack.next_difficulty,
                                address,
                                range_mode,
                            ) {
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

    fn persist_prequeue_reject_async(
        &self,
        conn_id: &str,
        job_id: &str,
        nonce: u64,
        received_at: Instant,
        reason: &str,
    ) {
        let engine = Arc::clone(&self.engine);
        let conn_id = conn_id.to_string();
        let job_id = job_id.to_string();
        let reason = reason.to_string();
        tokio::spawn(async move {
            let _ = tokio::task::spawn_blocking(move || {
                engine.record_prequeue_reject(&conn_id, &job_id, nonce, received_at, &reason);
            })
            .await;
        });
    }
}

async fn run_submit_worker(
    engine: Arc<PoolEngine>,
    dispatcher: Arc<SubmitDispatcher>,
    submit_rx: flume::Receiver<SubmitWorkItem>,
    candidate_lane: bool,
) {
    while let Ok(queued) = submit_rx.recv_async().await {
        let started_at = Instant::now();
        if candidate_lane {
            dispatcher.candidate_queue.pop_and_record_wait(started_at);
        } else {
            dispatcher.regular_queue.pop_and_record_wait(started_at);
        }
        let engine = Arc::clone(&engine);
        let conn_id = queued.conn_id.clone();
        let job_id = queued.params.job_id.clone();
        let submit_job_id = job_id.clone();
        let nonce = queued.params.nonce;
        let claimed_hash = queued.params.claimed_hash.clone();
        let completion_tx = queued.completion_tx.clone();
        let req_id = queued.req_id;
        let received_at = queued.received_at;
        let candidate_permit = queued.candidate_permit;
        let outcome = match tokio::task::spawn_blocking(move || {
            engine.submit_with_received_at(
                &conn_id,
                submit_job_id,
                nonce,
                claimed_hash,
                received_at,
            )
        })
        .await
        {
            Ok(result) => SubmitCompletionOutcome::Finished(result),
            Err(err) => SubmitCompletionOutcome::WorkerFailure(err.to_string()),
        };
        if let Some(permit) = candidate_permit {
            permit.release();
        }
        if completion_tx
            .send(SubmitCompletion {
                req_id,
                job_id,
                nonce,
                outcome,
            })
            .is_err()
        {
            continue;
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
        retarget_on_job_tick, run_tcp_outbound_writer, CandidateClaimTracker, OutboundHandle,
        StratumResponse, StratumServer,
    };
    use crate::config::Config;
    use crate::engine::canonical_share_reject_reason;
    use crate::engine::{InMemoryJobs, InMemoryNode, Job, PoolEngine, ShareRecord, ShareStore};
    use crate::jobs::{JobManager, MinerJob};
    use crate::node::NodeClient;
    use crate::pow::PowHasher;
    use crate::protocol::{StratumNotify, StratumRequest};
    use crate::stats::PoolStats;
    use crate::validation::ValidationEngine;
    use serde::Serialize;
    use serde_json::{json, Value};
    use std::collections::HashMap;
    use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
    use std::sync::Arc;
    use std::time::{Duration, Instant};
    use tokio::io::{duplex, AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
    use tokio::net::{
        tcp::{OwnedReadHalf, OwnedWriteHalf},
        TcpListener, TcpStream,
    };

    const TEST_SUBMIT_DELAY: Duration = Duration::from_millis(200);

    #[derive(Debug, Clone, Copy)]
    struct SleepyQueueHasher;

    impl SleepyQueueHasher {
        fn hash_for_nonce(nonce: u64) -> [u8; 32] {
            if nonce % 2 == 0 {
                [0u8; 32]
            } else {
                let mut out = [0u8; 32];
                out[24..].copy_from_slice(&nonce.to_be_bytes());
                out
            }
        }

        fn claimed_hash_hex(nonce: u64) -> String {
            hex::encode(Self::hash_for_nonce(nonce))
        }
    }

    impl PowHasher for SleepyQueueHasher {
        fn hash(&self, _header_base: &[u8], nonce: u64) -> anyhow::Result<[u8; 32]> {
            std::thread::sleep(TEST_SUBMIT_DELAY);
            Ok(Self::hash_for_nonce(nonce))
        }
    }

    fn stratum_test_cfg() -> Config {
        Config {
            validation_mode: "full".to_string(),
            enable_vardiff: false,
            initial_share_difficulty: 1,
            min_share_difficulty: 1,
            max_share_difficulty: 1_000_000,
            max_verifiers: 2,
            candidate_verifiers: 1,
            regular_verifiers: 1,
            candidate_submit_queue: 1,
            regular_submit_queue: 1,
            candidate_submit_workers: 1,
            regular_submit_workers: 1,
            candidate_validation_queue: 8,
            regular_validation_queue: 8,
            max_validation_queue: 8,
            validation_wait_timeout: "2s".to_string(),
            job_timeout: "10s".to_string(),
            stratum_idle_timeout: "60s".to_string(),
            stratum_submit_rate_limit_window: "10s".to_string(),
            stratum_submit_rate_limit_max: 64,
            ..Config::default()
        }
    }

    fn stratum_test_job(id: &str, height: u64) -> Job {
        Job {
            id: id.to_string(),
            height,
            header_base: vec![0xAB; 92],
            network_target: [0u8; 32],
            network_difficulty: 1,
            template_id: Some(format!("tmpl-{height}")),
            full_block: None,
        }
    }

    fn nonce_with_parity(job: &MinerJob, even: bool, offset: u64) -> u64 {
        let target_parity = if even { 0 } else { 1 };
        let mut nonce = if job.nonce_start % 2 == target_parity {
            job.nonce_start
        } else {
            job.nonce_start.saturating_add(1)
        };
        nonce = nonce.saturating_add(offset.saturating_mul(2));
        assert!(
            nonce <= job.nonce_end,
            "assignment range must have enough nonces for the test"
        );
        nonce
    }

    fn test_miner_address(seed: u8) -> String {
        bs58::encode([seed; 64]).into_string()
    }

    async fn build_tcp_test_server(cfg: Config) -> (Arc<StratumServer>, Arc<JobManager>) {
        tokio::task::spawn_blocking(move || {
            let node_client =
                Arc::new(NodeClient::new("http://127.0.0.1:1", "").expect("node client"));
            let jobs = JobManager::new(node_client, cfg.clone());
            jobs.install_test_job(stratum_test_job("job-initial", 100));

            let validation = Arc::new(ValidationEngine::new(
                cfg.clone(),
                Arc::new(SleepyQueueHasher),
            ));
            let engine = Arc::new(PoolEngine::new(
                cfg.clone(),
                validation,
                Arc::clone(&jobs) as Arc<dyn crate::engine::JobRepository>,
                Arc::new(crate::engine::InMemoryStore::default()),
                Arc::new(InMemoryNode::default()),
            ));
            let stats = Arc::new(PoolStats::default());
            let listen_addr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0));
            let server = StratumServer::new(listen_addr, engine, Arc::clone(&jobs), stats, cfg);
            (server, jobs)
        })
        .await
        .expect("build tcp test server on blocking pool")
    }

    async fn start_test_tcp_listener(
        server: Arc<StratumServer>,
    ) -> (SocketAddr, tokio::task::JoinHandle<anyhow::Result<()>>) {
        let listener = TcpListener::bind(SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0)))
            .await
            .expect("bind test listener");
        let addr = listener.local_addr().expect("listener addr");
        let task = tokio::spawn(async move { server.run_tcp_listener(listener).await });
        (addr, task)
    }

    async fn connect_test_client(addr: SocketAddr) -> (BufReader<OwnedReadHalf>, OwnedWriteHalf) {
        let stream = TcpStream::connect(addr).await.expect("connect test client");
        let (reader, writer) = stream.into_split();
        (BufReader::new(reader), writer)
    }

    async fn shutdown_test_listener(
        listener_task: tokio::task::JoinHandle<anyhow::Result<()>>,
        server: Arc<StratumServer>,
        jobs: Arc<JobManager>,
    ) {
        listener_task.abort();
        let _ = listener_task.await;
        for _ in 0..40 {
            if server.conn_state.lock().total == 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        tokio::task::spawn_blocking(move || {
            drop(server);
            drop(jobs);
        })
        .await
        .expect("drop test server on blocking pool");
    }

    async fn write_json_line<T: Serialize>(writer: &mut OwnedWriteHalf, value: &T) {
        let payload = serde_json::to_string(value).expect("serialize request");
        writer
            .write_all(payload.as_bytes())
            .await
            .expect("write request");
        writer.write_all(b"\n").await.expect("write newline");
        writer.flush().await.expect("flush request");
    }

    async fn read_json_line(reader: &mut BufReader<OwnedReadHalf>) -> Value {
        let mut line = String::new();
        tokio::time::timeout(Duration::from_secs(2), reader.read_line(&mut line))
            .await
            .expect("timed out waiting for message")
            .expect("read line");
        assert!(!line.trim().is_empty(), "expected a JSON message");
        serde_json::from_str(line.trim()).expect("decode json line")
    }

    async fn login_test_client(
        reader: &mut BufReader<OwnedReadHalf>,
        writer: &mut OwnedWriteHalf,
        req_id: u64,
        address: &str,
        worker: &str,
    ) -> MinerJob {
        write_json_line(
            writer,
            &StratumRequest {
                id: req_id,
                method: crate::protocol::METHOD_LOGIN.to_string(),
                params: json!({
                    "address": address,
                    "worker": worker,
                    "protocol_version": 2,
                    "capabilities": ["submit_claimed_hash"],
                }),
            },
        )
        .await;

        let login = read_json_line(reader).await;
        assert_eq!(login["id"].as_u64(), Some(req_id));
        assert_eq!(login["status"].as_str(), Some("ok"));

        let job = read_json_line(reader).await;
        assert_eq!(job["method"].as_str(), Some("job"));
        serde_json::from_value(job["params"].clone()).expect("decode miner job")
    }

    async fn send_submit(
        writer: &mut OwnedWriteHalf,
        req_id: u64,
        job_id: &str,
        nonce: u64,
        claimed_hash: String,
    ) {
        write_json_line(
            writer,
            &StratumRequest {
                id: req_id,
                method: crate::protocol::METHOD_SUBMIT.to_string(),
                params: json!({
                    "job_id": job_id,
                    "nonce": nonce,
                    "claimed_hash": claimed_hash,
                }),
            },
        )
        .await;
    }

    async fn read_response_map(
        reader: &mut BufReader<OwnedReadHalf>,
        ids: &[u64],
    ) -> (HashMap<u64, Value>, Vec<u64>) {
        let mut remaining = ids
            .iter()
            .copied()
            .collect::<std::collections::BTreeSet<_>>();
        let mut responses = HashMap::new();
        let mut order = Vec::new();
        while !remaining.is_empty() {
            let message = read_json_line(reader).await;
            let Some(id) = message.get("id").and_then(Value::as_u64) else {
                continue;
            };
            if remaining.remove(&id) {
                order.push(id);
                responses.insert(id, message);
            }
        }
        (responses, order)
    }

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
        assert_eq!(
            canonical_share_reject_reason("server busy, retry"),
            "server busy"
        );
        assert_eq!(
            canonical_share_reject_reason("validation timeout"),
            "validation timeout"
        );
        assert_eq!(
            canonical_share_reject_reason("candidate claim busy, retry"),
            "candidate claim busy"
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

    #[test]
    fn candidate_claim_tracker_enforces_inflight_and_rate_limits() {
        let tracker = CandidateClaimTracker::new(Duration::from_secs(60), 2, 1);
        let now = Instant::now();

        let first = tracker
            .try_acquire("addr|127.0.0.1".to_string(), now)
            .expect("first candidate claim should acquire");
        assert!(
            tracker
                .try_acquire("addr|127.0.0.1".to_string(), now + Duration::from_millis(1))
                .is_err(),
            "second in-flight candidate claim should be rejected"
        );
        first.release();

        let second = tracker
            .try_acquire("addr|127.0.0.1".to_string(), now + Duration::from_millis(2))
            .expect("claim should acquire once the first permit is released");
        second.release();

        assert!(
            tracker
                .try_acquire("addr|127.0.0.1".to_string(), now + Duration::from_millis(3))
                .is_err(),
            "third claim inside the same window should hit the rate limit"
        );
        assert!(
            tracker
                .try_acquire(
                    "other|127.0.0.1".to_string(),
                    now + Duration::from_millis(3)
                )
                .is_ok(),
            "other addresses should not share the rate limiter bucket"
        );
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn tcp_submit_queue_saturation_returns_busy_and_connection_recovers() {
        let cfg = stratum_test_cfg();
        let (server, _jobs) = build_tcp_test_server(cfg).await;
        let (addr, listener_task) = start_test_tcp_listener(Arc::clone(&server)).await;
        let (mut reader, mut writer) = connect_test_client(addr).await;
        let address = test_miner_address(0x55);
        let miner_job = login_test_client(&mut reader, &mut writer, 1, &address, "rig-a").await;

        let nonce1 = nonce_with_parity(&miner_job, false, 0);
        let nonce2 = nonce_with_parity(&miner_job, false, 1);
        let nonce3 = nonce_with_parity(&miner_job, false, 2);
        let nonce4 = nonce_with_parity(&miner_job, false, 3);

        send_submit(
            &mut writer,
            2,
            &miner_job.job_id,
            nonce1,
            SleepyQueueHasher::claimed_hash_hex(nonce1),
        )
        .await;
        send_submit(
            &mut writer,
            3,
            &miner_job.job_id,
            nonce2,
            SleepyQueueHasher::claimed_hash_hex(nonce2),
        )
        .await;
        send_submit(
            &mut writer,
            4,
            &miner_job.job_id,
            nonce3,
            SleepyQueueHasher::claimed_hash_hex(nonce3),
        )
        .await;

        let (responses, _) = read_response_map(&mut reader, &[2, 3, 4]).await;
        let busy_count = responses
            .values()
            .filter(|message| {
                message["error"]
                    .as_str()
                    .is_some_and(|err| err.contains("server busy"))
            })
            .count();
        let ok_count = responses
            .values()
            .filter(|message| message["status"].as_str() == Some("ok"))
            .count();
        assert!(
            ok_count >= 1,
            "at least one submit should still be accepted"
        );
        assert!(
            busy_count >= 1,
            "at least one submit should fail fast once the regular submit queue is full"
        );

        send_submit(
            &mut writer,
            5,
            &miner_job.job_id,
            nonce4,
            SleepyQueueHasher::claimed_hash_hex(nonce4),
        )
        .await;
        let response = read_json_line(&mut reader).await;
        assert_eq!(response["id"].as_u64(), Some(5));
        assert_eq!(response["status"].as_str(), Some("ok"));

        drop(writer);
        drop(reader);
        shutdown_test_listener(listener_task, server, _jobs).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn tcp_candidate_submit_finishes_before_other_connections_regular_backlog() {
        let mut cfg = stratum_test_cfg();
        cfg.regular_submit_queue = 3;
        let (server, _jobs) = build_tcp_test_server(cfg).await;
        let (addr, listener_task) = start_test_tcp_listener(Arc::clone(&server)).await;
        let (mut reader_a, mut writer_a) = connect_test_client(addr).await;
        let (mut reader_b, mut writer_b) = connect_test_client(addr).await;

        let job_a = login_test_client(
            &mut reader_a,
            &mut writer_a,
            1,
            &test_miner_address(0x61),
            "rig-a",
        )
        .await;
        let job_b = login_test_client(
            &mut reader_b,
            &mut writer_b,
            2,
            &test_miner_address(0x62),
            "rig-b",
        )
        .await;

        let regular1 = nonce_with_parity(&job_a, false, 0);
        let regular2 = nonce_with_parity(&job_a, false, 1);
        let regular3 = nonce_with_parity(&job_a, false, 2);
        let candidate = nonce_with_parity(&job_b, true, 0);

        send_submit(
            &mut writer_a,
            10,
            &job_a.job_id,
            regular1,
            SleepyQueueHasher::claimed_hash_hex(regular1),
        )
        .await;
        send_submit(
            &mut writer_a,
            11,
            &job_a.job_id,
            regular2,
            SleepyQueueHasher::claimed_hash_hex(regular2),
        )
        .await;
        send_submit(
            &mut writer_a,
            12,
            &job_a.job_id,
            regular3,
            SleepyQueueHasher::claimed_hash_hex(regular3),
        )
        .await;
        send_submit(
            &mut writer_b,
            20,
            &job_b.job_id,
            candidate,
            SleepyQueueHasher::claimed_hash_hex(candidate),
        )
        .await;

        let mut responses = HashMap::new();
        let mut order = Vec::new();
        while responses.len() < 4 {
            tokio::select! {
                message = read_json_line(&mut reader_a) => {
                    if let Some(id) = message.get("id").and_then(Value::as_u64) {
                        order.push(id);
                        responses.insert(id, message);
                    }
                }
                message = read_json_line(&mut reader_b) => {
                    if let Some(id) = message.get("id").and_then(Value::as_u64) {
                        order.push(id);
                        responses.insert(id, message);
                    }
                }
            }
        }

        let candidate_pos = order
            .iter()
            .position(|id| *id == 20)
            .expect("candidate response should arrive");
        let queued_regular_pos = order
            .iter()
            .position(|id| *id == 12)
            .expect("queued regular response should arrive");
        assert!(
            candidate_pos < queued_regular_pos,
            "candidate submit should finish before the deeper queued regular submit"
        );
        assert_eq!(responses[&20]["status"].as_str(), Some("ok"));

        drop(writer_a);
        drop(reader_a);
        drop(writer_b);
        drop(reader_b);
        shutdown_test_listener(listener_task, server, _jobs).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn tcp_job_notifications_still_flow_while_submits_are_backed_up() {
        let cfg = stratum_test_cfg();
        let (server, jobs) = build_tcp_test_server(cfg).await;
        let (addr, listener_task) = start_test_tcp_listener(Arc::clone(&server)).await;
        let (mut reader, mut writer) = connect_test_client(addr).await;
        let address = test_miner_address(0x71);
        let miner_job = login_test_client(&mut reader, &mut writer, 1, &address, "rig-a").await;

        let nonce1 = nonce_with_parity(&miner_job, false, 0);
        let nonce2 = nonce_with_parity(&miner_job, false, 1);
        send_submit(
            &mut writer,
            2,
            &miner_job.job_id,
            nonce1,
            SleepyQueueHasher::claimed_hash_hex(nonce1),
        )
        .await;
        send_submit(
            &mut writer,
            3,
            &miner_job.job_id,
            nonce2,
            SleepyQueueHasher::claimed_hash_hex(nonce2),
        )
        .await;

        jobs.install_test_job(stratum_test_job("job-next", 101));

        let message = tokio::time::timeout(Duration::from_millis(150), read_json_line(&mut reader))
            .await
            .expect("new job should arrive before the queued submit finishes");
        assert_eq!(message["method"].as_str(), Some("job"));
        assert_eq!(message["params"]["height"].as_u64(), Some(101));
        assert_eq!(message["params"]["template_id"].as_str(), Some("tmpl-101"));

        drop(writer);
        drop(reader);
        shutdown_test_listener(listener_task, server, jobs).await;
    }
}
