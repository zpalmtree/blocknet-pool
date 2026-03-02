package main

import (
	"context"
	"math/rand"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	ShareStatusVerified    = "verified"
	ShareStatusProvisional = "provisional"
)

type ValidationTask struct {
	Address         string
	Nonce           uint64
	HeaderBase      []byte
	ShareTarget     [32]byte
	NetworkTarget   [32]byte
	ClaimedHash     [32]byte
	HasClaimedHash  bool
	ForceFullVerify bool
	ResultCh        chan ValidationResult
}

type ValidationResult struct {
	Accepted         bool
	RejectReason     string
	Hash             [32]byte
	Verified         bool
	IsBlockCandidate bool
	SuspectedFraud   bool
	EscalateRisk     bool
}

type validationAddressState struct {
	totalShares    uint64
	sampledShares  uint64
	invalidSamples uint64
	forcedUntil    time.Time
	provisionalAt  []time.Time
}

type ValidationEngine struct {
	config *Config

	workerCount int
	regularQ    chan *ValidationTask
	candidateQ  chan *ValidationTask

	stateMu sync.Mutex
	states  map[string]*validationAddressState

	rngMu sync.Mutex
	rng   *rand.Rand

	inflight atomic.Int64
	fraud    atomic.Uint64
}

type ValidationSnapshot struct {
	InFlight              int64  `json:"in_flight"`
	CandidateQueueDepth   int    `json:"candidate_queue_depth"`
	RegularQueueDepth     int    `json:"regular_queue_depth"`
	TrackedAddresses      int    `json:"tracked_addresses"`
	ForcedVerifyAddresses int    `json:"forced_verify_addresses"`
	TotalShares           uint64 `json:"total_shares"`
	SampledShares         uint64 `json:"sampled_shares"`
	InvalidSamples        uint64 `json:"invalid_samples"`
	PendingProvisional    uint64 `json:"pending_provisional"`
	FraudDetections       uint64 `json:"fraud_detections"`
}

func NewValidationEngine(config *Config) *ValidationEngine {
	workerCount := config.MaxVerifiers
	if workerCount <= 0 {
		workerCount = runtime.NumCPU() / 2
		if workerCount < 1 {
			workerCount = 1
		}
	}

	queueSize := config.MaxValidationQueue
	if queueSize <= 0 {
		queueSize = 2048
	}

	return &ValidationEngine{
		config:      config,
		workerCount: workerCount,
		regularQ:    make(chan *ValidationTask, queueSize),
		candidateQ:  make(chan *ValidationTask, queueSize),
		states:      make(map[string]*validationAddressState),
		rng:         rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

func (ve *ValidationEngine) Start(ctx context.Context) {
	for i := 0; i < ve.workerCount; i++ {
		go ve.worker(ctx)
	}
}

func (ve *ValidationEngine) Submit(task *ValidationTask, candidate bool) bool {
	if candidate {
		select {
		case ve.candidateQ <- task:
			return true
		default:
			return false
		}
	}

	select {
	case ve.regularQ <- task:
		return true
	default:
		return false
	}
}

func (ve *ValidationEngine) worker(ctx context.Context) {
	for {
		// Candidate shares are always served first.
		select {
		case <-ctx.Done():
			return
		case task := <-ve.candidateQ:
			ve.process(task)
			continue
		default:
		}

		select {
		case <-ctx.Done():
			return
		case task := <-ve.candidateQ:
			ve.process(task)
		case task := <-ve.regularQ:
			ve.process(task)
		}
	}
}

func (ve *ValidationEngine) process(task *ValidationTask) {
	ve.inflight.Add(1)
	defer ve.inflight.Add(-1)

	fullVerify := task.ForceFullVerify || ve.shouldFullyVerify(task.Address)
	var result ValidationResult
	result.Verified = fullVerify

	if fullVerify {
		hash := PowHash(task.HeaderBase, task.Nonce)
		result.Hash = hash

		if task.HasClaimedHash && task.ClaimedHash != hash {
			result.RejectReason = "invalid share proof"
			result.SuspectedFraud = true
			ve.fraud.Add(1)
		} else if !CheckTarget(hash, task.ShareTarget) {
			result.RejectReason = "low difficulty share"
		} else {
			result.Accepted = true
			result.IsBlockCandidate = CheckTarget(hash, task.NetworkTarget)
		}
	} else {
		if !task.HasClaimedHash {
			result.RejectReason = "claimed hash required"
		} else if !CheckTarget(task.ClaimedHash, task.ShareTarget) {
			result.RejectReason = "low difficulty share"
		} else {
			result.Accepted = true
			result.Hash = task.ClaimedHash
			result.IsBlockCandidate = CheckTarget(task.ClaimedHash, task.NetworkTarget)
		}
	}

	invalidSample := fullVerify && !result.Accepted
	provisionalAccepted := result.Accepted && !fullVerify
	if ve.updateAddressState(task.Address, fullVerify, invalidSample, result.SuspectedFraud, provisionalAccepted) {
		result.EscalateRisk = true
	}

	task.ResultCh <- result
}

func (ve *ValidationEngine) shouldFullyVerify(address string) bool {
	if strings.EqualFold(ve.config.ValidationMode, "full") {
		return true
	}

	now := time.Now()
	var totalShares uint64
	var forcedUntil time.Time
	ve.stateMu.Lock()
	st := ve.states[address]
	if st == nil {
		st = &validationAddressState{}
		ve.states[address] = st
	}
	totalShares = st.totalShares
	ve.pruneProvisionalLocked(st, now)
	maxProvisional := ve.config.MaxProvisionalShares
	if maxProvisional > 0 && len(st.provisionalAt) >= maxProvisional {
		st.forcedUntil = now.Add(ve.config.ProvisionalShareDelayDur())
	}
	forcedUntil = st.forcedUntil
	ve.stateMu.Unlock()

	if now.Before(forcedUntil) {
		return true
	}

	nextShareIdx := totalShares + 1
	if ve.config.WarmupShares > 0 && int(nextShareIdx) <= ve.config.WarmupShares {
		return true
	}
	if ve.config.MinSampleEvery > 0 && nextShareIdx%uint64(ve.config.MinSampleEvery) == 0 {
		return true
	}

	rate := ve.config.SampleRate
	if rate <= 0 {
		return false
	}
	if rate >= 1 {
		return true
	}

	ve.rngMu.Lock()
	v := ve.rng.Float64()
	ve.rngMu.Unlock()
	return v < rate
}

func (ve *ValidationEngine) updateAddressState(address string, sampled, invalidSample, suspectedFraud, provisionalAccepted bool) bool {
	ve.stateMu.Lock()
	defer ve.stateMu.Unlock()

	st := ve.states[address]
	if st == nil {
		st = &validationAddressState{}
		ve.states[address] = st
	}

	st.totalShares++
	now := time.Now()
	ve.pruneProvisionalLocked(st, now)
	if provisionalAccepted {
		st.provisionalAt = append(st.provisionalAt, now)
	}
	if sampled {
		st.sampledShares++
		if invalidSample {
			st.invalidSamples++
		}
	}

	if suspectedFraud {
		st.forcedUntil = now.Add(ve.config.ForcedVerifyDurationDur())
		return true
	}

	minSamples := ve.config.InvalidSampleMin
	if minSamples < 1 {
		minSamples = 1
	}
	if st.sampledShares < uint64(minSamples) {
		return false
	}
	invalidRatio := float64(st.invalidSamples) / float64(st.sampledShares)
	if invalidRatio > ve.config.InvalidSampleThreshold {
		st.forcedUntil = now.Add(ve.config.ForcedVerifyDurationDur())
		return true
	}
	return false
}

func (ve *ValidationEngine) QueueDepths() (candidate, regular int) {
	return len(ve.candidateQ), len(ve.regularQ)
}

func (ve *ValidationEngine) ProcessInline(task *ValidationTask) ValidationResult {
	ch := make(chan ValidationResult, 1)
	task.ResultCh = ch
	ve.process(task)
	return <-ch
}

func (ve *ValidationEngine) Snapshot() ValidationSnapshot {
	snap := ValidationSnapshot{
		InFlight:            ve.inflight.Load(),
		CandidateQueueDepth: len(ve.candidateQ),
		RegularQueueDepth:   len(ve.regularQ),
		FraudDetections:     ve.fraud.Load(),
	}

	now := time.Now()
	ve.stateMu.Lock()
	snap.TrackedAddresses = len(ve.states)
	for _, st := range ve.states {
		ve.pruneProvisionalLocked(st, now)
		snap.TotalShares += st.totalShares
		snap.SampledShares += st.sampledShares
		snap.InvalidSamples += st.invalidSamples
		snap.PendingProvisional += uint64(len(st.provisionalAt))
		if now.Before(st.forcedUntil) {
			snap.ForcedVerifyAddresses++
		}
	}
	ve.stateMu.Unlock()

	return snap
}

func (ve *ValidationEngine) pruneProvisionalLocked(st *validationAddressState, now time.Time) {
	if st == nil || len(st.provisionalAt) == 0 {
		return
	}
	delay := ve.config.ProvisionalShareDelayDur()
	if delay <= 0 {
		st.provisionalAt = st.provisionalAt[:0]
		return
	}
	cutoff := now.Add(-delay)
	idx := 0
	for idx < len(st.provisionalAt) && !st.provisionalAt[idx].After(cutoff) {
		idx++
	}
	if idx > 0 {
		st.provisionalAt = append(st.provisionalAt[:0], st.provisionalAt[idx:]...)
	}
}
