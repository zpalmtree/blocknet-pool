package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

func TestRefreshTemplateLeavesTemplateIDEmptyWhenDaemonOmitsIt(t *testing.T) {
	const rewardAddr = "SEiNEtestRewardAddr"

	templateBlock := fmt.Sprintf(
		`{"header":{"height":42,"difficulty":7},"transactions":[{"type":"coinbase","outputs":[{"address":"%s","amount":50}]}]}`,
		rewardAddr,
	)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/wallet/address":
			io.WriteString(w, `{"address":"`+rewardAddr+`","view_only":false}`)
		case "/api/mining/blocktemplate":
			io.WriteString(w, `{"block":`+templateBlock+`,"target":"`+strings.Repeat("ff", 32)+`","header_base":"`+strings.Repeat("00", 92)+`","reward_address_used":"`+rewardAddr+`"}`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	cfg := DefaultConfig()
	cfg.DaemonAPI = srv.URL
	node := NewNodeClient(cfg.DaemonAPI, "")
	jm := NewJobManager(node, cfg)

	jm.refreshTemplate()
	job := jm.CurrentJob()
	if job == nil {
		t.Fatal("expected current job to be populated")
	}
	if job.TemplateID != "" {
		t.Fatalf("expected empty template_id when daemon omits it, got %q", job.TemplateID)
	}
}

func TestSubmitBlockSkipsCompactWhenTemplateIDEmpty(t *testing.T) {
	var calls atomic.Int32
	var gotBody map[string]any

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/mining/submitblock" {
			http.NotFound(w, r)
			return
		}
		calls.Add(1)
		body, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(body, &gotBody)
		io.WriteString(w, `{"accepted":true,"hash":"abc","height":10}`)
	}))
	defer srv.Close()

	cfg := DefaultConfig()
	node := NewNodeClient(srv.URL, "")
	jm := NewJobManager(node, cfg)

	job := &Job{
		ID:         "job-1",
		TemplateID: "",
		Block:      json.RawMessage(`{"header":{"nonce":0},"transactions":[]}`),
	}

	result, err := jm.SubmitBlock(job, 77)
	if err != nil {
		t.Fatalf("submit block: %v", err)
	}
	if !result.Accepted {
		t.Fatal("expected submit to be accepted")
	}
	if calls.Load() != 1 {
		t.Fatalf("expected 1 submit request, got %d", calls.Load())
	}
	if _, ok := gotBody["template_id"]; ok {
		t.Fatalf("expected full payload submit without template_id, got body=%v", gotBody)
	}

	header, ok := gotBody["header"].(map[string]any)
	if !ok {
		t.Fatalf("expected header object in submit payload, got body=%v", gotBody)
	}
	if nonce, ok := header["nonce"].(float64); !ok || uint64(nonce) != 77 {
		t.Fatalf("expected header.nonce=77, got %v", header["nonce"])
	}
}

func TestSubmitBlockUsesCompactWhenTemplateIDPresent(t *testing.T) {
	var calls atomic.Int32
	var gotBody map[string]any

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/mining/submitblock" {
			http.NotFound(w, r)
			return
		}
		calls.Add(1)
		body, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(body, &gotBody)
		io.WriteString(w, `{"accepted":true,"hash":"abc","height":11}`)
	}))
	defer srv.Close()

	cfg := DefaultConfig()
	node := NewNodeClient(srv.URL, "")
	jm := NewJobManager(node, cfg)

	job := &Job{
		ID:         "job-1",
		TemplateID: "tmpl-123",
		Block:      json.RawMessage(`{"header":{"nonce":0},"transactions":[]}`),
	}

	result, err := jm.SubmitBlock(job, 88)
	if err != nil {
		t.Fatalf("submit block: %v", err)
	}
	if !result.Accepted {
		t.Fatal("expected submit to be accepted")
	}
	if calls.Load() != 1 {
		t.Fatalf("expected 1 submit request, got %d", calls.Load())
	}
	if gotBody["template_id"] != "tmpl-123" {
		t.Fatalf("expected compact submit template_id=tmpl-123, got body=%v", gotBody)
	}
	if nonce, ok := gotBody["nonce"].(float64); !ok || uint64(nonce) != 88 {
		t.Fatalf("expected compact submit nonce=88, got %v", gotBody["nonce"])
	}
}

func TestSubmitBlockFallsBackToFullPayloadAfterCompactFailure(t *testing.T) {
	var calls atomic.Int32
	var firstBody map[string]any
	var secondBody map[string]any

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/mining/submitblock" {
			http.NotFound(w, r)
			return
		}
		body, _ := io.ReadAll(r.Body)
		cur := calls.Add(1)
		switch cur {
		case 1:
			_ = json.Unmarshal(body, &firstBody)
			http.Error(w, `{"error":"unsupported compact submit"}`, http.StatusBadRequest)
		case 2:
			_ = json.Unmarshal(body, &secondBody)
			io.WriteString(w, `{"accepted":true,"hash":"abc","height":12}`)
		default:
			t.Fatalf("unexpected extra submit call #%d", cur)
		}
	}))
	defer srv.Close()

	cfg := DefaultConfig()
	node := NewNodeClient(srv.URL, "")
	jm := NewJobManager(node, cfg)

	job := &Job{
		ID:         "job-1",
		TemplateID: "tmpl-legacy-fallback",
		Block:      json.RawMessage(`{"header":{"nonce":0},"transactions":[]}`),
	}

	result, err := jm.SubmitBlock(job, 99)
	if err != nil {
		t.Fatalf("submit block with compact fallback: %v", err)
	}
	if !result.Accepted {
		t.Fatal("expected fallback submit to be accepted")
	}
	if calls.Load() != 2 {
		t.Fatalf("expected 2 submit requests (compact+fallback), got %d", calls.Load())
	}
	if firstBody["template_id"] != "tmpl-legacy-fallback" {
		t.Fatalf("expected first request to be compact, got body=%v", firstBody)
	}
	if _, ok := secondBody["template_id"]; ok {
		t.Fatalf("expected second request to be full payload without template_id, got body=%v", secondBody)
	}
}

func TestResolveRewardAddressAutoLoadsAndUnlocksWallet(t *testing.T) {
	const rewardAddr = "addr-auto-wallet-ready"

	var loadCalls atomic.Int32
	var unlockCalls atomic.Int32
	var addressCalls atomic.Int32

	var walletLoaded atomic.Bool
	var walletUnlocked atomic.Bool

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/wallet/address":
			addressCalls.Add(1)
			if !walletLoaded.Load() {
				http.Error(w, `{"error":"no wallet loaded"}`, http.StatusServiceUnavailable)
				return
			}
			if !walletUnlocked.Load() {
				http.Error(w, `{"error":"wallet is locked"}`, http.StatusForbidden)
				return
			}
			io.WriteString(w, `{"address":"`+rewardAddr+`","view_only":false}`)
		case "/api/wallet/load":
			loadCalls.Add(1)
			walletLoaded.Store(true)
			io.WriteString(w, `{"loaded":true,"address":"`+rewardAddr+`"}`)
		case "/api/wallet/unlock":
			unlockCalls.Add(1)
			if !walletLoaded.Load() {
				http.Error(w, `{"error":"no wallet loaded"}`, http.StatusServiceUnavailable)
				return
			}
			walletUnlocked.Store(true)
			io.WriteString(w, `{"locked":false}`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	t.Setenv(walletPasswordEnv, "test-password")

	cfg := DefaultConfig()
	node := NewNodeClient(srv.URL, "")
	jm := NewJobManager(node, cfg)

	addr := jm.resolveRewardAddress()
	if addr != rewardAddr {
		t.Fatalf("expected recovered reward address %q, got %q", rewardAddr, addr)
	}
	if loadCalls.Load() != 1 {
		t.Fatalf("expected wallet load call, got %d", loadCalls.Load())
	}
	if unlockCalls.Load() != 1 {
		t.Fatalf("expected wallet unlock call, got %d", unlockCalls.Load())
	}
	if addressCalls.Load() < 2 {
		t.Fatalf("expected wallet address to be retried after recovery, got %d calls", addressCalls.Load())
	}
}
