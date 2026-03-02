package main

import (
	"path/filepath"
	"testing"
	"time"
)

func TestStoreAddressRiskEscalationAndQueries(t *testing.T) {
	storePath := filepath.Join(t.TempDir(), "pool-risk.db")
	store, err := OpenStore(storePath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer store.Close()

	addr := "test-address-risk"
	base := 1 * time.Second
	maxDur := 3 * time.Second
	force := 5 * time.Second

	state, err := store.EscalateAddressRisk(addr, "invalid share proof", base, maxDur, force, true)
	if err != nil {
		t.Fatalf("escalate risk #1: %v", err)
	}
	if state.Strikes != 1 {
		t.Fatalf("expected strikes=1, got %d", state.Strikes)
	}

	quarantined, state2, err := store.IsAddressQuarantined(addr)
	if err != nil {
		t.Fatalf("is quarantined: %v", err)
	}
	if !quarantined {
		t.Fatalf("expected address %q to be quarantined", addr)
	}
	if state2 == nil {
		t.Fatalf("expected risk state for %q", addr)
	}

	forced, _, err := store.ShouldForceVerifyAddress(addr)
	if err != nil {
		t.Fatalf("should force verify: %v", err)
	}
	if !forced {
		t.Fatalf("expected address %q to be force-verified", addr)
	}

	state, err = store.EscalateAddressRisk(addr, "repeat offense", base, maxDur, force, true)
	if err != nil {
		t.Fatalf("escalate risk #2: %v", err)
	}
	if state.Strikes != 2 {
		t.Fatalf("expected strikes=2, got %d", state.Strikes)
	}

	qCount, fCount, err := store.GetRiskSummary()
	if err != nil {
		t.Fatalf("risk summary: %v", err)
	}
	if qCount < 1 {
		t.Fatalf("expected at least one quarantined address, got %d", qCount)
	}
	if fCount < 1 {
		t.Fatalf("expected at least one forced-verify address, got %d", fCount)
	}
}

func TestStoreGetAddressRiskMissingReturnsNil(t *testing.T) {
	storePath := filepath.Join(t.TempDir(), "pool-risk-empty.db")
	store, err := OpenStore(storePath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer store.Close()

	addr := "missing-risk-address"

	state, err := store.GetAddressRisk(addr)
	if err != nil {
		t.Fatalf("get address risk: %v", err)
	}
	if state != nil {
		t.Fatalf("expected nil risk state for missing address, got %+v", *state)
	}

	quarantined, qState, err := store.IsAddressQuarantined(addr)
	if err != nil {
		t.Fatalf("is quarantined: %v", err)
	}
	if quarantined {
		t.Fatalf("expected address %q to not be quarantined", addr)
	}
	if qState != nil {
		t.Fatalf("expected nil quarantine state for missing address, got %+v", *qState)
	}

	forced, fState, err := store.ShouldForceVerifyAddress(addr)
	if err != nil {
		t.Fatalf("should force verify: %v", err)
	}
	if forced {
		t.Fatalf("expected address %q to not be forced", addr)
	}
	if fState != nil {
		t.Fatalf("expected nil force-verify state for missing address, got %+v", *fState)
	}
}
