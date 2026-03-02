package main

import "testing"

func testValidationConfig() *Config {
	cfg := DefaultConfig()
	cfg.ValidationMode = "probabilistic"
	cfg.SampleRate = 0
	cfg.WarmupShares = 0
	cfg.MinSampleEvery = 0
	cfg.InvalidSampleThreshold = 0.2
	cfg.InvalidSampleMin = 3
	cfg.ForcedVerifyDuration = "1h"
	cfg.ProvisionalShareDelay = "1h"
	cfg.MaxProvisionalShares = 2
	return cfg
}

func TestValidationEngineInvalidSampleForcesFullVerify(t *testing.T) {
	ve := NewValidationEngine(testValidationConfig())
	address := "addr-invalid-threshold"

	ve.updateAddressState(address, true, false, false, false)
	ve.updateAddressState(address, true, true, false, false)
	ve.updateAddressState(address, true, false, false, false)

	if !ve.shouldFullyVerify(address) {
		t.Fatalf("expected address %q to be forced into full verification", address)
	}
}

func TestValidationEngineProvisionalCapForcesFullVerify(t *testing.T) {
	ve := NewValidationEngine(testValidationConfig())
	address := "addr-provisional-cap"

	ve.updateAddressState(address, false, false, false, true)
	ve.updateAddressState(address, false, false, false, true)

	if !ve.shouldFullyVerify(address) {
		t.Fatalf("expected address %q to be forced after provisional cap", address)
	}
}

func TestValidationSnapshotIncludesPendingProvisional(t *testing.T) {
	ve := NewValidationEngine(testValidationConfig())
	address := "addr-snapshot"

	ve.updateAddressState(address, false, false, false, true)
	ve.updateAddressState(address, true, false, false, false)

	snap := ve.Snapshot()
	if snap.PendingProvisional == 0 {
		t.Fatal("expected pending provisional shares in snapshot")
	}
	if snap.TotalShares < 2 {
		t.Fatalf("expected at least 2 total shares in snapshot, got %d", snap.TotalShares)
	}
}
