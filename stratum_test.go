package main

import "testing"

func TestShouldInlineValidationOnQueueFullLegacySubmit(t *testing.T) {
	if !shouldInlineValidationOnQueueFull(false, false) {
		t.Fatal("expected legacy submit (no claimed hash) to inline on queue full")
	}
}

func TestShouldInlineValidationOnQueueFullCandidateHint(t *testing.T) {
	if !shouldInlineValidationOnQueueFull(true, true) {
		t.Fatal("expected candidate hint to inline on queue full")
	}
}

func TestShouldInlineValidationOnQueueFullRegularV2(t *testing.T) {
	if shouldInlineValidationOnQueueFull(true, false) {
		t.Fatal("expected regular v2 share to reject on queue full")
	}
}

func TestNormalizeStratumProtocolVersion(t *testing.T) {
	tests := []struct {
		name     string
		input    uint32
		expected uint32
	}{
		{name: "missing", input: 0, expected: StratumProtocolVersionMin},
		{name: "legacy", input: 1, expected: 1},
		{name: "current", input: 2, expected: 2},
		{name: "future", input: 99, expected: StratumProtocolVersionCurrent},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := normalizeStratumProtocolVersion(tc.input); got != tc.expected {
				t.Fatalf("expected %d, got %d", tc.expected, got)
			}
		})
	}
}

func TestNormalizeCapabilityList(t *testing.T) {
	caps := normalizeCapabilityList([]string{
		" submit_claimed_hash ",
		"SHARE_VALIDATION_STATUS",
		"",
		"submit_claimed_hash",
	})
	if len(caps) != 2 {
		t.Fatalf("expected 2 unique capabilities, got %d", len(caps))
	}
	if caps[0] != StratumCapabilitySubmitClaimedHash {
		t.Fatalf("expected first capability %q, got %q", StratumCapabilitySubmitClaimedHash, caps[0])
	}
	if caps[1] != StratumCapabilityValidationStatus {
		t.Fatalf("expected second capability %q, got %q", StratumCapabilityValidationStatus, caps[1])
	}
}

func TestBuildLoginResultProtocolV1(t *testing.T) {
	result := buildLoginResult(&Config{}, 1)
	if result.ProtocolVersion != 1 {
		t.Fatalf("expected protocol version 1, got %d", result.ProtocolVersion)
	}
	for _, capability := range result.Capabilities {
		if capability == StratumCapabilitySubmitClaimedHash {
			t.Fatalf("did not expect %q for protocol v1", StratumCapabilitySubmitClaimedHash)
		}
	}
	if len(result.RequiredCapabilities) != 0 {
		t.Fatalf("expected no required capabilities, got %v", result.RequiredCapabilities)
	}
}

func TestBuildLoginResultProtocolV2Required(t *testing.T) {
	cfg := &Config{StratumSubmitV2Required: true}
	result := buildLoginResult(cfg, 2)
	if result.ProtocolVersion != 2 {
		t.Fatalf("expected protocol version 2, got %d", result.ProtocolVersion)
	}
	hasClaimedHash := false
	for _, capability := range result.Capabilities {
		if capability == StratumCapabilitySubmitClaimedHash {
			hasClaimedHash = true
			break
		}
	}
	if !hasClaimedHash {
		t.Fatalf("expected %q capability in login result", StratumCapabilitySubmitClaimedHash)
	}
	if len(result.RequiredCapabilities) != 1 || result.RequiredCapabilities[0] != StratumCapabilitySubmitClaimedHash {
		t.Fatalf("unexpected required capabilities: %v", result.RequiredCapabilities)
	}
}
