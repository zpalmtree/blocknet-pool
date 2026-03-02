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
