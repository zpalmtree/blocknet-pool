package main

import "testing"

func TestConfigNormalizeClampsInitialShareDifficulty(t *testing.T) {
	cfg := DefaultConfig()
	cfg.InitialShareDifficulty = 0

	cfg.normalize()

	if cfg.InitialShareDifficulty != 1 {
		t.Fatalf("expected initial share difficulty to clamp to 1, got %d", cfg.InitialShareDifficulty)
	}
}
