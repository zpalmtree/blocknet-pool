package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestConfigNormalizeClampsInitialShareDifficulty(t *testing.T) {
	cfg := DefaultConfig()
	cfg.InitialShareDifficulty = 0

	cfg.normalize()

	if cfg.InitialShareDifficulty != 1 {
		t.Fatalf("expected initial share difficulty to clamp to 1, got %d", cfg.InitialShareDifficulty)
	}
}

func TestGenerateDefaultEnv(t *testing.T) {
	envPath := filepath.Join(t.TempDir(), ".env")

	created, err := GenerateDefaultEnv(envPath)
	if err != nil {
		t.Fatalf("generate env: %v", err)
	}
	if !created {
		t.Fatal("expected .env to be created")
	}

	raw, err := os.ReadFile(envPath)
	if err != nil {
		t.Fatalf("read env: %v", err)
	}
	text := string(raw)
	if !strings.Contains(text, "BLOCKNET_WALLET_PASSWORD=CHANGE_ME") {
		t.Fatalf("expected placeholder password in env template, got: %q", text)
	}

	created, err = GenerateDefaultEnv(envPath)
	if err != nil {
		t.Fatalf("regenerate env: %v", err)
	}
	if created {
		t.Fatal("expected existing .env to be preserved")
	}
}
