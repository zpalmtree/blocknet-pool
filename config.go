package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"
)

type Config struct {
	// Pool identity
	PoolName string `json:"pool_name"`
	PoolURL  string `json:"pool_url"`

	// Network
	StratumPort int    `json:"stratum_port"`
	APIPort     int    `json:"api_port"`
	APIHost     string `json:"api_host"`

	// Blocknet daemon
	DaemonBinary      string `json:"daemon_binary"`
	DaemonDataDir     string `json:"daemon_data_dir"` // node data dir containing api.cookie
	DaemonAPI         string `json:"daemon_api"`
	DaemonToken       string `json:"daemon_token"`
	PoolWalletAddress string `json:"pool_wallet_address"` // deprecated: runtime wallet address is sourced from daemon API

	// Mining
	InitialShareDifficulty  uint64  `json:"initial_share_difficulty"`
	BlockPollInterval       string  `json:"block_poll_interval"`
	JobTimeout              string  `json:"job_timeout"`
	ValidationMode          string  `json:"validation_mode"` // "full" or "probabilistic"
	MaxVerifiers            int     `json:"max_verifiers"`   // max concurrent expensive PoW verifications
	MaxValidationQueue      int     `json:"max_validation_queue"`
	SampleRate              float64 `json:"sample_rate"` // percentage [0,1] for post-warmup sampling
	WarmupShares            int     `json:"warmup_shares"`
	MinSampleEvery          int     `json:"min_sample_every"`         // force one full verify every N shares
	InvalidSampleThreshold  float64 `json:"invalid_sample_threshold"` // ratio [0,1]
	InvalidSampleMin        int     `json:"invalid_sample_min"`       // minimum sampled shares before threshold enforcement
	ForcedVerifyDuration    string  `json:"forced_verify_duration"`   // e.g. "24h"
	QuarantineDuration      string  `json:"quarantine_duration"`      // base quarantine duration for risk escalation
	MaxQuarantineDuration   string  `json:"max_quarantine_duration"`  // cap for escalated quarantine duration
	ProvisionalShareDelay   string  `json:"provisional_share_delay"`  // e.g. "15m"
	MaxProvisionalShares    int     `json:"max_provisional_shares"`   // per-address cap for outstanding provisional shares
	StratumSubmitV2Required bool    `json:"stratum_submit_v2_required"`

	// Pool fee
	PoolFeeFlat float64 `json:"pool_fee_flat"` // flat fee in BNT per block (e.g. 20.0 = 20 BNT)
	PoolFeePct  float64 `json:"pool_fee_pct"`  // percentage fee per block (e.g. 5.0 = 5%)

	// Payouts
	PayoutScheme        string  `json:"payout_scheme"`         // "pplns" or "proportional"
	PPLNSWindow         int     `json:"pplns_window"`          // number of shares to look back (deprecated - use PPLNSWindowDuration)
	PPLNSWindowDuration string  `json:"pplns_window_duration"` // time window for PPLNS (e.g. "24h")
	BlocksBeforePayout  int     `json:"blocks_before_payout"`
	MinPayoutAmount     float64 `json:"min_payout_amount"`
	BlockFinderBonus    bool    `json:"block_finder_bonus"`
	BlockFinderBonusPct float64 `json:"block_finder_bonus_pct"`
	PayoutInterval      string  `json:"payout_interval"`

	// Storage
	DatabasePath string `json:"database_path"`

	// Frontend
	TemplatePath string `json:"template_path"`
	StaticPath   string `json:"static_path"`

	// API Security
	APIKey string `json:"api_key"` // optional API key for protected endpoints (leave empty to disable)

	// Runtime-only fields (not loaded from JSON).
	LogPath string `json:"-"`
}

func DefaultConfig() *Config {
	return &Config{
		PoolName:                "blocknet pool",
		PoolURL:                 "http://localhost:8080",
		StratumPort:             3333,
		APIPort:                 8080,
		APIHost:                 "0.0.0.0",
		DaemonBinary:            "./blocknet",
		DaemonDataDir:           "data",
		DaemonAPI:               "http://127.0.0.1:8332",
		DaemonToken:             "",
		PoolWalletAddress:       "", // deprecated; kept for backward compatibility
		InitialShareDifficulty:  1,
		BlockPollInterval:       "2s",
		JobTimeout:              "5m",
		ValidationMode:          "probabilistic",
		MaxVerifiers:            2,
		MaxValidationQueue:      2048,
		SampleRate:              0.05,
		WarmupShares:            50,
		MinSampleEvery:          20,
		InvalidSampleThreshold:  0.01,
		InvalidSampleMin:        50,
		ForcedVerifyDuration:    "24h",
		QuarantineDuration:      "1h",
		MaxQuarantineDuration:   "168h",
		ProvisionalShareDelay:   "15m",
		MaxProvisionalShares:    200,
		StratumSubmitV2Required: false,
		PoolFeeFlat:             0,
		PoolFeePct:              0,
		PayoutScheme:            "pplns",
		PPLNSWindow:             1000,
		PPLNSWindowDuration:     "24h",
		BlocksBeforePayout:      120,
		MinPayoutAmount:         0.1,
		BlockFinderBonus:        false,
		BlockFinderBonusPct:     5.0,
		PayoutInterval:          "1h",
		DatabasePath:            "pool.db",
		TemplatePath:            "templates",
		StaticPath:              "static",
		APIKey:                  "", // optional - generate with: openssl rand -hex 32
	}
}

func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	cfg := DefaultConfig()
	if err := json.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	cfg.normalize()
	return cfg, nil
}

func (c *Config) normalize() {
	mode := strings.ToLower(strings.TrimSpace(c.ValidationMode))
	if mode != "full" && mode != "probabilistic" {
		mode = "probabilistic"
	}
	c.ValidationMode = mode

	if c.MaxVerifiers < 0 {
		c.MaxVerifiers = 0
	}
	if c.MaxValidationQueue < 1 {
		c.MaxValidationQueue = 2048
	}
	if c.SampleRate < 0 {
		c.SampleRate = 0
	}
	if c.SampleRate > 1 {
		c.SampleRate = 1
	}
	if c.WarmupShares < 0 {
		c.WarmupShares = 0
	}
	if c.MinSampleEvery < 0 {
		c.MinSampleEvery = 0
	}
	if c.InvalidSampleMin < 1 {
		c.InvalidSampleMin = 1
	}
	if c.InvalidSampleThreshold <= 0 || c.InvalidSampleThreshold > 1 {
		c.InvalidSampleThreshold = 0.01
	}
	if c.MaxProvisionalShares < 0 {
		c.MaxProvisionalShares = 0
	}
	if c.InitialShareDifficulty < 1 {
		c.InitialShareDifficulty = 1
	}
}

func (c *Config) BlockPollDuration() time.Duration {
	d, err := time.ParseDuration(c.BlockPollInterval)
	if err != nil {
		return 2 * time.Second
	}
	return d
}

func (c *Config) JobTimeoutDuration() time.Duration {
	d, err := time.ParseDuration(c.JobTimeout)
	if err != nil {
		return 5 * time.Minute
	}
	return d
}

func (c *Config) ForcedVerifyDurationDur() time.Duration {
	d, err := time.ParseDuration(c.ForcedVerifyDuration)
	if err != nil {
		return 24 * time.Hour
	}
	return d
}

func (c *Config) ProvisionalShareDelayDur() time.Duration {
	d, err := time.ParseDuration(c.ProvisionalShareDelay)
	if err != nil {
		return 15 * time.Minute
	}
	return d
}

func (c *Config) QuarantineDurationDur() time.Duration {
	d, err := time.ParseDuration(c.QuarantineDuration)
	if err != nil {
		return 1 * time.Hour
	}
	return d
}

func (c *Config) MaxQuarantineDurationDur() time.Duration {
	d, err := time.ParseDuration(c.MaxQuarantineDuration)
	if err != nil {
		return 168 * time.Hour
	}
	return d
}

func (c *Config) PayoutIntervalDuration() time.Duration {
	d, err := time.ParseDuration(c.PayoutInterval)
	if err != nil {
		return 1 * time.Hour
	}
	return d
}

func (c *Config) PPLNSWindowDur() time.Duration {
	if c.PPLNSWindowDuration == "" {
		// Fallback to old share-count based system if duration not set
		return 0
	}
	d, err := time.ParseDuration(c.PPLNSWindowDuration)
	if err != nil {
		return 24 * time.Hour
	}
	return d
}

// PoolFee returns the total pool fee in atomic units for a given block reward.
// Flat fee is applied first, then percentage on the remainder.
func (c *Config) PoolFee(reward uint64) uint64 {
	var fee uint64

	// Flat fee (BNT -> atomic: multiply by 1e8)
	if c.PoolFeeFlat > 0 {
		flat := uint64(c.PoolFeeFlat * 100_000_000)
		if flat > reward {
			return reward
		}
		fee += flat
	}

	// Percentage fee on the remainder
	if c.PoolFeePct > 0 {
		remainder := reward - fee
		pct := uint64(float64(remainder) * c.PoolFeePct / 100.0)
		fee += pct
	}

	if fee > reward {
		return reward
	}
	return fee
}

func GenerateDefaultConfig(path string) error {
	cfg := DefaultConfig()
	data, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0644)
}

func GenerateDefaultEnv(path string) (bool, error) {
	if _, err := os.Stat(path); err == nil {
		return false, nil
	} else if !os.IsNotExist(err) {
		return false, err
	}

	content := strings.Join([]string{
		"# Blocknet Pool environment",
		"# REQUIRED: set your daemon wallet password before starting the pool.",
		"BLOCKNET_WALLET_PASSWORD=CHANGE_ME",
		"",
	}, "\n")

	if err := os.WriteFile(path, []byte(content), 0600); err != nil {
		return false, err
	}
	return true, nil
}
