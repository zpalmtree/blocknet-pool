package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/joho/godotenv"
)

func main() {
	log.SetFlags(log.Ltime | log.Lshortfile)

	configPath := "config.json"
	if len(os.Args) > 1 {
		switch os.Args[1] {
		case "init":
			if err := GenerateDefaultConfig(configPath); err != nil {
				log.Fatalf("failed to generate config: %v", err)
			}
			envPath := filepath.Join(filepath.Dir(configPath), ".env")
			createdEnv, err := GenerateDefaultEnv(envPath)
			if err != nil {
				log.Fatalf("failed to generate env file: %v", err)
			}
			fmt.Printf("wrote %s\n", configPath)
			if createdEnv {
				fmt.Printf("wrote %s\n", envPath)
			} else {
				fmt.Printf("kept existing %s\n", envPath)
			}
			fmt.Println("IMPORTANT: edit .env and set BLOCKNET_WALLET_PASSWORD before running the pool.")
			return
		case "--config", "-c":
			if len(os.Args) > 2 {
				configPath = os.Args[2]
			}
		case "--help", "-h":
			fmt.Println("usage: blocknet-pool [command] [flags]")
			fmt.Println()
			fmt.Println("commands:")
			fmt.Println("  init          generate default config.json and .env")
			fmt.Println()
			fmt.Println("flags:")
			fmt.Println("  -c, --config  path to config file (default: config.json)")
			return
		}
	}

	loadDotEnv(configPath)

	// Load config
	cfg, err := LoadConfig(configPath)
	if err != nil {
		log.Fatalf("config: %v", err)
	}

	// Mirror logs to a file for troubleshooting endpoints.
	cfg.LogPath = filepath.Join(filepath.Dir(configPath), "pool.log")
	if lf, err := os.OpenFile(cfg.LogPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644); err != nil {
		log.Printf("WARNING: failed to open log file %s: %v", cfg.LogPath, err)
	} else {
		log.SetOutput(io.MultiWriter(os.Stdout, lf))
		log.Printf("logging to stdout and %s", cfg.LogPath)
	}

	log.Printf("starting %s", cfg.PoolName)

	// Ensure template directory exists
	if err := os.MkdirAll(cfg.TemplatePath, 0755); err != nil {
		log.Fatalf("create template dir: %v", err)
	}

	// Read daemon token from cookie file if not set in config.
	// The node writes api.cookie to its data dir on startup.
	if cfg.DaemonToken == "" {
		cookiePath := filepath.Join(cfg.DaemonDataDir, "api.cookie")
		if data, err := os.ReadFile(cookiePath); err == nil {
			cfg.DaemonToken = string(data)
			log.Printf("loaded daemon token from %s", cookiePath)
		} else {
			log.Printf("WARNING: no daemon_token and cannot read %s: %v", cookiePath, err)
		}
	}

	// Open storage
	store, err := OpenStore(cfg.DatabasePath)
	if err != nil {
		log.Fatalf("storage: %v", err)
	}
	defer store.Close()

	// Create node client
	node := NewNodeClient(cfg.DaemonAPI, cfg.DaemonToken)

	// Check node connectivity
	status, err := node.GetStatus()
	if err != nil {
		log.Printf("WARNING: cannot reach node at %s: %v", cfg.DaemonAPI, err)
		log.Printf("the pool will retry when the node becomes available")
	} else {
		log.Printf("node connected: height=%d peers=%d syncing=%v", status.ChainHeight, status.Peers, status.Syncing)
	}

	// Create components
	stats := NewPoolStats()
	jobMgr := NewJobManager(node, cfg)
	stratum := NewStratumServer(cfg, jobMgr, store, stats)
	payout := NewPayoutProcessor(cfg, store, node)
	web := NewWebServer(cfg, store, stats, node, stratum, jobMgr)

	// Start everything
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	jobMgr.Start(ctx)
	log.Printf("job manager started (poll interval: %s)", cfg.BlockPollInterval)

	if err := stratum.Start(ctx); err != nil {
		log.Fatalf("stratum: %v", err)
	}
	log.Printf("stratum server started on port %d", cfg.StratumPort)

	payout.Start(ctx)
	log.Printf("payout processor started (scheme: %s, interval: %s)", cfg.PayoutScheme, cfg.PayoutInterval)

	// Start background cleanup for expired seen shares (runs daily)
	go func() {
		ticker := time.NewTicker(24 * time.Hour)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := store.CleanExpiredSeenShares(); err != nil {
					log.Printf("failed to clean expired seen shares: %v", err)
				}
			}
		}
	}()

	// Graceful shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigCh
		log.Println("shutting down...")
		cancel()
		stratum.Stop()
		jobMgr.Stop()
		payout.Stop()
		store.Close()
		os.Exit(0)
	}()

	// Start web server (blocks)
	log.Printf("web server starting on %s:%d", cfg.APIHost, cfg.APIPort)
	if err := web.ListenAndServe(); err != nil {
		log.Fatalf("web server: %v", err)
	}
}

func loadDotEnv(configPath string) {
	candidates := []string{
		filepath.Join(filepath.Dir(configPath), ".env"),
		".env",
	}

	seen := make(map[string]struct{}, len(candidates))
	for _, path := range candidates {
		path = filepath.Clean(path)
		if _, ok := seen[path]; ok {
			continue
		}
		seen[path] = struct{}{}

		if err := godotenv.Load(path); err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			log.Printf("WARNING: failed loading env file %s: %v", path, err)
			continue
		}
		log.Printf("loaded environment from %s", path)
	}
}
