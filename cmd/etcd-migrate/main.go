package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"time"
)

type Config struct {
	SnapshotPath    string
	KineEndpoint    string
	BatchSize       int
	DryRun          bool
	SkipSystemKeys  bool
	ProgressReport  int
	TLSCert         string
	TLSKey          string
	TLSCACert       string
}

type MigrationStats struct {
	TotalKeys     int64
	ProcessedKeys int64
	ErrorCount    int64
	SkippedKeys   int64
	StartTime     time.Time
}

func main() {
	config := &Config{}
	
	flag.StringVar(&config.SnapshotPath, "snapshot", "", "Path to etcd snapshot file (.db)")
	flag.StringVar(&config.KineEndpoint, "kine-endpoint", "", "Kine endpoint URL")
	flag.IntVar(&config.BatchSize, "batch-size", 100, "Number of keys to process in each batch")
	flag.BoolVar(&config.DryRun, "dry-run", false, "Show what would be migrated without actually doing it")
	flag.BoolVar(&config.SkipSystemKeys, "skip-system", false, "Skip Kubernetes system keys (events, etc.)")
	flag.IntVar(&config.ProgressReport, "progress", 1000, "Report progress every N keys")
	flag.StringVar(&config.TLSCert, "tls-cert", "", "TLS certificate file")
	flag.StringVar(&config.TLSKey, "tls-key", "", "TLS private key file")
	flag.StringVar(&config.TLSCACert, "tls-ca", "", "TLS CA certificate file")
	
	var verify bool
	flag.BoolVar(&verify, "verify", false, "Run verification after migration")
	flag.Parse()

	if config.SnapshotPath == "" || config.KineEndpoint == "" {
		fmt.Fprintf(os.Stderr, "Usage: %s -snapshot <path> -kine-endpoint <url>\n", os.Args[0])
		flag.PrintDefaults()
		os.Exit(1)
	}

	if err := runMigration(config, verify); err != nil {
		log.Fatalf("Migration failed: %v", err)
	}
}

func runMigration(config *Config, verify bool) error {
	log.Printf("Starting etcd to kine migration...")
	log.Printf("Snapshot: %s", config.SnapshotPath)
	log.Printf("Kine endpoint: %s", config.KineEndpoint)
	log.Printf("Dry run: %v", config.DryRun)
	log.Printf("Verification: %v", verify)

	// Initialize migration tool
	migrator, err := NewMigrator(config)
	if err != nil {
		return fmt.Errorf("failed to initialize migrator: %w", err)
	}
	defer migrator.Close()

	// Run migration with optional verification
	if verify && !config.DryRun {
		return migrator.RunFullMigrationWithVerification(context.Background())
	}

	// Run the migration only
	stats, err := migrator.Migrate(context.Background())
	if err != nil {
		return fmt.Errorf("migration failed: %w", err)
	}

	// Print final statistics
	duration := time.Since(stats.StartTime)
	log.Printf("Migration completed successfully!")
	log.Printf("Total keys: %d", stats.TotalKeys)
	log.Printf("Processed keys: %d", stats.ProcessedKeys)
	log.Printf("Skipped keys: %d", stats.SkippedKeys)
	log.Printf("Errors: %d", stats.ErrorCount)
	log.Printf("Duration: %v", duration)
	if duration > 0 {
		log.Printf("Rate: %.2f keys/second", float64(stats.ProcessedKeys)/duration.Seconds())
	}

	return nil
}