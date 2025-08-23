package main

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/k3s-io/kine/pkg/client"
	"github.com/k3s-io/kine/pkg/endpoint"
	"go.etcd.io/bbolt"
)

type Migrator struct {
	config     *Config
	etcdDB     *bbolt.DB
	kineClient client.Client
}

type KeyValue struct {
	Key      []byte
	Value    []byte
	Revision int64
	Lease    int64
}

func NewMigrator(config *Config) (*Migrator, error) {
	// Open etcd snapshot
	etcdDB, err := bbolt.Open(config.SnapshotPath, 0600, &bbolt.Options{
		ReadOnly: true,
		Timeout:  10 * time.Second,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to open etcd snapshot: %w", err)
	}

	var kineClient client.Client
	if !config.DryRun {
		// Configure kine client
		etcdConfig := endpoint.ETCDConfig{
			Endpoints: []string{config.KineEndpoint},
		}

		// Configure TLS if provided
		if config.TLSCert != "" && config.TLSKey != "" {
			etcdConfig.TLSConfig.CertFile = config.TLSCert
			etcdConfig.TLSConfig.KeyFile = config.TLSKey
			etcdConfig.TLSConfig.CAFile = config.TLSCACert
		}

		kineClient, err = client.New(etcdConfig)
		if err != nil {
			etcdDB.Close()
			return nil, fmt.Errorf("failed to connect to kine: %w", err)
		}
	}

	return &Migrator{
		config:     config,
		etcdDB:     etcdDB,
		kineClient: kineClient,
	}, nil
}

func (m *Migrator) Close() {
	if m.etcdDB != nil {
		m.etcdDB.Close()
	}
	if m.kineClient != nil {
		m.kineClient.Close()
	}
}

func (m *Migrator) Migrate(ctx context.Context) (*MigrationStats, error) {
	stats := &MigrationStats{
		StartTime: time.Now(),
	}

	log.Printf("Scanning etcd snapshot for keys...")

	// First pass: count total keys
	err := m.etcdDB.View(func(tx *bbolt.Tx) error {
		return m.countKeys(tx, stats)
	})
	if err != nil {
		return stats, fmt.Errorf("failed to scan etcd snapshot: %w", err)
	}

	log.Printf("Found %d total keys in snapshot", stats.TotalKeys)

	if m.config.DryRun {
		log.Printf("Dry run mode - would migrate %d keys", stats.TotalKeys-stats.SkippedKeys)
		return stats, nil
	}

	log.Printf("Starting data migration...")

	// Second pass: migrate data in batches
	batch := make([]KeyValue, 0, m.config.BatchSize)
	
	err = m.etcdDB.View(func(tx *bbolt.Tx) error {
		return m.migrateKeys(ctx, tx, stats, &batch)
	})
	if err != nil {
		return stats, fmt.Errorf("migration failed: %w", err)
	}

	// Process any remaining keys in the final batch
	if len(batch) > 0 {
		if err := m.processBatch(ctx, batch, stats); err != nil {
			log.Printf("Error processing final batch: %v", err)
			stats.ErrorCount += int64(len(batch))
		}
	}

	return stats, nil
}

func (m *Migrator) countKeys(tx *bbolt.Tx, stats *MigrationStats) error {
	// etcd stores data in the "key" bucket
	bucket := tx.Bucket([]byte("key"))
	if bucket == nil {
		return fmt.Errorf("etcd snapshot does not contain 'key' bucket")
	}

	return bucket.ForEach(func(k, v []byte) error {
		if m.shouldSkipKey(string(k)) {
			stats.SkippedKeys++
		} else {
			stats.TotalKeys++
		}
		return nil
	})
}

func (m *Migrator) migrateKeys(ctx context.Context, tx *bbolt.Tx, stats *MigrationStats, batch *[]KeyValue) error {
	bucket := tx.Bucket([]byte("key"))
	if bucket == nil {
		return fmt.Errorf("etcd snapshot does not contain 'key' bucket")
	}

	return bucket.ForEach(func(k, v []byte) error {
		key := string(k)
		
		if m.shouldSkipKey(key) {
			return nil
		}

		// Parse etcd's key-value structure
		kv, err := m.parseEtcdKeyValue(k, v)
		if err != nil {
			log.Printf("Warning: failed to parse key %s: %v", key, err)
			stats.ErrorCount++
			return nil
		}

		*batch = append(*batch, kv)

		// Process batch when it reaches the configured size
		if len(*batch) >= m.config.BatchSize {
			if err := m.processBatch(ctx, *batch, stats); err != nil {
				log.Printf("Error processing batch: %v", err)
				stats.ErrorCount += int64(len(*batch))
			}
			*batch = (*batch)[:0] // Reset slice
			
			// Report progress after processing batch
			if stats.ProcessedKeys%int64(m.config.ProgressReport) == 0 {
				elapsed := time.Since(stats.StartTime)
				rate := float64(stats.ProcessedKeys) / elapsed.Seconds()
				log.Printf("Processed %d/%d keys (%.1f keys/sec)", stats.ProcessedKeys, stats.TotalKeys, rate)
			}
		}

		return nil
	})
}

func (m *Migrator) parseEtcdKeyValue(k, v []byte) (KeyValue, error) {
	// etcd stores values with metadata
	// For now, we'll extract the basic key-value pair
	// In a production implementation, you'd parse the full etcd metadata
	
	return KeyValue{
		Key:   k,
		Value: v,
		// Note: etcd revision parsing would require understanding the specific
		// storage format used by etcd. For now, we'll use a simple approach.
		Revision: 0,
		Lease:    0,
	}, nil
}

func (m *Migrator) processBatch(ctx context.Context, batch []KeyValue, stats *MigrationStats) error {
	for _, kv := range batch {
		err := m.kineClient.Create(ctx, string(kv.Key), kv.Value)
		if err != nil {
			// If key exists, try to update it
			if strings.Contains(err.Error(), "key exists") {
				// Get current revision for update
				current, getErr := m.kineClient.Get(ctx, string(kv.Key))
				if getErr != nil {
					log.Printf("Failed to get existing key %s: %v", string(kv.Key), getErr)
					stats.ErrorCount++
					continue
				}
				
				updateErr := m.kineClient.Update(ctx, string(kv.Key), current.Modified, kv.Value)
				if updateErr != nil {
					log.Printf("Failed to update key %s: %v", string(kv.Key), updateErr)
					stats.ErrorCount++
					continue
				}
			} else {
				log.Printf("Failed to create key %s: %v", string(kv.Key), err)
				stats.ErrorCount++
				continue
			}
		}
		
		stats.ProcessedKeys++
	}
	
	return nil
}

func (m *Migrator) shouldSkipKey(key string) bool {
	if !m.config.SkipSystemKeys {
		return false
	}

	// Skip common Kubernetes system keys that are ephemeral
	systemPrefixes := []string{
		"/registry/events/",
		"/registry/minions/",
		"/registry/ranges/serviceips",
		"/registry/ranges/servicenodeports",
	}

	for _, prefix := range systemPrefixes {
		if strings.HasPrefix(key, prefix) {
			return true
		}
	}

	return false
}