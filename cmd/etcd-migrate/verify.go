package main

import (
	"context"
	"fmt"
	"log"
	"strings"

	"go.etcd.io/bbolt"
)

type VerificationResult struct {
	TotalKeys      int64
	MatchingKeys   int64
	MissingKeys    int64
	DifferentKeys  int64
	ErrorCount     int64
	MissingInEtcd  int64
}

func (m *Migrator) Verify(ctx context.Context) (*VerificationResult, error) {
	if m.config.DryRun {
		return nil, fmt.Errorf("verification not available in dry-run mode")
	}

	log.Printf("Starting data verification...")
	
	result := &VerificationResult{}

	// Verify etcd keys exist in kine
	err := m.etcdDB.View(func(tx *bbolt.Tx) error {
		return m.verifyEtcdToKine(ctx, tx, result)
	})
	if err != nil {
		return result, fmt.Errorf("verification failed: %w", err)
	}

	// Verify kine doesn't have extra keys (optional reverse check)
	if err := m.verifyKineToEtcd(ctx, result); err != nil {
		log.Printf("Warning: reverse verification had issues: %v", err)
	}

	log.Printf("Verification completed:")
	log.Printf("  Total etcd keys checked: %d", result.TotalKeys)
	log.Printf("  Matching keys: %d", result.MatchingKeys)
	log.Printf("  Missing in kine: %d", result.MissingKeys)
	log.Printf("  Different values: %d", result.DifferentKeys)
	log.Printf("  Errors: %d", result.ErrorCount)
	log.Printf("  Extra keys in kine: %d", result.MissingInEtcd)

	if result.MissingKeys > 0 || result.DifferentKeys > 0 {
		return result, fmt.Errorf("verification failed: %d missing, %d different", 
			result.MissingKeys, result.DifferentKeys)
	}

	return result, nil
}

func (m *Migrator) verifyEtcdToKine(ctx context.Context, tx *bbolt.Tx, result *VerificationResult) error {
	bucket := tx.Bucket([]byte("key"))
	if bucket == nil {
		return fmt.Errorf("etcd snapshot does not contain 'key' bucket")
	}

	return bucket.ForEach(func(k, v []byte) error {
		key := string(k)
		
		if m.shouldSkipKey(key) {
			return nil
		}

		result.TotalKeys++

		// Get key from kine
		kineValue, err := m.kineClient.Get(ctx, key)
		if err != nil {
			if strings.Contains(err.Error(), "not found") {
				result.MissingKeys++
				log.Printf("Missing key in kine: %s", key)
			} else {
				result.ErrorCount++
				log.Printf("Error getting key %s from kine: %v", key, err)
			}
			return nil
		}

		// Parse etcd value for comparison
		etcdKV, err := m.parseEtcdKeyValue(k, v)
		if err != nil {
			result.ErrorCount++
			log.Printf("Error parsing etcd key %s: %v", key, err)
			return nil
		}

		// Compare values
		if string(etcdKV.Value) != string(kineValue.Data) {
			result.DifferentKeys++
			log.Printf("Different value for key %s", key)
			log.Printf("  etcd: %q", string(etcdKV.Value))
			log.Printf("  kine: %q", string(kineValue.Data))
		} else {
			result.MatchingKeys++
		}

		// Report progress for large datasets
		if result.TotalKeys%1000 == 0 {
			log.Printf("Verified %d keys...", result.TotalKeys)
		}

		return nil
	})
}

func (m *Migrator) verifyKineToEtcd(ctx context.Context, result *VerificationResult) error {
	// This is a simplified reverse check
	// In practice, you might want to list all keys from kine and check against etcd
	// For now, we'll just log that this feature could be implemented
	
	log.Printf("Reverse verification (kine->etcd) not implemented in this version")
	log.Printf("To fully verify, manually check that kine doesn't contain unexpected keys")
	
	return nil
}

func (m *Migrator) RunFullMigrationWithVerification(ctx context.Context) error {
	// Run migration
	stats, err := m.Migrate(ctx)
	if err != nil {
		return fmt.Errorf("migration failed: %w", err)
	}

	if m.config.DryRun {
		return nil
	}

	// Verify results
	log.Printf("Migration completed, starting verification...")
	
	verification, err := m.Verify(ctx)
	if err != nil {
		return fmt.Errorf("verification failed: %w", err)
	}

	// Summary report
	log.Printf("\n=== MIGRATION SUMMARY ===")
	log.Printf("Migration:")
	log.Printf("  Processed: %d keys", stats.ProcessedKeys)
	log.Printf("  Errors: %d", stats.ErrorCount)
	log.Printf("  Skipped: %d", stats.SkippedKeys)
	
	log.Printf("Verification:")
	log.Printf("  Verified: %d keys", verification.MatchingKeys)
	log.Printf("  Missing: %d", verification.MissingKeys)
	log.Printf("  Different: %d", verification.DifferentKeys)
	log.Printf("  Errors: %d", verification.ErrorCount)

	if stats.ErrorCount == 0 && verification.MissingKeys == 0 && verification.DifferentKeys == 0 {
		log.Printf("\n✅ Migration completed successfully!")
		return nil
	} else {
		log.Printf("\n❌ Migration completed with issues")
		return fmt.Errorf("migration had %d errors, verification found %d missing and %d different keys",
			stats.ErrorCount, verification.MissingKeys, verification.DifferentKeys)
	}
}