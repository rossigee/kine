package server

import (
	"context"
	"fmt"
	"testing"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
)

// mockBackend is a simple in-memory backend for testing
type mockBackend struct {
	data map[string]*KeyValue
	rev  int64
}

func newMockBackend() *mockBackend {
	return &mockBackend{
		data: make(map[string]*KeyValue),
		rev:  0,
	}
}

func (m *mockBackend) Start(ctx context.Context) error { return nil }

<<<<<<< HEAD
func (m *mockBackend) Get(ctx context.Context, key, rangeEnd string, limit, revision int64, keysOnly bool) (int64, *KeyValue, error) {
	if kv, exists := m.data[key]; exists {
		return m.rev, kv, nil
	}
	return m.rev, nil, nil
}

func (m *mockBackend) Create(ctx context.Context, key string, value []byte, lease int64) (int64, error) {
	if _, exists := m.data[key]; exists {
		return m.rev, ErrKeyExists
	}
	m.rev++
	m.data[key] = &KeyValue{
		Key:            key,
		Value:          value,
		CreateRevision: m.rev,
		ModRevision:    m.rev,
		Lease:          lease,
	}
	return m.rev, nil
}

func (m *mockBackend) Update(ctx context.Context, key string, value []byte, revision, lease int64) (int64, *KeyValue, bool, error) {
	if kv, exists := m.data[key]; exists && kv.ModRevision == revision {
		m.rev++
		oldKv := *kv // copy
		kv.Value = value
		kv.ModRevision = m.rev
		kv.Lease = lease
		return m.rev, &oldKv, true, nil
	}
	if kv, exists := m.data[key]; exists {
		return m.rev, kv, false, nil
	}
	return m.rev, nil, false, nil
}

func (m *mockBackend) Delete(ctx context.Context, key string, revision int64) (int64, *KeyValue, bool, error) {
	if kv, exists := m.data[key]; exists {
		if revision == 0 || kv.ModRevision == revision {
			delete(m.data, key)
			m.rev++
			return m.rev, kv, true, nil
		}
		return m.rev, kv, false, nil
	}
	return m.rev, nil, false, nil
}

<<<<<<< HEAD
func (m *mockBackend) List(ctx context.Context, prefix, startKey string, limit, revision int64, keysOnly bool) (int64, []*KeyValue, error) {
	var kvs []*KeyValue
	for key, kv := range m.data {
		// Check prefix match
		matchesPrefix := prefix == "" || (len(key) >= len(prefix) && key[:len(prefix)] == prefix)

		// Check start key condition (key >= startKey)
		matchesStart := startKey == "" || key >= startKey

		if matchesPrefix && matchesStart {
			kvs = append(kvs, kv)
		}
	}

	// Apply limit if specified
	if limit > 0 && int64(len(kvs)) > limit {
		kvs = kvs[:limit]
	}

	return m.rev, kvs, nil
}

func (m *mockBackend) Count(ctx context.Context, prefix, startKey string, revision int64) (int64, int64, error) {
	count := int64(0)
	for key := range m.data {
		// Check prefix match
		matchesPrefix := prefix == "" || (len(key) >= len(prefix) && key[:len(prefix)] == prefix)

		// Check start key condition (key >= startKey)
		matchesStart := startKey == "" || key >= startKey

		if matchesPrefix && matchesStart {
			count++
		}
	}
	return m.rev, count, nil
}

func (m *mockBackend) Watch(ctx context.Context, key string, revision int64) WatchResult {
	// Return a minimal WatchResult for testing
	eventCh := make(chan []*Event)
	errorCh := make(chan error)
	close(eventCh) // Close immediately for tests
	close(errorCh)
	return WatchResult{
		CurrentRevision: m.rev,
		CompactRevision: 0,
		Events:          eventCh,
		Errorc:          errorCh,
	}
}

func (m *mockBackend) DbSize(ctx context.Context) (int64, error) {
	return int64(len(m.data)), nil
}

func (m *mockBackend) CurrentRevision(ctx context.Context) (int64, error) {
	return m.rev, nil
}

func (m *mockBackend) Compact(ctx context.Context, revision int64) (int64, error) {
	return m.rev, nil
}

func TestLimitedServer_Put(t *testing.T) {
	backend := newMockBackend()
	server := &LimitedServer{backend: backend}
	ctx := context.Background()

	t.Run("CreateNewKey", func(t *testing.T) {
		req := &etcdserverpb.PutRequest{
			Key:   []byte("test-key"),
			Value: []byte("test-value"),
			Lease: 0,
		}

		resp, err := server.Put(ctx, req)
		if err != nil {
			t.Fatalf("Put failed: %v", err)
		}

		if resp.Header.Revision != 1 {
			t.Errorf("Expected revision 1, got %d", resp.Header.Revision)
		}

		// Verify key was created
<<<<<<< HEAD
		_, kv, err := backend.Get(ctx, "test-key", "", 1, 0, false)
=======
		_, kv, err := backend.Get(ctx, "test-key", "", 1, 0)
>>>>>>> master
		if err != nil {
			t.Fatalf("Get failed: %v", err)
		}
		if kv == nil {
			t.Fatal("Key was not created")
		}
		if string(kv.Value) != "test-value" {
			t.Errorf("Expected value 'test-value', got '%s'", string(kv.Value))
		}
	})

	t.Run("UpdateExistingKey", func(t *testing.T) {
		// First create a key
		req := &etcdserverpb.PutRequest{
			Key:   []byte("update-key"),
			Value: []byte("original-value"),
		}
		_, err := server.Put(ctx, req)
		if err != nil {
			t.Fatalf("Initial Put failed: %v", err)
		}

		// Now update it
		req.Value = []byte("updated-value")
		resp, err := server.Put(ctx, req)
		if err != nil {
			t.Fatalf("Update Put failed: %v", err)
		}

		if resp.Header.Revision <= 1 {
			t.Errorf("Expected revision > 1 for update, got %d", resp.Header.Revision)
		}

		// Verify key was updated
<<<<<<< HEAD
		_, kv, err := backend.Get(ctx, "update-key", "", 1, 0, false)
=======
		_, kv, err := backend.Get(ctx, "update-key", "", 1, 0)
>>>>>>> master
		if err != nil {
			t.Fatalf("Get failed: %v", err)
		}
		if string(kv.Value) != "updated-value" {
			t.Errorf("Expected updated value 'updated-value', got '%s'", string(kv.Value))
		}
	})

	t.Run("PutWithPrevKv", func(t *testing.T) {
		// Create initial key
		backend.Create(ctx, "prevkv-key", []byte("initial-value"), 0)

		req := &etcdserverpb.PutRequest{
			Key:    []byte("prevkv-key"),
			Value:  []byte("new-value"),
			PrevKv: true,
		}

		resp, err := server.Put(ctx, req)
		if err != nil {
			t.Fatalf("Put with PrevKv failed: %v", err)
		}

		if resp.PrevKv == nil {
			t.Fatal("Expected PrevKv to be returned")
		}

		if string(resp.PrevKv.Value) != "initial-value" {
			t.Errorf("Expected previous value 'initial-value', got '%s'", string(resp.PrevKv.Value))
		}
	})

	t.Run("PutIgnoreLease", func(t *testing.T) {
		req := &etcdserverpb.PutRequest{
			Key:         []byte("ignore-lease-key"),
			Value:       []byte("value"),
			IgnoreLease: true,
		}

		_, err := server.Put(ctx, req)
		if err == nil {
			t.Error("Expected error for IgnoreLease, got nil")
		}
	})

	t.Run("PutIgnoreValue", func(t *testing.T) {
		req := &etcdserverpb.PutRequest{
			Key:         []byte("ignore-value-key"),
			Value:       []byte("value"),
			IgnoreValue: true,
		}

		_, err := server.Put(ctx, req)
		if err == nil {
			t.Error("Expected error for IgnoreValue, got nil")
		}
	})
}

func TestKVServerBridge_Put(t *testing.T) {
	backend := newMockBackend()
	limited := &LimitedServer{backend: backend}
	bridge := &KVServerBridge{limited: limited}
	ctx := context.Background()

	req := &etcdserverpb.PutRequest{
		Key:   []byte("bridge-test-key"),
		Value: []byte("bridge-test-value"),
	}

	resp, err := bridge.Put(ctx, req)
	if err != nil {
		t.Fatalf("Bridge Put failed: %v", err)
	}

	if resp.Header.Revision != 1 {
		t.Errorf("Expected revision 1, got %d", resp.Header.Revision)
	}

	// Verify through backend
<<<<<<< HEAD
	_, kv, err := backend.Get(ctx, "bridge-test-key", "", 1, 0, false)
=======
	_, kv, err := backend.Get(ctx, "bridge-test-key", "", 1, 0)
>>>>>>> master
	if err != nil {
		t.Fatalf("Backend Get failed: %v", err)
	}
	if kv == nil {
		t.Fatal("Key was not created through bridge")
	}
}

func TestKVServerBridge_Range_KeysOnly(t *testing.T) {
	backend := newMockBackend()
	limited := &LimitedServer{backend: backend}
	bridge := &KVServerBridge{limited: limited}
	ctx := context.Background()

	// Setup test data - use hierarchical keys that kine expects
	backend.Create(ctx, "test/key1", []byte("value1"), 0)
	backend.Create(ctx, "test/key2", []byte("value2"), 0)
	backend.Create(ctx, "other/key", []byte("other-value"), 0)

	t.Run("KeysOnlyFalse", func(t *testing.T) {
		req := &etcdserverpb.RangeRequest{
			Key:      []byte("test/"),
			RangeEnd: []byte("test0"), // Range from "test/" to "test0"
			KeysOnly: false,
		}

		resp, err := bridge.Range(ctx, req)
		if err != nil {
			t.Fatalf("Range failed: %v", err)
		}

		if len(resp.Kvs) != 2 {
			t.Errorf("Expected 2 keys, got %d", len(resp.Kvs))
		}

		// Verify values are included
		for _, kv := range resp.Kvs {
			if len(kv.Value) == 0 {
				t.Error("Expected value to be included, got empty value")
			}
		}
	})

	t.Run("KeysOnlyTrue", func(t *testing.T) {
		req := &etcdserverpb.RangeRequest{
			Key:      []byte("test/"),
			RangeEnd: []byte("test0"), // Range from "test/" to "test0"
			KeysOnly: true,
		}

		resp, err := bridge.Range(ctx, req)
		if err != nil {
			t.Fatalf("Range with KeysOnly failed: %v", err)
		}

		if len(resp.Kvs) != 2 {
			t.Errorf("Expected 2 keys, got %d", len(resp.Kvs))
		}

		// Verify values are excluded
		for _, kv := range resp.Kvs {
			if kv.Value != nil {
				t.Errorf("Expected nil value for KeysOnly, got %v", kv.Value)
			}
			if len(kv.Key) == 0 {
				t.Error("Expected key to be present")
			}
		}

		// Verify other metadata is still present
		for _, kv := range resp.Kvs {
			if kv.CreateRevision == 0 {
				t.Error("Expected CreateRevision to be present")
			}
			if kv.ModRevision == 0 {
				t.Error("Expected ModRevision to be present")
			}
		}
	})

	t.Run("KeysOnlySingleKey", func(t *testing.T) {
		req := &etcdserverpb.RangeRequest{
			Key:      []byte("test/key1"),
			KeysOnly: true,
		}

		resp, err := bridge.Range(ctx, req)
		if err != nil {
			t.Fatalf("Single key Range with KeysOnly failed: %v", err)
		}

		if len(resp.Kvs) != 1 {
			t.Errorf("Expected 1 key, got %d", len(resp.Kvs))
		}

		if resp.Kvs[0].Value != nil {
			t.Error("Expected nil value for KeysOnly single key")
		}

		if string(resp.Kvs[0].Key) != "test/key1" {
			t.Errorf("Expected key 'test/key1', got '%s'", string(resp.Kvs[0].Key))
		}
	})
}

func TestToKVsKeysOnly(t *testing.T) {
	kvs := []*KeyValue{
		{
			Key:            "test1",
			Value:          []byte("value1"),
			CreateRevision: 1,
			ModRevision:    1,
			Lease:          0,
		},
		{
			Key:            "test2",
			Value:          []byte("value2"),
			CreateRevision: 2,
			ModRevision:    2,
			Lease:          123,
		},
	}

	result := toKVsKeysOnly(kvs...)

	if len(result) != 2 {
		t.Errorf("Expected 2 items, got %d", len(result))
	}

	for i, kv := range result {
		if kv.Value != nil {
			t.Errorf("Item %d: Expected nil value, got %v", i, kv.Value)
		}
		if len(kv.Key) == 0 {
			t.Errorf("Item %d: Expected key to be present", i)
		}
		if kv.CreateRevision == 0 {
			t.Errorf("Item %d: Expected CreateRevision to be preserved", i)
		}
		if kv.ModRevision == 0 {
			t.Errorf("Item %d: Expected ModRevision to be preserved", i)
		}
		if i == 1 && kv.Lease != 123 {
			t.Errorf("Item %d: Expected Lease to be preserved, got %d", i, kv.Lease)
		}
	}

	// Test with nil input
	result = toKVsKeysOnly(nil)
	if result != nil {
		t.Error("Expected nil result for nil input")
	}

	// Test with empty slice
	result = toKVsKeysOnly()
	if result != nil {
		t.Error("Expected nil result for empty input")
	}
}

func TestLimitedServer_DeleteRange(t *testing.T) {
	backend := newMockBackend()
	server := &LimitedServer{backend: backend}
	ctx := context.Background()

	t.Run("DeleteSingleKey", func(t *testing.T) {
		// Create test key
		backend.Create(ctx, "delete-key", []byte("delete-value"), 0)

		req := &etcdserverpb.DeleteRangeRequest{
			Key: []byte("delete-key"),
		}

		resp, err := server.DeleteRange(ctx, req)
		if err != nil {
			t.Fatalf("DeleteRange failed: %v", err)
		}

		if resp.Deleted != 1 {
			t.Errorf("Expected 1 deleted key, got %d", resp.Deleted)
		}

		// Verify key was deleted
<<<<<<< HEAD
		_, kv, err := backend.Get(ctx, "delete-key", "", 1, 0, false)
=======
		_, kv, err := backend.Get(ctx, "delete-key", "", 1, 0)
>>>>>>> master
		if err != nil {
			t.Fatalf("Get after delete failed: %v", err)
		}
		if kv != nil {
			t.Error("Expected key to be deleted")
		}
	})

	t.Run("DeleteSingleKeyWithPrevKv", func(t *testing.T) {
		// Create test key
		backend.Create(ctx, "prevkv-delete-key", []byte("prevkv-delete-value"), 123)

		req := &etcdserverpb.DeleteRangeRequest{
			Key:    []byte("prevkv-delete-key"),
			PrevKv: true,
		}

		resp, err := server.DeleteRange(ctx, req)
		if err != nil {
			t.Fatalf("DeleteRange with PrevKv failed: %v", err)
		}

		if resp.Deleted != 1 {
			t.Errorf("Expected 1 deleted key, got %d", resp.Deleted)
		}

		if len(resp.PrevKvs) != 1 {
			t.Errorf("Expected 1 PrevKv, got %d", len(resp.PrevKvs))
		}

		if string(resp.PrevKvs[0].Value) != "prevkv-delete-value" {
			t.Errorf("Expected previous value 'prevkv-delete-value', got '%s'", string(resp.PrevKvs[0].Value))
		}

		if resp.PrevKvs[0].Lease != 123 {
			t.Errorf("Expected lease 123, got %d", resp.PrevKvs[0].Lease)
		}
	})

	t.Run("DeleteNonexistentKey", func(t *testing.T) {
		req := &etcdserverpb.DeleteRangeRequest{
			Key: []byte("nonexistent-key"),
		}

		resp, err := server.DeleteRange(ctx, req)
		if err != nil {
			t.Fatalf("DeleteRange of nonexistent key failed: %v", err)
		}

		if resp.Deleted != 0 {
			t.Errorf("Expected 0 deleted keys, got %d", resp.Deleted)
		}
	})

	t.Run("DeleteRange", func(t *testing.T) {
		// Create test keys
		backend.Create(ctx, "range-key1", []byte("range-value1"), 0)
		backend.Create(ctx, "range-key2", []byte("range-value2"), 0)
		backend.Create(ctx, "range-key3", []byte("range-value3"), 0)
		backend.Create(ctx, "other-key", []byte("other-value"), 0)

		req := &etcdserverpb.DeleteRangeRequest{
			Key:      []byte("range-key"),
			RangeEnd: []byte("range-kez"), // Delete range-key* but not other-key
		}

		resp, err := server.DeleteRange(ctx, req)
		if err != nil {
			t.Fatalf("DeleteRange failed: %v", err)
		}

		if resp.Deleted != 3 {
			t.Errorf("Expected 3 deleted keys, got %d", resp.Deleted)
		}

		// Verify range keys were deleted
		for i := 1; i <= 3; i++ {
			key := fmt.Sprintf("range-key%d", i)
<<<<<<< HEAD
			_, kv, err := backend.Get(ctx, key, "", 1, 0, false)
=======
			_, kv, err := backend.Get(ctx, key, "", 1, 0)
>>>>>>> master
			if err != nil {
				t.Fatalf("Get after range delete failed: %v", err)
			}
			if kv != nil {
				t.Errorf("Expected key %s to be deleted", key)
			}
		}

		// Verify other key was not deleted
<<<<<<< HEAD
		_, kv, err := backend.Get(ctx, "other-key", "", 1, 0, false)
=======
		_, kv, err := backend.Get(ctx, "other-key", "", 1, 0)
>>>>>>> master
		if err != nil {
			t.Fatalf("Get other-key after range delete failed: %v", err)
		}
		if kv == nil {
			t.Error("Expected other-key to remain")
		}
	})

	t.Run("DeleteRangeWithPrevKv", func(t *testing.T) {
		// Create test keys
		backend.Create(ctx, "prev-range1", []byte("prev-value1"), 0)
		backend.Create(ctx, "prev-range2", []byte("prev-value2"), 0)

		req := &etcdserverpb.DeleteRangeRequest{
			Key:      []byte("prev-range"),
			RangeEnd: []byte("prev-rangz"),
			PrevKv:   true,
		}

		resp, err := server.DeleteRange(ctx, req)
		if err != nil {
			t.Fatalf("DeleteRange with PrevKv failed: %v", err)
		}

		if resp.Deleted != 2 {
			t.Errorf("Expected 2 deleted keys, got %d", resp.Deleted)
		}

		if len(resp.PrevKvs) != 2 {
			t.Errorf("Expected 2 PrevKvs, got %d", len(resp.PrevKvs))
		}

		// Verify previous values are returned
		found := make(map[string]bool)
		for _, kv := range resp.PrevKvs {
			found[string(kv.Value)] = true
		}

		if !found["prev-value1"] || !found["prev-value2"] {
			t.Error("Expected both previous values to be returned")
		}
	})
}

func TestKVServerBridge_DeleteRange(t *testing.T) {
	backend := newMockBackend()
	limited := &LimitedServer{backend: backend}
	bridge := &KVServerBridge{limited: limited}
	ctx := context.Background()

	// Create test key
	backend.Create(ctx, "bridge-delete-key", []byte("bridge-delete-value"), 0)

	req := &etcdserverpb.DeleteRangeRequest{
		Key: []byte("bridge-delete-key"),
	}

	resp, err := bridge.DeleteRange(ctx, req)
	if err != nil {
		t.Fatalf("Bridge DeleteRange failed: %v", err)
	}

	if resp.Deleted != 1 {
		t.Errorf("Expected 1 deleted key, got %d", resp.Deleted)
	}

	// Verify through backend
<<<<<<< HEAD
	_, kv, err := backend.Get(ctx, "bridge-delete-key", "", 1, 0, false)
=======
	_, kv, err := backend.Get(ctx, "bridge-delete-key", "", 1, 0)
>>>>>>> master
	if err != nil {
		t.Fatalf("Backend Get after bridge delete failed: %v", err)
	}
	if kv != nil {
		t.Error("Expected key to be deleted through bridge")
	}
}

// Regression and Edge Case Tests
func TestPutRegressionTests(t *testing.T) {
	backend := newMockBackend()
	server := &LimitedServer{backend: backend}
	ctx := context.Background()

	t.Run("PutEmptyKey", func(t *testing.T) {
		req := &etcdserverpb.PutRequest{
			Key:   []byte(""),
			Value: []byte("empty-key-value"),
		}

		resp, err := server.Put(ctx, req)
		if err != nil {
			t.Fatalf("Put with empty key failed: %v", err)
		}

		if resp.Header.Revision == 0 {
			t.Error("Expected non-zero revision for empty key")
		}

		// Verify empty key was created
<<<<<<< HEAD
		_, kv, err := backend.Get(ctx, "", "", 1, 0, false)
=======
		_, kv, err := backend.Get(ctx, "", "", 1, 0)
>>>>>>> master
		if err != nil {
			t.Fatalf("Get empty key failed: %v", err)
		}
		if kv == nil {
			t.Error("Expected empty key to be created")
		}
	})

	t.Run("PutEmptyValue", func(t *testing.T) {
		req := &etcdserverpb.PutRequest{
			Key:   []byte("empty-value-key"),
			Value: []byte(""),
		}

		resp, err := server.Put(ctx, req)
		if err != nil {
			t.Fatalf("Put with empty value failed: %v", err)
		}

		if resp.Header.Revision == 0 {
			t.Error("Expected non-zero revision for empty value")
		}

		// Verify empty value was stored
<<<<<<< HEAD
		_, kv, err := backend.Get(ctx, "empty-value-key", "", 1, 0, false)
=======
		_, kv, err := backend.Get(ctx, "empty-value-key", "", 1, 0)
>>>>>>> master
		if err != nil {
			t.Fatalf("Get empty value failed: %v", err)
		}
		if kv == nil {
			t.Error("Expected key with empty value to be created")
		}
		if len(kv.Value) != 0 {
			t.Errorf("Expected empty value, got %v", kv.Value)
		}
	})

	t.Run("PutNilValue", func(t *testing.T) {
		req := &etcdserverpb.PutRequest{
			Key:   []byte("nil-value-key"),
			Value: nil,
		}

		resp, err := server.Put(ctx, req)
		if err != nil {
			t.Fatalf("Put with nil value failed: %v", err)
		}

		if resp.Header.Revision == 0 {
			t.Error("Expected non-zero revision for nil value")
		}

		// Verify nil value was handled properly
<<<<<<< HEAD
		_, kv, err := backend.Get(ctx, "nil-value-key", "", 1, 0, false)
=======
		_, kv, err := backend.Get(ctx, "nil-value-key", "", 1, 0)
>>>>>>> master
		if err != nil {
			t.Fatalf("Get nil value failed: %v", err)
		}
		if kv == nil {
			t.Error("Expected key with nil value to be created")
		}
	})

	t.Run("PutLargeValue", func(t *testing.T) {
		largeValue := make([]byte, 1024*1024) // 1MB
		for i := range largeValue {
			largeValue[i] = byte(i % 256)
		}

		req := &etcdserverpb.PutRequest{
			Key:   []byte("large-value-key"),
			Value: largeValue,
		}

		resp, err := server.Put(ctx, req)
		if err != nil {
			t.Fatalf("Put with large value failed: %v", err)
		}

		if resp.Header.Revision == 0 {
			t.Error("Expected non-zero revision for large value")
		}

		// Verify large value was stored correctly
<<<<<<< HEAD
		_, kv, err := backend.Get(ctx, "large-value-key", "", 1, 0, false)
=======
		_, kv, err := backend.Get(ctx, "large-value-key", "", 1, 0)
>>>>>>> master
		if err != nil {
			t.Fatalf("Get large value failed: %v", err)
		}
		if kv == nil {
			t.Error("Expected large value key to be created")
		}
		if len(kv.Value) != len(largeValue) {
			t.Errorf("Expected large value length %d, got %d", len(largeValue), len(kv.Value))
		}
	})

	t.Run("PutWithLease", func(t *testing.T) {
		req := &etcdserverpb.PutRequest{
			Key:   []byte("lease-key"),
			Value: []byte("lease-value"),
			Lease: 12345,
		}

		_, err := server.Put(ctx, req)
		if err != nil {
			t.Fatalf("Put with lease failed: %v", err)
		}

		// Verify lease was preserved
<<<<<<< HEAD
		_, kv, err := backend.Get(ctx, "lease-key", "", 1, 0, false)
=======
		_, kv, err := backend.Get(ctx, "lease-key", "", 1, 0)
>>>>>>> master
		if err != nil {
			t.Fatalf("Get lease key failed: %v", err)
		}
		if kv.Lease != 12345 {
			t.Errorf("Expected lease 12345, got %d", kv.Lease)
		}
	})
}

func TestDeleteRangeRegressionTests(t *testing.T) {
	backend := newMockBackend()
	server := &LimitedServer{backend: backend}
	ctx := context.Background()

	t.Run("DeleteEmptyKey", func(t *testing.T) {
		// Create empty key first
		backend.Create(ctx, "", []byte("empty-key-value"), 0)

		req := &etcdserverpb.DeleteRangeRequest{
			Key: []byte(""),
		}

		resp, err := server.DeleteRange(ctx, req)
		if err != nil {
			t.Fatalf("Delete empty key failed: %v", err)
		}

		if resp.Deleted != 1 {
			t.Errorf("Expected 1 deleted key for empty key, got %d", resp.Deleted)
		}
	})

	t.Run("DeleteWithEmptyRangeEnd", func(t *testing.T) {
		// Create test key
		backend.Create(ctx, "single-delete", []byte("single-value"), 0)

		req := &etcdserverpb.DeleteRangeRequest{
			Key:      []byte("single-delete"),
			RangeEnd: []byte(""), // Empty range end should act as single key delete
		}

		resp, err := server.DeleteRange(ctx, req)
		if err != nil {
			t.Fatalf("Delete with empty range end failed: %v", err)
		}

		if resp.Deleted != 1 {
			t.Errorf("Expected 1 deleted key, got %d", resp.Deleted)
		}
	})

	t.Run("DeleteRangeNoMatches", func(t *testing.T) {
		req := &etcdserverpb.DeleteRangeRequest{
			Key:      []byte("nomatch-prefix"),
			RangeEnd: []byte("nomatch-prefiz"),
		}

		resp, err := server.DeleteRange(ctx, req)
		if err != nil {
			t.Fatalf("Delete range with no matches failed: %v", err)
		}

		if resp.Deleted != 0 {
			t.Errorf("Expected 0 deleted keys for no matches, got %d", resp.Deleted)
		}

		if len(resp.PrevKvs) != 0 {
			t.Error("Expected no PrevKvs for no matches")
		}
	})

	t.Run("DeleteRangeAllKeys", func(t *testing.T) {
		// Create multiple test keys
		backend.Create(ctx, "all1", []byte("value1"), 0)
		backend.Create(ctx, "all2", []byte("value2"), 0)
		backend.Create(ctx, "all3", []byte("value3"), 0)

		req := &etcdserverpb.DeleteRangeRequest{
			Key:      []byte("all"),
			RangeEnd: []byte("alm"), // Should match all*
		}

		resp, err := server.DeleteRange(ctx, req)
		if err != nil {
			t.Fatalf("Delete all keys in range failed: %v", err)
		}

		if resp.Deleted != 3 {
			t.Errorf("Expected 3 deleted keys, got %d", resp.Deleted)
		}
	})
}

func TestRangeRegressionTests(t *testing.T) {
	backend := newMockBackend()
	limited := &LimitedServer{backend: backend}
	bridge := &KVServerBridge{limited: limited}
	ctx := context.Background()

	t.Run("RangeEmptyKey", func(t *testing.T) {
		// Create empty key
		backend.Create(ctx, "", []byte("empty-key-value"), 0)

		req := &etcdserverpb.RangeRequest{
			Key: []byte(""),
		}

		resp, err := bridge.Range(ctx, req)
		if err != nil {
			t.Fatalf("Range with empty key failed: %v", err)
		}

		if len(resp.Kvs) != 1 {
			t.Errorf("Expected 1 key for empty key range, got %d", len(resp.Kvs))
		}
	})

	t.Run("RangeKeysOnlyWithEmptyValues", func(t *testing.T) {
		// Create keys with empty values
		backend.Create(ctx, "empty/val1", []byte(""), 0)
		backend.Create(ctx, "empty/val2", nil, 0)

		req := &etcdserverpb.RangeRequest{
			Key:      []byte("empty/"),
			RangeEnd: []byte("empty0"),
			KeysOnly: true,
		}

		resp, err := bridge.Range(ctx, req)
		if err != nil {
			t.Fatalf("Range KeysOnly with empty values failed: %v", err)
		}

		if len(resp.Kvs) != 2 {
			t.Errorf("Expected 2 keys, got %d", len(resp.Kvs))
		}

		// Verify all values are nil (KeysOnly)
		for _, kv := range resp.Kvs {
			if kv.Value != nil {
				t.Error("Expected nil value for KeysOnly with empty values")
			}
		}
	})

	t.Run("RangeWithSpecialCharacters", func(t *testing.T) {
		// Create keys with special characters under a common prefix
		specialKeys := []string{
			"special/key-with-dashes",
			"special/key_with_underscores",
			"special/key.with.dots",
			"special/key-with-spaces",
			"special/key-with-numbers123",
			"special/key-with-symbols@#$",
		}

		for _, key := range specialKeys {
			backend.Create(ctx, key, []byte("special-value"), 0)
		}

		req := &etcdserverpb.RangeRequest{
			Key:      []byte("special/"),
			RangeEnd: []byte("special0"),
		}

		resp, err := bridge.Range(ctx, req)
		if err != nil {
			t.Fatalf("Range with special characters failed: %v", err)
		}

		if len(resp.Kvs) < len(specialKeys) {
			t.Errorf("Expected at least %d keys with special characters, got %d", len(specialKeys), len(resp.Kvs))
		}
	})
}

// Concurrent access tests (basic)
func TestConcurrentPutOperations(t *testing.T) {
	backend := newMockBackend()
	server := &LimitedServer{backend: backend}
	ctx := context.Background()

	// Note: This is a basic test. In a real scenario, you'd need proper synchronization
	// and a thread-safe mock backend for true concurrent testing
	t.Run("SequentialConcurrentPuts", func(t *testing.T) {
		for i := 0; i < 10; i++ {
			req := &etcdserverpb.PutRequest{
				Key:   []byte(fmt.Sprintf("concurrent-key-%d", i)),
				Value: []byte(fmt.Sprintf("concurrent-value-%d", i)),
			}

			_, err := server.Put(ctx, req)
			if err != nil {
				t.Fatalf("Concurrent put %d failed: %v", i, err)
			}
		}

		// Verify all keys were created
		for i := 0; i < 10; i++ {
			key := fmt.Sprintf("concurrent-key-%d", i)
<<<<<<< HEAD
			_, kv, err := backend.Get(ctx, key, "", 1, 0, false)
=======
			_, kv, err := backend.Get(ctx, key, "", 1, 0)
>>>>>>> master
			if err != nil {
				t.Fatalf("Get concurrent key %s failed: %v", key, err)
			}
			if kv == nil {
				t.Errorf("Expected concurrent key %s to exist", key)
			}
		}
	})
}
