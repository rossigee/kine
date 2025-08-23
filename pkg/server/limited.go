package server

import (
	"context"
	"time"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
)

type LimitedServer struct {
	notifyInterval time.Duration
	backend        Backend
	scheme         string
}

func (l *LimitedServer) Range(ctx context.Context, r *etcdserverpb.RangeRequest) (*RangeResponse, error) {
	if len(r.RangeEnd) == 0 {
		return l.get(ctx, r)
	}
	return l.list(ctx, r)
}

func txnHeader(rev int64) *etcdserverpb.ResponseHeader {
	return &etcdserverpb.ResponseHeader{
		Revision: rev,
	}
}

func (l *LimitedServer) Txn(ctx context.Context, txn *etcdserverpb.TxnRequest) (*etcdserverpb.TxnResponse, error) {
	if put := isCreate(txn); put != nil {
		return l.create(ctx, put)
	}
	if rev, key, ok := isDelete(txn); ok {
		return l.delete(ctx, key, rev)
	}
	if rev, key, value, lease, ok := isUpdate(txn); ok {
		return l.update(ctx, rev, key, value, lease)
	}
	if ver, ok := isCompact(txn); ok {
		return l.compact(ctx, ver)
	}
	return nil, ErrNotSupported
}

func (l *LimitedServer) Put(ctx context.Context, r *etcdserverpb.PutRequest) (*etcdserverpb.PutResponse, error) {
	if r.IgnoreLease {
		return nil, unsupported("ignoreLease")
	}
	if r.IgnoreValue {
		return nil, unsupported("ignoreValue")
	}

	key := string(r.Key)
	var prevKv *KeyValue

	// If PrevKv is requested, get the current value before updating
	if r.PrevKv {
		_, kv, _ := l.backend.Get(ctx, key, "", 1, 0)
		if kv != nil {
			// Make a deep copy to avoid issues with shared memory
			prevKv = &KeyValue{
				Key:            kv.Key,
				Value:          make([]byte, len(kv.Value)),
				CreateRevision: kv.CreateRevision,
				ModRevision:    kv.ModRevision,
				Lease:          kv.Lease,
			}
			copy(prevKv.Value, kv.Value)
		}
	}

	// Try to create first (most common case for new keys)
	rev, err := l.backend.Create(ctx, key, r.Value, r.Lease)
	if err == ErrKeyExists {
		// Key exists, get current revision and update
		_, kv, getErr := l.backend.Get(ctx, key, "", 1, 0)
		if getErr != nil {
			return nil, getErr
		}
		if kv == nil {
			// Key was deleted between create attempt and get
			// Try create again
			rev, err = l.backend.Create(ctx, key, r.Value, r.Lease)
			if err != nil {
				return nil, err
			}
		} else {
			// Update with current revision
			updateRev, _, _, updateErr := l.backend.Update(ctx, key, r.Value, kv.ModRevision, r.Lease)
			if updateErr != nil {
				return nil, updateErr
			}
			rev = updateRev
		}
	} else if err != nil {
		return nil, err
	}

	response := &etcdserverpb.PutResponse{
		Header: &etcdserverpb.ResponseHeader{
			Revision: rev,
		},
	}

	// Handle PrevKv option
	if r.PrevKv && prevKv != nil {
		response.PrevKv = &mvccpb.KeyValue{
			Key:            []byte(prevKv.Key),
			Value:          prevKv.Value,
			CreateRevision: prevKv.CreateRevision,
			ModRevision:    prevKv.ModRevision,
			Lease:          prevKv.Lease,
		}
	}

	return response, nil
}

func (l *LimitedServer) DeleteRange(ctx context.Context, r *etcdserverpb.DeleteRangeRequest) (*etcdserverpb.DeleteRangeResponse, error) {
	key := string(r.Key)

	// Handle single key deletion (most common case)
	if len(r.RangeEnd) == 0 {
		rev, kv, ok, err := l.backend.Delete(ctx, key, 0) // 0 revision means delete regardless of revision
		if err != nil {
			return nil, err
		}

		response := &etcdserverpb.DeleteRangeResponse{
			Header: &etcdserverpb.ResponseHeader{
				Revision: rev,
			},
			Deleted: 0,
		}

		if ok && kv != nil {
			response.Deleted = 1
			if r.PrevKv {
				response.PrevKvs = []*mvccpb.KeyValue{
					{
						Key:            []byte(kv.Key),
						Value:          kv.Value,
						CreateRevision: kv.CreateRevision,
						ModRevision:    kv.ModRevision,
						Lease:          kv.Lease,
					},
				}
			}
		}

		return response, nil
	}

	// Handle range deletion (prefix-based)
	// This is a simplified implementation - for production, you'd want to optimize this
	rangeEnd := string(r.RangeEnd)

	// First, list all keys in the range to potentially delete
	rev, kvs, err := l.backend.List(ctx, key, "", 0, 0) // Get all keys with prefix
	if err != nil {
		return nil, err
	}

	var deleted int64
	var prevKvs []*mvccpb.KeyValue

	// Delete each key in the range
	for _, kv := range kvs {
		// Check if key is within range
		if kv.Key >= key && (rangeEnd == "" || kv.Key < rangeEnd) {
			delRev, delKv, ok, err := l.backend.Delete(ctx, kv.Key, 0)
			if err != nil {
				// Continue with other deletions even if one fails
				continue
			}
			if ok {
				deleted++
				rev = delRev // Update to latest revision
				if r.PrevKv && delKv != nil {
					prevKvs = append(prevKvs, &mvccpb.KeyValue{
						Key:            []byte(delKv.Key),
						Value:          delKv.Value,
						CreateRevision: delKv.CreateRevision,
						ModRevision:    delKv.ModRevision,
						Lease:          delKv.Lease,
					})
				}
			}
		}
	}

	return &etcdserverpb.DeleteRangeResponse{
		Header: &etcdserverpb.ResponseHeader{
			Revision: rev,
		},
		Deleted: deleted,
		PrevKvs: prevKvs,
	}, nil
}

type ResponseHeader struct {
	Revision int64
}

type RangeResponse struct {
	Header *etcdserverpb.ResponseHeader
	Kvs    []*KeyValue
	More   bool
	Count  int64
}
