package sqllog

import (
	"context"
	"database/sql"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/k3s-io/kine/pkg/drivers/generic"
	"github.com/k3s-io/kine/pkg/server"
)

// sqliteAfterOldValSQL is the After query adapted for SQLite (unnumbered ? params).
// Column order must exactly match the scan() dests in sql.go:
//
//	1  MAX(rkv.id)           → rev
//	2  MAX(crkv.prev_rev)    → compact (NullInt64)
//	3  kv.id                 → event.KV.ModRevision
//	4  kv.name               → event.KV.Key
//	5  kv.created            → event.Create  (int→bool)
//	6  kv.deleted            → event.Delete  (int→bool)
//	7  kv.create_revision    → event.KV.CreateRevision
//	8  kv.prev_revision      → event.PrevKV.ModRevision
//	9  kv.lease              → event.KV.Lease
//	10 kv.value              → event.KV.Value
//	11 kv.old_value          → event.PrevKV.Value
const sqliteAfterOldValSQL = `
	SELECT
		(SELECT MAX(rkv.id) FROM kine AS rkv),
		(SELECT MAX(crkv.prev_revision) FROM kine AS crkv WHERE crkv.name = 'compact_rev_key'),
		kv.id, kv.name, kv.created, kv.deleted, kv.create_revision, kv.prev_revision, kv.lease, kv.value, kv.old_value
	FROM kine AS kv
	WHERE kv.name LIKE ? ESCAPE '^' AND kv.id > ?
	ORDER BY kv.id ASC`

const sqliteFillSQL = `
	INSERT INTO kine(id, name, created, deleted, create_revision, prev_revision, lease, value, old_value)
	VALUES(?, ?, 0, 1, 0, 0, 0, NULL, NULL)`

// newTestDialect creates a *generic.Generic backed by an in-memory SQLite
// database with the kine table already created.
func newTestDialect(t *testing.T) *generic.Generic {
	t.Helper()
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	_, err = db.Exec(`CREATE TABLE kine (
		id               INTEGER PRIMARY KEY,
		name             TEXT,
		created          INTEGER DEFAULT 0,
		deleted          INTEGER DEFAULT 0,
		create_revision  INTEGER DEFAULT 0,
		prev_revision    INTEGER DEFAULT 0,
		lease            INTEGER DEFAULT 0,
		value            BLOB,
		old_value        BLOB
	)`)
	if err != nil {
		t.Fatalf("create kine table: %v", err)
	}

	return &generic.Generic{
		DB:             db,
		AfterOldValSQL: sqliteAfterOldValSQL,
		FillSQL:        sqliteFillSQL,
		ErrCode: func(err error) string {
			if err == nil {
				return ""
			}
			return err.Error()
		},
	}
}

// insertTestRow inserts a single row with the given id and name into the kine
// table, simulating a normal (non-deleted) key creation.
func insertTestRow(t *testing.T, db *sql.DB, id int64, name string) {
	t.Helper()
	_, err := db.Exec(
		`INSERT INTO kine(id, name, created, deleted, create_revision, prev_revision, lease, value, old_value)
		 VALUES(?, ?, 1, 0, ?, 0, 0, 'val', NULL)`,
		id, name, id,
	)
	if err != nil {
		t.Fatalf("insertTestRow id=%d: %v", id, err)
	}
}

// newTestSQLLog creates a SQLLog using the given dialect and sets its context.
// compactInterval=0 disables automatic compaction.
func newTestSQLLog(t *testing.T, d server.Dialect, ctx context.Context) *SQLLog {
	t.Helper()
	s := New(d, 0, 0, 0, 0, 512, 512)
	s.ctx = ctx
	return s
}

// TestPoll_WithDialectNotificationChannel_ForwardsEvent verifies that poll()
// wakes on a revision arriving in the dialect's NotificationChannel and
// delivers the corresponding DB row as an event to the result channel.
func TestPoll_WithDialectNotificationChannel_ForwardsEvent(t *testing.T) {
	d := newTestDialect(t)
	d.NotificationChannel = make(chan int64, 16)

	insertTestRow(t, d.DB, 11, "/registry/pods/default/mypod")

	// Pre-buffer the notification so poll receives it on its first select.
	d.NotificationChannel <- 11

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s := newTestSQLLog(t, d, ctx)
	result := make(chan server.Events, 8)
	go s.poll(result, 10)

	select {
	case events := <-result:
		cancel()
		if len(events) == 0 {
			t.Fatal("received empty events slice")
		}
		got := events[0]
		if got.KV.Key != "/registry/pods/default/mypod" {
			t.Errorf("KV.Key = %q, want %q", got.KV.Key, "/registry/pods/default/mypod")
		}
		if got.KV.ModRevision != 11 {
			t.Errorf("KV.ModRevision = %d, want 11", got.KV.ModRevision)
		}
		if !got.Create {
			t.Error("Create = false, want true")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("no event received within 3s after dialect notification")
	}
}

// TestPoll_WithDialectNotificationChannel_SkipsStaleRevision verifies that a
// notification whose revision is not greater than currentRev is ignored and no
// event is emitted.
func TestPoll_WithDialectNotificationChannel_SkipsStaleRevision(t *testing.T) {
	d := newTestDialect(t)
	d.NotificationChannel = make(chan int64, 16)

	// Revision 5 is stale relative to the starting currentRev of 10.
	d.NotificationChannel <- 5

	// Short-lived context: if no spurious event arrives before timeout, the test passes.
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	s := newTestSQLLog(t, d, ctx)
	result := make(chan server.Events, 8)
	go s.poll(result, 10)

	select {
	case events := <-result:
		// result is closed when poll exits (ctx expired); nil/empty is expected.
		if len(events) > 0 {
			t.Errorf("unexpected events after stale notification: %v", events)
		}
	case <-ctx.Done():
		// Timeout with no events — correct behaviour.
	}
}

// TestPoll_WithoutDialectNotificationChannel_UsesInternalNotify verifies that
// when the dialect has no NotificationChannel, poll() correctly responds to
// the internal s.notify channel and delivers events.
func TestPoll_WithoutDialectNotificationChannel_UsesInternalNotify(t *testing.T) {
	d := newTestDialect(t)
	// NotificationChannel intentionally left nil.

	insertTestRow(t, d.DB, 11, "/registry/configmaps/default/mymap")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s := newTestSQLLog(t, d, ctx)

	// Pre-buffer so poll receives it from s.notify on its first select.
	s.notify <- 11

	result := make(chan server.Events, 8)
	go s.poll(result, 10)

	select {
	case events := <-result:
		cancel()
		if len(events) == 0 {
			t.Fatal("received empty events slice")
		}
		if events[0].KV.Key != "/registry/configmaps/default/mymap" {
			t.Errorf("KV.Key = %q, want %q", events[0].KV.Key, "/registry/configmaps/default/mymap")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("no event received within 3s via internal notify channel")
	}
}
