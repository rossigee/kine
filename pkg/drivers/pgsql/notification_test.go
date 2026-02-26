package pgsql

import (
	"context"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// postgresTestDSN returns the DSN for integration tests, or skips if not set.
func postgresTestDSN(t *testing.T) string {
	t.Helper()
	dsn := os.Getenv("KINE_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("KINE_TEST_POSTGRES_DSN not set; skipping PostgreSQL integration test")
	}
	return dsn
}

// TestStartNotificationListener_ExitsOnCancelledContext verifies that when the
// context is already cancelled the listener goroutine returns without blocking.
func TestStartNotificationListener_ExitsOnCancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	ch := make(chan int64, 1)
	done := make(chan struct{})
	go func() {
		startNotificationListener(ctx, "postgres://invalid:invalid@localhost/nonexistent", ch)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("startNotificationListener did not exit within 2s after context cancellation")
	}
}

// TestStartNotificationListener_Integration_ForwardsRevisionToChannel verifies
// that a pg_notify on kine_changes is forwarded to the notification channel.
func TestStartNotificationListener_Integration_ForwardsRevisionToChannel(t *testing.T) {
	dsn := postgresTestDSN(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch := make(chan int64, 16)
	go startNotificationListener(ctx, dsn, ch)

	// Give the listener time to connect and issue LISTEN.
	time.Sleep(300 * time.Millisecond)

	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect for pg_notify: %v", err)
	}
	defer conn.Close(ctx)

	const expectedRev = int64(42)
	_, err = conn.Exec(ctx, "SELECT pg_notify('kine_changes', $1)", strconv.FormatInt(expectedRev, 10))
	if err != nil {
		t.Fatalf("pg_notify: %v", err)
	}

	select {
	case got := <-ch:
		if got != expectedRev {
			t.Errorf("received revision %d, want %d", got, expectedRev)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("notification not received within 3s")
	}
}

// TestStartNotificationListener_Integration_DropsWhenChannelFull verifies that
// notifications are dropped (non-blocking) when the output channel is full,
// and the listener does not deadlock.
func TestStartNotificationListener_Integration_DropsWhenChannelFull(t *testing.T) {
	dsn := postgresTestDSN(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Unbuffered channel: every send via the select-default branch will be dropped.
	ch := make(chan int64)
	go startNotificationListener(ctx, dsn, ch)

	time.Sleep(300 * time.Millisecond)

	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect for pg_notify: %v", err)
	}
	defer conn.Close(ctx)

	for i := 1; i <= 10; i++ {
		_, err = conn.Exec(ctx, "SELECT pg_notify('kine_changes', $1)", strconv.Itoa(i))
		if err != nil {
			t.Fatalf("pg_notify[%d]: %v", i, err)
		}
	}

	// The listener must still be alive and accepting new notifications.
	time.Sleep(200 * time.Millisecond)
	_, err = conn.Exec(ctx, "SELECT pg_notify('kine_changes', '99')")
	if err != nil {
		t.Fatalf("pg_notify after drops: %v", err)
	}
}

// TestStartNotificationListener_Integration_IgnoresInvalidPayload verifies that
// a non-integer payload does not crash the listener and subsequent valid
// notifications are still forwarded.
func TestStartNotificationListener_Integration_IgnoresInvalidPayload(t *testing.T) {
	dsn := postgresTestDSN(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch := make(chan int64, 16)
	go startNotificationListener(ctx, dsn, ch)

	time.Sleep(300 * time.Millisecond)

	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect for pg_notify: %v", err)
	}
	defer conn.Close(ctx)

	// Invalid payload first.
	_, err = conn.Exec(ctx, "SELECT pg_notify('kine_changes', 'not-a-number')")
	if err != nil {
		t.Fatalf("pg_notify invalid: %v", err)
	}

	// Valid payload immediately after.
	const expectedRev = int64(99)
	_, err = conn.Exec(ctx, "SELECT pg_notify('kine_changes', $1)", strconv.FormatInt(expectedRev, 10))
	if err != nil {
		t.Fatalf("pg_notify valid: %v", err)
	}

	select {
	case got := <-ch:
		if got != expectedRev {
			t.Errorf("received %d, want %d", got, expectedRev)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("listener did not forward valid notification after invalid payload within 3s")
	}
}
