package metrics

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

// TestObserveNotification_IncrementsCounter verifies that each call to
// ObserveNotification increments the kine_notifications_total counter for
// the specified result label by exactly one.
func TestObserveNotification_IncrementsCounter(t *testing.T) {
	results := []string{"success", "error", "dropped", "parse_error"}
	for _, result := range results {
		before := testutil.ToFloat64(NotificationTotal.WithLabelValues(result))
		ObserveNotification("postgres", result, time.Millisecond)
		after := testutil.ToFloat64(NotificationTotal.WithLabelValues(result))
		if diff := after - before; diff != 1.0 {
			t.Errorf("result=%q: counter delta = %v, want 1.0", result, diff)
		}
	}
}

// TestObserveNotification_DoesNotPanic verifies that a range of latency values
// (including zero and large durations) do not cause a panic.
func TestObserveNotification_DoesNotPanic(t *testing.T) {
	latencies := []time.Duration{0, time.Microsecond, time.Millisecond, 100 * time.Millisecond, time.Second, 10 * time.Second}
	for _, lat := range latencies {
		ObserveNotification("postgres", "success", lat)
	}
}

// TestSetNotificationQueueSize verifies that SetNotificationQueueSize sets and
// updates the kine_notification_queue_size gauge to the provided value.
func TestSetNotificationQueueSize(t *testing.T) {
	SetNotificationQueueSize("postgres", 42.0)
	if got := testutil.ToFloat64(NotificationQueueSize.WithLabelValues("postgres")); got != 42.0 {
		t.Errorf("after Set(42): got %v, want 42.0", got)
	}

	SetNotificationQueueSize("postgres", 0.0)
	if got := testutil.ToFloat64(NotificationQueueSize.WithLabelValues("postgres")); got != 0.0 {
		t.Errorf("after Set(0): got %v, want 0.0", got)
	}
}

// TestSetNotificationQueueSize_MultipleDrivers verifies that each driver label
// is tracked independently.
func TestSetNotificationQueueSize_MultipleDrivers(t *testing.T) {
	SetNotificationQueueSize("postgres", 10.0)
	SetNotificationQueueSize("mysql", 20.0)

	if got := testutil.ToFloat64(NotificationQueueSize.WithLabelValues("postgres")); got != 10.0 {
		t.Errorf("postgres queue size = %v, want 10.0", got)
	}
	if got := testutil.ToFloat64(NotificationQueueSize.WithLabelValues("mysql")); got != 20.0 {
		t.Errorf("mysql queue size = %v, want 20.0", got)
	}
}
