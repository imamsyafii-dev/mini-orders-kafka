package store

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/imamsyafii-dev/mini-orders-kafka/internal/event"
)

func TestApplyOrderCreatedIsPersistentAndIdempotent(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "orders.db")

	store, err := OpenSQLite(path)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}

	orderA := testOrder("evt-a")

	applied, err := store.ApplyOrderCreated(ctx, orderA)
	if err != nil {
		t.Fatalf("apply first event: %v", err)
	}
	if !applied {
		t.Fatal("first event should apply its business effect")
	}

	assertCounts(t, store, 1, 1)

	applied, err = store.ApplyOrderCreated(ctx, orderA)
	if err != nil {
		t.Fatalf("apply duplicate event: %v", err)
	}
	if applied {
		t.Fatal("duplicate event must not apply its business effect")
	}

	assertCounts(t, store, 1, 1)

	if err := store.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	reopened, err := OpenSQLite(path)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	defer reopened.Close()

	applied, err = reopened.ApplyOrderCreated(ctx, orderA)
	if err != nil {
		t.Fatalf("apply persisted duplicate event: %v", err)
	}
	if applied {
		t.Fatal("persisted duplicate event must not apply its business effect")
	}

	orderB := testOrder("evt-b")

	applied, err = reopened.ApplyOrderCreated(ctx, orderB)
	if err != nil {
		t.Fatalf("apply distinct event: %v", err)
	}
	if !applied {
		t.Fatal("distinct event should apply its business effect")
	}

	assertCounts(t, reopened, 2, 2)
}

func testOrder(eventID string) event.OrderCreated {
	return event.OrderCreated{
		EventID:    eventID,
		EventType:  "order.created",
		OccurredAt: time.Now().UTC(),
		Payload: event.OrderCreatedData{
			OrderID:    "order-001",
			CustomerID: "customer-001",
			Amount:     175000,
			Sequence:   1,
		},
	}
}

func assertCounts(
	t *testing.T,
	store *SQLiteStore,
	wantProcessedEvents int,
	wantBusinessEffects int,
) {
	t.Helper()

	var processedEvents int
	if err := store.db.QueryRow(
		`SELECT COUNT(*) FROM processed_events`,
	).Scan(&processedEvents); err != nil {
		t.Fatalf("count processed events: %v", err)
	}

	var businessEffects int
	if err := store.db.QueryRow(
		`SELECT COUNT(*) FROM business_effects`,
	).Scan(&businessEffects); err != nil {
		t.Fatalf("count business effects: %v", err)
	}

	if processedEvents != wantProcessedEvents {
		t.Fatalf(
			"processed_events count = %d, want %d",
			processedEvents,
			wantProcessedEvents,
		)
	}

	if businessEffects != wantBusinessEffects {
		t.Fatalf(
			"business_effects count = %d, want %d",
			businessEffects,
			wantBusinessEffects,
		)
	}
}
