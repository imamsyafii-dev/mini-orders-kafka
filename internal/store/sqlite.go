package store

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/imamsyafii-dev/mini-orders-kafka/internal/event"
	_ "modernc.org/sqlite"
)

type SQLiteStore struct {
	db *sql.DB
}

func OpenSQLite(path string) (*SQLiteStore, error) {
	dir := filepath.Dir(path)
	if dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, fmt.Errorf("create database directory: %w", err)
		}
	}

	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("open SQLite database: %w", err)
	}

	db.SetMaxOpenConns(1)

	store := &SQLiteStore{db: db}
	if err := store.initialize(); err != nil {
		_ = db.Close()
		return nil, err
	}

	return store, nil
}

func (s *SQLiteStore) Close() error {
	return s.db.Close()
}

func (s *SQLiteStore) initialize() error {
	const schema = `
CREATE TABLE IF NOT EXISTS processed_events (
	event_id TEXT PRIMARY KEY,
	event_type TEXT NOT NULL,
	processed_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS business_effects (
	event_id TEXT PRIMARY KEY,
	order_id TEXT NOT NULL,
	amount INTEGER NOT NULL,
	applied_at TEXT NOT NULL
);
`

	if _, err := s.db.Exec(schema); err != nil {
		return fmt.Errorf("initialize database schema: %w", err)
	}

	return nil
}

func (s *SQLiteStore) ApplyOrderCreated(
	ctx context.Context,
	order event.OrderCreated,
) (bool, error) {
	if order.EventID == "" {
		return false, fmt.Errorf("event_id is required")
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, fmt.Errorf("begin transaction: %w", err)
	}

	now := time.Now().UTC().Format(time.RFC3339Nano)

	result, err := tx.ExecContext(
		ctx,
		`INSERT OR IGNORE INTO processed_events (
			event_id,
			event_type,
			processed_at
		) VALUES (?, ?, ?)`,
		order.EventID,
		order.EventType,
		now,
	)
	if err != nil {
		_ = tx.Rollback()
		return false, fmt.Errorf("insert processed event: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		_ = tx.Rollback()
		return false, fmt.Errorf("read processed event result: %w", err)
	}

	if rowsAffected == 0 {
		if err := tx.Commit(); err != nil {
			return false, fmt.Errorf("commit duplicate event transaction: %w", err)
		}

		return false, nil
	}

	_, err = tx.ExecContext(
		ctx,
		`INSERT INTO business_effects (
			event_id,
			order_id,
			amount,
			applied_at
		) VALUES (?, ?, ?, ?)`,
		order.EventID,
		order.Payload.OrderID,
		order.Payload.Amount,
		now,
	)
	if err != nil {
		_ = tx.Rollback()
		return false, fmt.Errorf("apply business effect: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("commit business transaction: %w", err)
	}

	return true, nil
}
