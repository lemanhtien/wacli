package store

import (
	"errors"
	"strings"

	"github.com/mattn/go-sqlite3"
)

// WithTx runs fn inside a single SQLite transaction. The *DB handed to fn is a
// transaction-scoped view of the store: every sqlc-backed method (UpsertMessage,
// UpsertChat, UpsertContact, ...) issued on it runs on the transaction, and the
// transaction is committed when fn returns nil or rolled back otherwise.
//
// Only sqlc-backed methods are transaction-aware. Hand-written helpers that go
// through the raw connection pool (search, migrations, MessageExists) must not
// be called on the transaction-scoped view: they would run on a second
// connection and block behind the write lock the transaction holds.
func (d *DB) WithTx(fn func(tx *DB) error) (err error) {
	tx, err := d.sql.Begin()
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()
	scoped := &DB{path: d.path, sql: d.sql, q: d.q.WithTx(tx), ftsEnabled: d.ftsEnabled}
	if err = fn(scoped); err != nil {
		return err
	}
	return tx.Commit()
}

// IsBusyError reports whether err is a transient SQLite lock contention error
// (SQLITE_BUSY / SQLITE_LOCKED), i.e. the write should be retried rather than
// treated as lost.
func IsBusyError(err error) bool {
	if err == nil {
		return false
	}
	var sqliteErr sqlite3.Error
	if errors.As(err, &sqliteErr) {
		return sqliteErr.Code == sqlite3.ErrBusy || sqliteErr.Code == sqlite3.ErrLocked
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "database is locked") ||
		strings.Contains(msg, "database table is locked") ||
		strings.Contains(msg, "database is busy")
}
