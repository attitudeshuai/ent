// Copyright 2019-present Facebook Inc. All rights reserved.
// This source code is licensed under the Apache 2.0 license found
// in the LICENSE file in the root directory of this source tree.

package ent

import (
	"context"
	stdsql "database/sql"
	"database/sql/driver"
	"errors"
	"testing"
	"time"

	"entgo.io/ent/dialect"
	entsql "entgo.io/ent/dialect/sql"
	"entgo.io/ent/dialect/sql/sqlgraph"

	"github.com/stretchr/testify/require"
)

// fakeReadTx is a dialect.Tx that records the statements it executed.
type fakeReadTx struct {
	queries   int
	committed bool
	rolled    bool
	rows      entsql.ColumnScanner
}

func (*fakeReadTx) Exec(context.Context, string, any, any) error { return nil }

func (tx *fakeReadTx) Query(_ context.Context, _ string, _, v any) error {
	tx.queries++
	rows := tx.rows
	if rows == nil {
		rows = emptyRows{}
	}
	*v.(*entsql.Rows) = entsql.Rows{ColumnScanner: rows}
	return nil
}

func (tx *fakeReadTx) Commit() error   { tx.committed = true; return nil }
func (tx *fakeReadTx) Rollback() error { tx.rolled = true; return nil }

// fakeReadDriver starts fakeReadTx transactions.
type fakeReadDriver struct {
	dialectName string
	rows        entsql.ColumnScanner
}

func (fakeReadDriver) Close() error { return nil }
func (d fakeReadDriver) Dialect() string {
	if d.dialectName == "" {
		return dialect.SQLite
	}
	return d.dialectName
}
func (fakeReadDriver) Exec(context.Context, string, any, any) error { return nil }
func (fakeReadDriver) Query(context.Context, string, any, any) error {
	return nil
}
func (d fakeReadDriver) Tx(context.Context) (dialect.Tx, error) {
	return &fakeReadTx{rows: d.rows}, nil
}

// emptyRows is a zero-row ColumnScanner used by the fake driver.
type emptyRows struct{}

func (emptyRows) Close() error                               { return nil }
func (emptyRows) Columns() ([]string, error)                 { return []string{"id"}, nil }
func (emptyRows) ColumnTypes() ([]*stdsql.ColumnType, error) { return nil, nil }
func (emptyRows) Err() error                                 { return nil }
func (emptyRows) Next() bool                                 { return false }
func (emptyRows) NextResultSet() bool                        { return false }
func (emptyRows) Scan(...any) error                          { return nil }

// oneRowRows returns a single row with an id, then runs afterFirstRow
// (used to cancel/expire the context once the main result set was consumed).
type oneRowRows struct {
	cols          []string
	values        []any
	nexted        bool
	afterFirstRow func()
}

func (r *oneRowRows) Close() error                               { return nil }
func (r *oneRowRows) Columns() ([]string, error)                 { return r.cols, nil }
func (r *oneRowRows) ColumnTypes() ([]*stdsql.ColumnType, error) { return nil, nil }
func (r *oneRowRows) Err() error                                 { return nil }
func (r *oneRowRows) NextResultSet() bool                        { return false }
func (r *oneRowRows) Next() bool {
	if !r.nexted {
		r.nexted = true
		return true
	}
	if r.afterFirstRow != nil {
		r.afterFirstRow()
	}
	return false
}
func (r *oneRowRows) Scan(dest ...any) error {
	for i := range dest {
		switch d := dest[i].(type) {
		case *stdsql.NullInt64:
			d.Valid = true
			d.Int64 = r.values[i].(int64)
		}
	}
	return nil
}

var _ driver.Tx = (*fakeReadTx)(nil)

func TestTxReadAbortedIsNonContinuable(t *testing.T) {
	for _, tc := range []struct {
		name     string
		aborted  func() context.Context
		isKind   func(error) bool
		sentinel error
	}{
		{
			name:     "canceled",
			aborted:  canceledCtx,
			isKind:   sqlgraph.IsCanceled,
			sentinel: context.Canceled,
		},
		{
			name:     "deadline",
			aborted:  expiredCtx,
			isKind:   sqlgraph.IsDeadlineExceeded,
			sentinel: context.DeadlineExceeded,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := NewClient(Driver(fakeReadDriver{}))
			tx, err := client.Tx(context.Background())
			require.NoError(t, err)

			// The non-aborted path is unaffected: a statement is sent.
			users, err := tx.User.Query().All(context.Background())
			require.NoError(t, err)
			require.Empty(t, users)

			// A read with an aborted context returns the structured abort
			// error and no statement is sent.
			_, err = tx.User.Query().All(tc.aborted())
			require.Error(t, err)
			require.True(t, sqlgraph.IsAborted(err))
			require.True(t, tc.isKind(err))
			require.True(t, errors.Is(err, tc.sentinel))
			var ae *sqlgraph.AbortError
			require.True(t, errors.As(err, &ae))
			require.Equal(t, sqlgraph.OpQueryNodes, ae.Op)
			require.Equal(t, sqlgraph.StageEntry, ae.Stage)

			// The transaction is non-continuable: further statements are
			// rejected before reaching the driver.
			_, err = tx.User.Query().All(context.Background())
			require.Error(t, err)
			require.True(t, errors.Is(err, dialect.ErrTxAborted))

			// Count is rejected as well.
			_, err = tx.User.Query().Count(context.Background())
			require.Error(t, err)
			require.True(t, errors.Is(err, dialect.ErrTxAborted))

			// Commit must fail rather than silently succeeding.
			err = tx.Commit()
			require.Error(t, err)
			require.True(t, errors.Is(err, dialect.ErrTxAborted))
			require.True(t, errors.Is(err, tc.sentinel))

			// The caller can still roll the transaction back explicitly.
			require.NoError(t, tx.Rollback())
		})
	}
}

func TestTxReadAbortedAfterFirstStatement(t *testing.T) {
	client := NewClient(Driver(fakeReadDriver{}))
	tx, err := client.Tx(context.Background())
	require.NoError(t, err)

	// Simulate the driver reporting a timeout for an in-flight statement.
	// The transaction must become non-continuable.
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	<-ctx.Done()
	_, err = tx.User.Query().All(ctx)
	require.True(t, sqlgraph.IsDeadlineExceeded(err))

	_, err = tx.User.Query().All(context.Background())
	require.True(t, errors.Is(err, dialect.ErrTxAborted))
	require.Error(t, tx.Commit())
	require.NoError(t, tx.Rollback())
}

func TestTxEagerLoadAbortedBeforeDerivedStatement(t *testing.T) {
	for _, tc := range []struct {
		name   string
		expire func(cancel context.CancelFunc)
		isKind func(error) bool
	}{
		{"canceled", func(cancel context.CancelFunc) { cancel() }, sqlgraph.IsCanceled},
		{"deadline", func(context.CancelFunc) { time.Sleep(5 * time.Millisecond) }, sqlgraph.IsDeadlineExceeded},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Millisecond)
			defer cancel()
			rows := &oneRowRows{
				cols:          []string{"id"},
				values:        []any{int64(1)},
				afterFirstRow: func() { tc.expire(cancel) },
			}
			client := NewClient(Driver(fakeReadDriver{rows: rows}))
			tx, err := client.Tx(context.Background())
			require.NoError(t, err)

			// The main statement returns one node; the context expires right
			// after it is consumed. The eager-loading statement must be
			// revalidated and never sent.
			users, err := tx.User.Query().WithGroups().All(ctx)
			require.Error(t, err)
			require.Nil(t, users)
			var ae *sqlgraph.AbortError
			require.True(t, errors.As(err, &ae), "got %v", err)
			require.Equal(t, sqlgraph.OpQueryNodes, ae.Op)
			require.Equal(t, sqlgraph.StageEagerLoad, ae.Stage)
			require.True(t, tc.isKind(err))

			ftx := tx.config.driver.(*txDriver).tx.(*fakeReadTx)
			require.Equal(t, 1, ftx.queries, "no eager-loading statement must be sent after the abort")

			// The transaction is poisoned: commit rejected, rollback allowed.
			require.True(t, errors.Is(tx.Commit(), dialect.ErrTxAborted))
			require.NoError(t, tx.Rollback())
		})
	}
}

func TestNonAbortedTxCommits(t *testing.T) {
	client := NewClient(Driver(fakeReadDriver{}))
	tx, err := client.Tx(context.Background())
	require.NoError(t, err)
	_, err = tx.User.Query().All(context.Background())
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
}

func canceledCtx() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx
}

func expiredCtx() context.Context {
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Millisecond))
	<-ctx.Done()
	_ = cancel
	return ctx
}
