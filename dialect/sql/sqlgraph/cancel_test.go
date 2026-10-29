// Copyright 2019-present Facebook Inc. All rights reserved.
// This source code is licensed under the Apache 2.0 license found
// in the LICENSE file in the root directory of this source tree.

package sqlgraph

import (
	"context"
	stdsql "database/sql"
	"errors"
	"fmt"
	"testing"
	"time"

	"entgo.io/ent/dialect"
	"entgo.io/ent/dialect/sql"
	"entgo.io/ent/schema/field"

	"github.com/stretchr/testify/require"
)

// abortDriver is a dialect.Driver that counts statements and records aborts.
type abortDriver struct {
	dialectName string
	queries     int
	execs       int
	queryErr    error
	rows        sql.ColumnScanner
	abortErr    error
}

func (d *abortDriver) Dialect() string {
	if d.dialectName == "" {
		return dialect.SQLite
	}
	return d.dialectName
}

func (d *abortDriver) Close() error { return nil }

func (d *abortDriver) Tx(context.Context) (dialect.Tx, error) {
	return dialect.NopTx(d), nil
}

func (d *abortDriver) Exec(context.Context, string, any, any) error {
	d.execs++
	return nil
}

func (d *abortDriver) Query(_ context.Context, _ string, _, v any) error {
	d.queries++
	if d.queryErr != nil {
		return d.queryErr
	}
	if d.rows != nil {
		*v.(*sql.Rows) = sql.Rows{ColumnScanner: d.rows}
	}
	return nil
}

func (d *abortDriver) Abort(err error) { d.abortErr = err }

// fakeRows is a ColumnScanner that reports a context error on Err,
// and records Close calls.
type fakeRows struct {
	cols     []string
	next     bool
	closed   bool
	scanErr  error
	rowsErr  error
	scanVals []any
}

func (r *fakeRows) Close() error                       { r.closed = true; return nil }
func (r *fakeRows) Columns() ([]string, error)         { return r.cols, nil }
func (r *fakeRows) ColumnTypes() ([]*stdsql.ColumnType, error) {
	return nil, nil
}
func (r *fakeRows) Err() error           { return r.rowsErr }
func (r *fakeRows) Next() bool           { return r.next }
func (r *fakeRows) NextResultSet() bool  { return false }
func (r *fakeRows) Scan(dest ...any) error {
	r.scanVals = dest
	return r.scanErr
}

func nodesSpec() *QuerySpec {
	return &QuerySpec{
		Node: &NodeSpec{
			Table:   "users",
			Columns: []string{"id", "name"},
			ID:      &FieldSpec{Column: "id", Type: field.TypeInt},
		},
		ScanValues: func(columns []string) ([]any, error) {
			return make([]any, len(columns)), nil
		},
		Assign: func(columns []string, values []any) error { return nil },
	}
}

func edgeSpec() *EdgeQuerySpec {
	return &EdgeQuerySpec{
		Edge: &EdgeSpec{
			Table:   "user_groups",
			Columns: []string{"group_id", "user_id"},
		},
		ScanValues: func() [2]any { return [2]any{new(int64), new(int64)} },
		Assign:     func(out, in any) error { return nil },
	}
}

func TestAbortErrorClassification(t *testing.T) {
	canceledCtx := canceledContext()
	deadlineCtx := deadlineContext()

	for _, tc := range []struct {
		name     string
		ctx      context.Context
		kind     AbortKind
		canceled bool
		deadline bool
		sentinel error
	}{
		{"canceled", canceledCtx, KindCanceled, true, false, context.Canceled},
		{"deadline", deadlineCtx, KindDeadline, false, true, context.DeadlineExceeded},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := CheckRead(tc.ctx, nil, OpQueryNodes, StageEntry)
			require.Error(t, err)
			var ae *AbortError
			require.True(t, errors.As(err, &ae))
			require.Equal(t, tc.kind, ae.Kind)
			require.Equal(t, OpQueryNodes, ae.Op)
			require.Equal(t, StageEntry, ae.Stage)
			require.True(t, errors.Is(err, tc.sentinel))
			require.Equal(t, tc.canceled, IsCanceled(err))
			require.Equal(t, tc.deadline, IsDeadlineExceeded(err))
			require.True(t, IsAborted(err))
		})
	}
}

func TestCheckReadAlive(t *testing.T) {
	require.NoError(t, CheckRead(context.Background(), nil, OpQueryNodes, StageEntry))
}

func TestQueryNodesAbortedAtEntry(t *testing.T) {
	for _, tc := range []struct {
		name     string
		ctx      context.Context
		kind     AbortKind
		sentinel error
	}{
		{"canceled", canceledContext(), KindCanceled, context.Canceled},
		{"deadline", deadlineContext(), KindDeadline, context.DeadlineExceeded},
	} {
		t.Run(tc.name, func(t *testing.T) {
			drv := &abortDriver{}
			err := QueryNodes(tc.ctx, drv, nodesSpec())
			requireAbort(t, err, tc.kind, OpQueryNodes, StageEntry, "users", tc.sentinel)
			// No statement was sent.
			require.Zero(t, drv.queries)
			// The transactional driver was marked non-continuable.
			requireAbortMarked(t, drv.abortErr, tc.kind, OpQueryNodes, StageEntry)
		})
	}
}

func TestCountNodesAbortedAtEntry(t *testing.T) {
	drv := &abortDriver{}
	n, err := CountNodes(deadlineContext(), drv, nodesSpec())
	require.Equal(t, 0, n)
	requireAbort(t, err, KindDeadline, OpCountNodes, StageEntry, "users", context.DeadlineExceeded)
	require.Zero(t, drv.queries)
	requireAbortMarked(t, drv.abortErr, KindDeadline, OpCountNodes, StageEntry)
}

func TestQueryEdgesAbortedAtEntry(t *testing.T) {
	drv := &abortDriver{}
	err := QueryEdges(canceledContext(), drv, edgeSpec())
	requireAbort(t, err, KindCanceled, OpQueryEdges, StageEntry, "user_groups", context.Canceled)
	require.Zero(t, drv.queries)
	requireAbortMarked(t, drv.abortErr, KindCanceled, OpQueryEdges, StageEntry)
}

func TestDerivedStatementRechecked(t *testing.T) {
	drv := &abortDriver{rows: &fakeRows{cols: []string{"id", "name"}, next: false, rowsErr: nil}}
	spec := nodesSpec()

	// The first statement completes successfully.
	require.NoError(t, QueryNodes(context.Background(), drv, spec))
	require.Equal(t, 1, drv.queries)
	require.Nil(t, drv.abortErr)

	// The deadline expires before the second (derived) statement. It must
	// not be sent and the open driver is marked aborted.
	err := QueryNodes(deadlineContext(), drv, spec)
	requireAbort(t, err, KindDeadline, OpQueryNodes, StageEntry, "users", context.DeadlineExceeded)
	require.Equal(t, 1, drv.queries, "no further statement must be sent after abort")
	requireAbortMarked(t, drv.abortErr, KindDeadline, OpQueryNodes, StageEntry)
}

func TestStatementDeadlineFromDriver(t *testing.T) {
	drv := &abortDriver{queryErr: context.DeadlineExceeded}
	err := QueryNodes(context.Background(), drv, nodesSpec())
	requireAbort(t, err, KindDeadline, OpQueryNodes, StageStatement, "users", context.DeadlineExceeded)
	require.Equal(t, 1, drv.queries)
	requireAbortMarked(t, drv.abortErr, KindDeadline, OpQueryNodes, StageStatement)
}

func TestStatementWrappedCancelFromDriver(t *testing.T) {
	drv := &abortDriver{queryErr: fmt.Errorf("driver: %w", context.Canceled)}
	err := QueryEdges(context.Background(), drv, edgeSpec())
	requireAbort(t, err, KindCanceled, OpQueryEdges, StageStatement, "user_groups", context.Canceled)
	requireAbortMarked(t, drv.abortErr, KindCanceled, OpQueryEdges, StageStatement)
}

func TestNonContextErrorPassesThrough(t *testing.T) {
	boom := errors.New("boom")
	drv := &abortDriver{queryErr: boom}
	err := QueryNodes(context.Background(), drv, nodesSpec())
	require.Equal(t, boom, err)
	require.False(t, IsAborted(err))
	require.Nil(t, drv.abortErr)
}

func TestAbortDuringScanClosesRows(t *testing.T) {
	rows := &fakeRows{cols: []string{"id", "name"}, next: false, rowsErr: context.Canceled}
	drv := &abortDriver{rows: rows}
	err := QueryNodes(context.Background(), drv, nodesSpec())
	requireAbort(t, err, KindCanceled, OpQueryNodes, StageScan, "users", context.Canceled)
	require.True(t, rows.closed, "the open result set must be closed after abort")
	require.Equal(t, 1, drv.queries)
	requireAbortMarked(t, drv.abortErr, KindCanceled, OpQueryNodes, StageScan)
}

func TestAbortDuringScanRowError(t *testing.T) {
	rows := &fakeRows{cols: []string{"id", "name"}, next: true, scanErr: context.DeadlineExceeded}
	drv := &abortDriver{rows: rows}
	err := QueryNodes(context.Background(), drv, nodesSpec())
	requireAbort(t, err, KindDeadline, OpQueryNodes, StageScan, "users", context.DeadlineExceeded)
	require.True(t, rows.closed)
}

func TestSuccessPathUnaffected(t *testing.T) {
	rows := &fakeRows{cols: []string{"id", "name"}, next: false}
	drv := &abortDriver{rows: rows}
	require.NoError(t, QueryNodes(context.Background(), drv, nodesSpec()))
	require.True(t, rows.closed)
	require.Equal(t, 1, drv.queries)
	require.Nil(t, drv.abortErr)
}

func TestQueryRowsGuard(t *testing.T) {
	t.Run("aborted before statement", func(t *testing.T) {
		drv := &abortDriver{}
		err := QueryRows(deadlineContext(), drv, OpQueryEdges, "SELECT 1", nil, &sql.Rows{})
		requireAbort(t, err, KindDeadline, OpQueryEdges, StageEntry, "", context.DeadlineExceeded)
		require.Zero(t, drv.queries)
	})
	t.Run("success", func(t *testing.T) {
		rows := &fakeRows{cols: []string{"id"}}
		drv := &abortDriver{rows: rows}
		out := &sql.Rows{}
		require.NoError(t, QueryRows(context.Background(), drv, OpQueryEdges, "SELECT 1", nil, out))
		require.Equal(t, 1, drv.queries)
	})
}

func requireAbort(t *testing.T, err error, kind AbortKind, op ReadOp, stage ReadStage, target string, sentinel error) {
	t.Helper()
	require.Error(t, err)
	var ae *AbortError
	require.True(t, errors.As(err, &ae), "expected *AbortError, got %v", err)
	require.Equal(t, kind, ae.Kind)
	require.Equal(t, op, ae.Op)
	require.Equal(t, stage, ae.Stage)
	require.Equal(t, target, ae.Target)
	require.True(t, errors.Is(err, sentinel))
}

func requireAbortMarked(t *testing.T, err error, kind AbortKind, op ReadOp, stage ReadStage) {
	t.Helper()
	sentinel := context.Canceled
	target := ""
	if kind == KindDeadline {
		sentinel = context.DeadlineExceeded
	}
	switch op {
	case OpQueryNodes, OpCountNodes:
		target = "users"
	case OpQueryEdges:
		target = "user_groups"
	}
	requireAbort(t, err, kind, op, stage, target, sentinel)
}

func canceledContext() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx
}

func deadlineContext() context.Context {
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Millisecond))
	<-ctx.Done()
	// The deadline already fired (DeadlineExceeded); cancel is a no-op now.
	_ = cancel
	return ctx
}
