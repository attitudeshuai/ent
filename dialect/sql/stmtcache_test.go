// Copyright 2019-present Facebook Inc. All rights reserved.
// This source code is licensed under the Apache 2.0 license found
// in the LICENSE file in the root directory of this source tree.

package sql

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"testing"

	"entgo.io/ent/dialect"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"
)

const cachedQuery = "SELECT id FROM users WHERE id = ?"

// errStaleStmt emulates a MySQL ER_UNKNOWN_STMT_HANDLER error reported when
// the server no longer holds the prepared statement handle.
var errStaleStmt = errors.New("error 1243 (HY000): unknown prepared statement handler (7) given to mysqld_stmt_execute")

func cachedRows(values ...int) *sqlmock.Rows {
	rows := sqlmock.NewRows([]string{"id"})
	for _, v := range values {
		rows.AddRow(v)
	}
	return rows
}

func requireCachedIDs(t *testing.T, drv *Driver, want ...int) {
	t.Helper()
	vr := &Rows{}
	err := drv.Query(context.Background(), cachedQuery, []any{want[len(want)-1]}, vr)
	require.NoError(t, err)
	var got []int
	for vr.Next() {
		var id int
		require.NoError(t, vr.Scan(&id))
		got = append(got, id)
	}
	require.NoError(t, vr.Err())
	require.NoError(t, vr.Close())
	require.Equal(t, want, got, "visible result order must not change")
}

// eventRecorder collects observed cache events.
type eventRecorder struct {
	mu     sync.Mutex
	events []StmtCacheEvent
}

func (r *eventRecorder) observer() func(StmtCacheEvent) {
	return func(e StmtCacheEvent) {
		r.mu.Lock()
		r.events = append(r.events, e)
		r.mu.Unlock()
	}
}

func (r *eventRecorder) kinds() []StmtCacheEventKind {
	r.mu.Lock()
	defer r.mu.Unlock()
	kinds := make([]StmtCacheEventKind, len(r.events))
	for i := range r.events {
		kinds[i] = r.events[i].Kind
	}
	return kinds
}

func (r *eventRecorder) scopes() []StmtCacheScope {
	r.mu.Lock()
	defer r.mu.Unlock()
	scopes := make([]StmtCacheScope, len(r.events))
	for i := range r.events {
		scopes[i] = r.events[i].Scope
	}
	return scopes
}

func (r *eventRecorder) reasons() []InvalidationReason {
	r.mu.Lock()
	defer r.mu.Unlock()
	var reasons []InvalidationReason
	for _, e := range r.events {
		if e.Kind == StmtCacheInvalidate || e.Kind == StmtCacheEvict {
			reasons = append(reasons, e.Reason)
		}
	}
	return reasons
}

func TestStmtCacheDisabledByDefault(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	require.NoError(t, err)
	drv := OpenDB(dialect.MySQL, db)

	_, enabled := drv.StmtCacheStats()
	require.False(t, enabled, "reuse must be opt-in")

	// No prepared statements are expected: every execution stays on the
	// raw path, exactly as before.
	mock.ExpectQuery(cachedQuery).WillReturnRows(cachedRows(1))
	mock.ExpectQuery(cachedQuery).WillReturnRows(cachedRows(2))
	mock.ExpectExec("INSERT INTO users (id) VALUES (?)").
		WillReturnResult(sqlmock.NewResult(0, 1))
	requireCachedIDs(t, drv, 1)
	requireCachedIDs(t, drv, 2)
	require.NoError(t, drv.Exec(context.Background(), "INSERT INTO users (id) VALUES (?)", []any{1}, nil))
	require.NoError(t, mock.ExpectationsWereMet())

	// Explicit invalidation is a harmless no-op when disabled.
	drv.InvalidateStmtCache()
}

func TestStmtCacheHitByShapeAndArgCount(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	require.NoError(t, err)
	var rec eventRecorder
	drv := OpenDB(dialect.MySQL, db, WithStmtCache(StmtCacheObserver(rec.observer())))

	// Same shape twice: a single prepare, two statement executions.
	prep := mock.ExpectPrepare(cachedQuery)
	prep.ExpectQuery().WithArgs(1).WillReturnRows(cachedRows(1))
	prep.ExpectQuery().WithArgs(2).WillReturnRows(cachedRows(2))
	requireCachedIDs(t, drv, 1)
	requireCachedIDs(t, drv, 2)
	require.NoError(t, mock.ExpectationsWereMet())

	stats, enabled := drv.StmtCacheStats()
	require.True(t, enabled)
	require.Equal(t, int64(1), stats.Misses)
	require.Equal(t, int64(1), stats.Hits)
	require.Equal(t, int64(1), stats.Prepares)
	require.Equal(t, int64(0), stats.Rebuilds)
	require.Equal(t, int64(0), stats.Evictions)
	require.Equal(t, int64(1), stats.ActiveStmts)
	require.Equal(t, []StmtCacheEventKind{StmtCacheMiss, StmtCacheHit}, rec.kinds())
}

func TestStmtCacheDistinctArgCount(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	require.NoError(t, err)
	drv := OpenDB(dialect.MySQL, db, WithStmtCache())

	// Same text with a different parameter count is a different entry.
	const q = "SELECT id FROM users WHERE id IN (?, ?)"
	p1 := mock.ExpectPrepare(cachedQuery)
	p1.ExpectQuery().WithArgs(1).WillReturnRows(cachedRows(1))
	p2 := mock.ExpectPrepare(q)
	p2.ExpectQuery().WithArgs(1, 2).WillReturnRows(cachedRows(2))
	p2.ExpectQuery().WithArgs(3, 4).WillReturnRows(cachedRows(4))

	requireCachedIDs(t, drv, 1)
	vr := &Rows{}
	require.NoError(t, drv.Query(context.Background(), q, []any{1, 2}, vr))
	require.NoError(t, vr.Close())
	vr = &Rows{}
	require.NoError(t, drv.Query(context.Background(), q, []any{3, 4}, vr))
	require.NoError(t, vr.Close())
	require.NoError(t, mock.ExpectationsWereMet())

	stats, _ := drv.StmtCacheStats()
	require.Equal(t, int64(2), stats.Misses)
	require.Equal(t, int64(1), stats.Hits)
	require.Equal(t, int64(2), stats.ActiveStmts)
}

func TestStmtCacheLRUEviction(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	require.NoError(t, err)
	var rec eventRecorder
	drv := OpenDB(dialect.MySQL, db,
		WithStmtCache(StmtCacheMaxEntries(2), StmtCacheObserver(rec.observer())))

	const qa, qb, qc = "SELECT a", "SELECT b", "SELECT c"
	pa := mock.ExpectPrepare(qa)
	pa.ExpectQuery().WillReturnRows(cachedRows(1))
	pb := mock.ExpectPrepare(qb)
	pb.ExpectQuery().WillReturnRows(cachedRows(2))
	pc := mock.ExpectPrepare(qc)
	pc.ExpectQuery().WillReturnRows(cachedRows(3))
	// Adding c evicts the least-recently-used entry a.
	pa.WillBeClosed()

	query := func(q string, v int) {
		vr := &Rows{}
		require.NoError(t, drv.Query(context.Background(), q, []any{}, vr))
		require.True(t, vr.Next())
		var id int
		require.NoError(t, vr.Scan(&id))
		require.Equal(t, v, id)
		require.NoError(t, vr.Close())
	}
	query(qa, 1)
	query(qb, 2)
	query(qc, 3)
	stats, _ := drv.StmtCacheStats()
	require.Equal(t, int64(1), stats.Evictions)
	require.Equal(t, int64(2), stats.ActiveStmts)

	// b was used most recently before re-creating a, so c is the LRU now.
	pb.ExpectQuery().WillReturnRows(cachedRows(22))
	query(qb, 22)
	pa2 := mock.ExpectPrepare(qa)
	pa2.ExpectQuery().WillReturnRows(cachedRows(11))
	pc.WillBeClosed()
	query(qa, 11)
	require.NoError(t, mock.ExpectationsWereMet())

	stats, _ = drv.StmtCacheStats()
	require.Equal(t, int64(2), stats.Evictions)
	require.Equal(t, int64(2), stats.ActiveStmts)
	require.Contains(t, rec.reasons(), InvalidateEviction)
}

// fifoPolicy is a custom declared policy for tests.
type fifoPolicy struct {
	order []StmtCacheKey
	used  int
}

func (p *fifoPolicy) Added(k StmtCacheKey) { p.order = append(p.order, k) }
func (p *fifoPolicy) Used(StmtCacheKey)    { p.used++ }
func (p *fifoPolicy) Removed(k StmtCacheKey) {
	for i, kk := range p.order {
		if kk == k {
			p.order = append(p.order[:i], p.order[i+1:]...)
			return
		}
	}
}
func (p *fifoPolicy) Victim() (StmtCacheKey, bool) {
	if len(p.order) == 0 {
		return StmtCacheKey{}, false
	}
	return p.order[0], true
}

func TestStmtCacheCustomPolicy(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	require.NoError(t, err)
	policy := &fifoPolicy{}
	drv := OpenDB(dialect.MySQL, db, WithStmtCache(
		StmtCacheMaxEntries(2),
		StmtCacheEvictionPolicy(func() EvictionPolicy { return policy }),
	))

	const qa, qb, qc = "SELECT a", "SELECT b", "SELECT c"
	pa := mock.ExpectPrepare(qa)
	pa.ExpectQuery().WillReturnRows(cachedRows(1))
	pb := mock.ExpectPrepare(qb)
	pb.ExpectQuery().WillReturnRows(cachedRows(2))
	pa.ExpectQuery().WillReturnRows(cachedRows(11)) // hit on a
	pc := mock.ExpectPrepare(qc)
	pc.ExpectQuery().WillReturnRows(cachedRows(3))
	// FIFO removes the first-added entry regardless of recent use.
	pa.WillBeClosed()

	query := func(q string) {
		vr := &Rows{}
		require.NoError(t, drv.Query(context.Background(), q, []any{}, vr))
		require.NoError(t, vr.Close())
	}
	query(qa)
	query(qb)
	query(qa) // touches a, but FIFO still evicts it first
	require.Greater(t, policy.used, 0)
	query(qc)
	require.NoError(t, mock.ExpectationsWereMet())
	stats, _ := drv.StmtCacheStats()
	require.Equal(t, int64(1), stats.Evictions)
}

// brokenPolicy always reports an untracked key as victim.
type brokenPolicy struct{}

func (brokenPolicy) Added(StmtCacheKey)           {}
func (brokenPolicy) Used(StmtCacheKey)            {}
func (brokenPolicy) Removed(StmtCacheKey)         {}
func (brokenPolicy) Victim() (StmtCacheKey, bool) { return StmtCacheKey{Query: "ghost"}, true }

func TestStmtCacheBrokenPolicyDoesNotSpin(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	require.NoError(t, err)
	drv := OpenDB(dialect.MySQL, db, WithStmtCache(
		StmtCacheMaxEntries(1),
		StmtCacheEvictionPolicy(func() EvictionPolicy { return brokenPolicy{} }),
	))
	for _, q := range []string{"SELECT a", "SELECT b"} {
		p := mock.ExpectPrepare(q)
		p.ExpectQuery().WillReturnRows(cachedRows(1))
		vr := &Rows{}
		require.NoError(t, drv.Query(context.Background(), q, []any{}, vr))
		require.NoError(t, vr.Close())
	}
	require.NoError(t, mock.ExpectationsWereMet())
	stats, _ := drv.StmtCacheStats()
	require.Equal(t, int64(0), stats.Evictions)
	require.Equal(t, int64(2), stats.ActiveStmts) // grew beyond budget gracefully
}

func TestStmtCacheUnboundedBudget(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	require.NoError(t, err)
	drv := OpenDB(dialect.MySQL, db, WithStmtCache(StmtCacheMaxEntries(0)))
	for _, q := range []string{"SELECT a", "SELECT b", "SELECT c"} {
		p := mock.ExpectPrepare(q)
		p.ExpectQuery().WillReturnRows(cachedRows(1))
		vr := &Rows{}
		require.NoError(t, drv.Query(context.Background(), q, []any{}, vr))
		require.NoError(t, vr.Close())
	}
	require.NoError(t, mock.ExpectationsWereMet())
	stats, _ := drv.StmtCacheStats()
	require.Equal(t, int64(0), stats.Evictions)
	require.Equal(t, int64(3), stats.ActiveStmts)
}

func TestStmtCacheRebuildsStaleStatement(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	require.NoError(t, err)
	var rec eventRecorder
	drv := OpenDB(dialect.MySQL, db, WithStmtCache(StmtCacheObserver(rec.observer())))

	prep := mock.ExpectPrepare(cachedQuery)
	prep.ExpectQuery().WillReturnRows(cachedRows(1)) // prime the cache (miss)
	prep.ExpectQuery().WillReturnError(errStaleStmt)
	prep.WillBeClosed()
	prep2 := mock.ExpectPrepare(cachedQuery)
	prep2.ExpectQuery().WillReturnRows(cachedRows(2)) // rebuilt statement works

	requireCachedIDs(t, drv, 1)
	requireCachedIDs(t, drv, 2) // stale error must not escape
	require.NoError(t, mock.ExpectationsWereMet())

	stats, _ := drv.StmtCacheStats()
	require.Equal(t, int64(1), stats.Misses)
	require.Equal(t, int64(1), stats.Hits)
	require.Equal(t, int64(1), stats.Rebuilds)
	require.Equal(t, int64(2), stats.Prepares)
	require.Equal(t, int64(1), stats.Invalidations)
	require.Equal(t, int64(1), stats.ActiveStmts)
	var rebuilds int
	for _, k := range rec.kinds() {
		if k == StmtCacheRebuild {
			rebuilds++
		}
	}
	require.Equal(t, 1, rebuilds)
	for _, e := range rec.events {
		if e.Kind == StmtCacheRebuild {
			require.ErrorIs(t, e.Err, errStaleStmt)
		}
	}
}

func TestStmtCacheRebuildsStaleExec(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	require.NoError(t, err)
	drv := OpenDB(dialect.MySQL, db, WithStmtCache())
	const q = "UPDATE users SET name = ? WHERE id = ?"

	prep := mock.ExpectPrepare(q)
	prep.ExpectExec().WithArgs("a", 1).WillReturnResult(sqlmock.NewResult(0, 1))
	prep.ExpectExec().WillReturnError(errStaleStmt)
	prep.WillBeClosed()
	prep2 := mock.ExpectPrepare(q)
	prep2.ExpectExec().WillReturnResult(sqlmock.NewResult(0, 1))

	exec := func() {
		require.NoError(t, drv.Exec(context.Background(), q, []any{"a", 1}, nil))
	}
	exec() // prime
	exec() // stale -> rebuild -> success, no raw re-execution
	require.NoError(t, mock.ExpectationsWereMet())
	stats, _ := drv.StmtCacheStats()
	require.Equal(t, int64(1), stats.Rebuilds)
}

func TestStmtCachePrepareFailureFallsBackToRaw(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	require.NoError(t, err)
	drv := OpenDB(dialect.MySQL, db, WithStmtCache())

	mock.ExpectPrepare(cachedQuery).WillReturnError(errors.New("cannot prepare"))
	mock.ExpectQuery(cachedQuery).WillReturnRows(cachedRows(1))
	requireCachedIDs(t, drv, 1)
	require.NoError(t, mock.ExpectationsWereMet())

	stats, _ := drv.StmtCacheStats()
	require.Equal(t, int64(1), stats.Misses)
	require.Equal(t, int64(0), stats.Prepares)
	require.Equal(t, int64(0), stats.ActiveStmts)
}

func TestStmtCacheBusinessErrorIsNotRetried(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	require.NoError(t, err)
	drv := OpenDB(dialect.MySQL, db, WithStmtCache())
	const q = "INSERT INTO users (id) VALUES (?)"
	errConstraint := errors.New("error 1062 (23000): duplicate entry '1' for key 'PRIMARY'")

	// First (miss) execution fails with a business error: the prepared
	// statement is discarded but nothing is retried.
	prep := mock.ExpectPrepare(q)
	prep.ExpectExec().WillReturnError(errConstraint)
	prep.WillBeClosed()
	// The next execution simply prepares again and succeeds.
	prep2 := mock.ExpectPrepare(q)
	prep2.ExpectExec().WillReturnResult(sqlmock.NewResult(1, 1))

	err = drv.Exec(context.Background(), q, []any{1}, nil)
	require.ErrorIs(t, err, errConstraint, "business error must surface unchanged")
	require.NoError(t, drv.Exec(context.Background(), q, []any{1}, nil))
	require.NoError(t, mock.ExpectationsWereMet())

	stats, _ := drv.StmtCacheStats()
	require.Equal(t, int64(0), stats.Rebuilds)
	require.Equal(t, int64(2), stats.Misses)
}

func TestStmtCacheRebuildFailureFallsBackToRaw(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	require.NoError(t, err)
	drv := OpenDB(dialect.MySQL, db, WithStmtCache())

	prep := mock.ExpectPrepare(cachedQuery)
	prep.ExpectQuery().WillReturnRows(cachedRows(1)) // prime
	prep.ExpectQuery().WillReturnError(errStaleStmt)
	prep.WillBeClosed()
	// Rebuild prepares, but execution fails again with a business error;
	// the identical raw path then serves the request successfully.
	prep2 := mock.ExpectPrepare(cachedQuery)
	prep2.ExpectQuery().WillReturnError(errors.New("syntax error near x"))
	prep2.WillBeClosed()
	mock.ExpectQuery(cachedQuery).WillReturnRows(cachedRows(9))

	requireCachedIDs(t, drv, 1)
	requireCachedIDs(t, drv, 9)
	require.NoError(t, mock.ExpectationsWereMet())

	stats, _ := drv.StmtCacheStats()
	require.Equal(t, int64(1), stats.Rebuilds)
	// The rebuilt statement failed too, so nothing stays cached; the next
	// execution transparently prepares again.
	require.Equal(t, int64(0), stats.ActiveStmts)
}

func TestStmtCacheInvalidatedByDDL(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	require.NoError(t, err)
	var rec eventRecorder
	drv := OpenDB(dialect.MySQL, db, WithStmtCache(StmtCacheObserver(rec.observer())))
	const ddl = "ALTER TABLE users ADD COLUMN age int"

	prep := mock.ExpectPrepare(cachedQuery)
	prep.ExpectQuery().WillReturnRows(cachedRows(1))
	prep.WillBeClosed() // flushed right after the DDL succeeds
	mock.ExpectExec(ddl).WillReturnResult(sqlmock.NewResult(0, 0))
	prep2 := mock.ExpectPrepare(cachedQuery)
	prep2.ExpectQuery().WillReturnRows(cachedRows(2))

	requireCachedIDs(t, drv, 1)
	require.NoError(t, drv.Exec(context.Background(), ddl, []any{}, nil))
	requireCachedIDs(t, drv, 2)
	require.NoError(t, mock.ExpectationsWereMet())

	stats, _ := drv.StmtCacheStats()
	require.Equal(t, int64(2), stats.Misses)
	require.Equal(t, int64(1), stats.Invalidations)
	require.Contains(t, rec.reasons(), InvalidateSchemaChange)
}

func TestStmtCacheManualInvalidation(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	require.NoError(t, err)
	drv := OpenDB(dialect.MySQL, db, WithStmtCache())

	prep := mock.ExpectPrepare(cachedQuery)
	prep.ExpectQuery().WillReturnRows(cachedRows(1))
	prep.WillBeClosed()
	requireCachedIDs(t, drv, 1)
	drv.InvalidateStmtCache()

	prep2 := mock.ExpectPrepare(cachedQuery)
	prep2.ExpectQuery().WillReturnRows(cachedRows(2))
	requireCachedIDs(t, drv, 2)
	require.NoError(t, mock.ExpectationsWereMet())

	stats, _ := drv.StmtCacheStats()
	require.Equal(t, int64(2), stats.Misses)
	require.Equal(t, int64(1), stats.Invalidations)
}

func TestStmtCacheTxScopeReuseAndDiscard(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	require.NoError(t, err)
	var rec eventRecorder
	drv := OpenDB(dialect.MySQL, db, WithStmtCache(StmtCacheObserver(rec.observer())))

	mock.ExpectBegin()
	prep := mock.ExpectPrepare(cachedQuery)
	prep.ExpectQuery().WillReturnRows(cachedRows(1))
	prep.ExpectQuery().WillReturnRows(cachedRows(2))
	prep.WillBeClosed()
	mock.ExpectCommit()

	tx, err := drv.Tx(context.Background())
	require.NoError(t, err)
	queryTx := func(v int) {
		vr := &Rows{}
		require.NoError(t, tx.Query(context.Background(), cachedQuery, []any{v}, vr))
		require.True(t, vr.Next())
		require.NoError(t, vr.Scan(&v))
		require.NoError(t, vr.Close())
	}
	queryTx(1)
	queryTx(2)
	require.NoError(t, tx.Commit())
	require.NoError(t, mock.ExpectationsWereMet())

	stats, _ := drv.StmtCacheStats()
	require.Equal(t, int64(1), stats.Misses)
	require.Equal(t, int64(1), stats.Hits)
	require.Equal(t, int64(0), stats.ActiveStmts, "tx statements must be released")
	require.Contains(t, rec.scopes(), StmtCacheScopeTx)
	require.Contains(t, rec.reasons(), InvalidateTransactionEnd)
}

func TestStmtCacheDDLInTxFlushesParentOnCommit(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	require.NoError(t, err)
	drv := OpenDB(dialect.MySQL, db, WithStmtCache())
	const ddl = "ALTER TABLE users ADD COLUMN age int"

	// A statement cached at the driver scope.
	parent := mock.ExpectPrepare(cachedQuery)
	parent.ExpectQuery().WillReturnRows(cachedRows(1))
	requireCachedIDs(t, drv, 1)

	// DDL runs inside a transaction and commits.
	mock.ExpectBegin()
	mock.ExpectExec(ddl).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectCommit()
	tx, err := drv.Tx(context.Background())
	require.NoError(t, err)
	require.NoError(t, tx.Exec(context.Background(), ddl, []any{}, nil))
	require.NoError(t, tx.Commit())

	// The parent cache was flushed: the same query is prepared again.
	parent.WillBeClosed()
	parent2 := mock.ExpectPrepare(cachedQuery)
	parent2.ExpectQuery().WillReturnRows(cachedRows(2))
	requireCachedIDs(t, drv, 2)
	require.NoError(t, mock.ExpectationsWereMet())

	stats, _ := drv.StmtCacheStats()
	require.Equal(t, int64(2), stats.Misses)
	require.Equal(t, int64(1), stats.Invalidations)
}

func TestStmtCacheDDLRollbackKeepsParent(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	require.NoError(t, err)
	drv := OpenDB(dialect.MySQL, db, WithStmtCache())
	const ddl = "ALTER TABLE users ADD COLUMN age int"

	parent := mock.ExpectPrepare(cachedQuery)
	parent.ExpectQuery().WillReturnRows(cachedRows(1))
	requireCachedIDs(t, drv, 1)

	mock.ExpectBegin()
	mock.ExpectExec(ddl).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectRollback()
	tx, err := drv.Tx(context.Background())
	require.NoError(t, err)
	require.NoError(t, tx.Exec(context.Background(), ddl, []any{}, nil))
	require.NoError(t, tx.Rollback())

	// Rolled-back DDL never changed the schema: the driver-scope statement
	// is still a hit.
	parent.ExpectQuery().WillReturnRows(cachedRows(2))
	requireCachedIDs(t, drv, 2)
	require.NoError(t, mock.ExpectationsWereMet())

	stats, _ := drv.StmtCacheStats()
	require.Equal(t, int64(1), stats.Misses)
	require.Equal(t, int64(1), stats.Hits)
}

func TestStmtCachePinnedConnectionScope(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	var rec eventRecorder
	drv := OpenDB(dialect.Postgres, db, WithStmtCache(StmtCacheObserver(rec.observer())))

	// WithVar pins a *sql.Conn: prepare happens on it, and the statement
	// is closed before the connection is returned (after RESET).
	mock.ExpectExec("SET foo = 'bar'").WillReturnResult(sqlmock.NewResult(0, 0))
	prep := mock.ExpectPrepare(cachedQuery)
	prep.ExpectQuery().WillReturnRows(cachedRows(1))
	prep.WillBeClosed()
	mock.ExpectExec("RESET foo").WillReturnResult(sqlmock.NewResult(0, 0))

	rows := &Rows{}
	require.NoError(t, drv.Query(
		WithVar(context.Background(), "foo", "bar"),
		cachedQuery, []any{1}, rows,
	))
	require.NoError(t, rows.Close())
	require.NoError(t, mock.ExpectationsWereMet())
	require.Contains(t, rec.scopes(), StmtCacheScopeConn)
	require.Contains(t, rec.reasons(), InvalidateConnectionRelease)
	stats, _ := drv.StmtCacheStats()
	require.Equal(t, int64(0), stats.ActiveStmts)
}

func TestStmtCacheResultPassthrough(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	require.NoError(t, err)
	drv := OpenDB(dialect.MySQL, db, WithStmtCache())
	const q = "INSERT INTO users (id) VALUES (?)"
	prep := mock.ExpectPrepare(q)
	prep.ExpectExec().WithArgs(42).WillReturnResult(sqlmock.NewResult(42, 1))
	var res sql.Result
	require.NoError(t, drv.Exec(context.Background(), q, []any{42}, &res))
	id, err := res.LastInsertId()
	require.NoError(t, err)
	require.Equal(t, int64(42), id)
	affected, err := res.RowsAffected()
	require.NoError(t, err)
	require.Equal(t, int64(1), affected)
	require.NoError(t, mock.ExpectationsWereMet())
}

// unpreparerExecQuerier implements ExecQuerier but deliberately does not
// implement PrepareContext.
type unpreparerExecQuerier struct {
	queries int
	execs   int
}

func (u *unpreparerExecQuerier) ExecContext(context.Context, string, ...any) (sql.Result, error) {
	u.execs++
	return nil, nil
}

func (u *unpreparerExecQuerier) QueryContext(context.Context, string, ...any) (*sql.Rows, error) {
	u.queries++
	return nil, nil
}

func TestStmtCacheUnsupportedExecQuerierFallsBack(t *testing.T) {
	eq := &unpreparerExecQuerier{}
	drv := NewDriver(dialect.MySQL, Conn{ExecQuerier: eq}, WithStmtCache())
	stats, enabled := drv.StmtCacheStats()
	require.True(t, enabled)
	require.Zero(t, stats.ActiveStmts)

	require.NoError(t, drv.Query(context.Background(), cachedQuery, []any{1}, &Rows{}))
	require.NoError(t, drv.Exec(context.Background(), "INSERT INTO users DEFAULT VALUES", []any{}, nil))
	require.Equal(t, 1, eq.queries)
	require.Equal(t, 1, eq.execs)
}
