// Copyright 2019-present Facebook Inc. All rights reserved.
// This source code is licensed under the Apache 2.0 license found
// in the LICENSE file in the root directory of this source tree.

package sql

import (
	"container/list"
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
)

// defaultStmtCacheEntries is the default reuse budget when the chain is
// enabled without an explicit budget.
const defaultStmtCacheEntries = 100

// StmtCacheScope identifies the lifecycle scope a cached statement belongs to.
type StmtCacheScope string

// Statement cache scopes.
const (
	// StmtCacheScopeDB caches statements prepared on a *sql.DB. Entries stay
	// valid across physical connections (database/sql re-prepares lazily)
	// and until explicitly invalidated.
	StmtCacheScopeDB StmtCacheScope = "db"
	// StmtCacheScopeTx caches statements prepared inside a single
	// transaction. Every entry is discarded when the transaction ends.
	StmtCacheScopeTx StmtCacheScope = "tx"
	// StmtCacheScopeConn caches statements prepared on a pinned *sql.Conn
	// (for example when session variables are present). Every entry is
	// discarded when the connection is returned to the pool.
	StmtCacheScopeConn StmtCacheScope = "conn"
)

// StmtCacheEventKind is the kind of an observed statement cache event.
type StmtCacheEventKind uint8

// Observed statement cache event kinds.
const (
	// StmtCacheHit is emitted when an execution reused a cached statement.
	StmtCacheHit StmtCacheEventKind = iota + 1
	// StmtCacheMiss is emitted when an execution had to prepare a statement.
	StmtCacheMiss
	// StmtCacheRebuild is emitted when a cached statement reported an
	// invalid-statement error, was discarded and rebuilt transparently.
	StmtCacheRebuild
	// StmtCacheInvalidate is emitted when entries are discarded due to a
	// transaction end, connection release, schema change or manual flush.
	StmtCacheInvalidate
	// StmtCacheEvict is emitted when an entry is removed to honor the budget.
	StmtCacheEvict
)

// InvalidationReason explains why cached statements were discarded.
type InvalidationReason string

// Invalidation reasons reported with StmtCacheInvalidate / StmtCacheEvict.
const (
	// InvalidateStale marks an entry whose statement reported an
	// invalid-statement error (bad connection or server-side eviction).
	InvalidateStale InvalidationReason = "stale"
	// InvalidateTransactionEnd marks a transaction-scope cache being
	// discarded because the transaction committed or rolled back.
	InvalidateTransactionEnd InvalidationReason = "transaction-end"
	// InvalidateConnectionRelease marks a connection-scope cache being
	// discarded because the pinned connection was returned to the pool.
	InvalidateConnectionRelease InvalidationReason = "connection-release"
	// InvalidateSchemaChange marks entries discarded after a DDL statement
	// was executed successfully.
	InvalidateSchemaChange InvalidationReason = "schema-change"
	// InvalidateManual marks entries discarded through the explicit API.
	InvalidateManual InvalidationReason = "manual"
	// InvalidateClose marks entries discarded while the driver is closing.
	InvalidateClose InvalidationReason = "close"
	// InvalidateEviction marks an entry removed to honor the budget.
	InvalidateEviction InvalidationReason = "eviction"
)

// StmtCacheKey identifies a reusable statement: its normalized shape (the
// parameterized SQL text) together with the number of bound parameters.
type StmtCacheKey struct {
	// Query is the statement text in its parameterized placeholder form.
	Query string
	// NumArgs is the number of bound parameters.
	NumArgs int
}

// StmtCacheStats holds cumulative reuse counters. Counters are shared across
// the driver, its transactions and its pinned-connection caches.
type StmtCacheStats struct {
	// Hits counts executions that reused a cached statement.
	Hits int64
	// Misses counts executions that prepared a new statement.
	Misses int64
	// Prepares counts successful PrepareContext calls performed by the chain.
	Prepares int64
	// Rebuilds counts transparent rebuilds after an invalid cached statement.
	Rebuilds int64
	// Evictions counts entries removed to honor the budget.
	Evictions int64
	// Invalidations counts entries discarded for any non-eviction reason.
	Invalidations int64
	// ActiveStmts is the current number of cached statements across scopes.
	ActiveStmts int64
}

// StmtCacheEvent is an observed statement cache event. Observer callbacks
// must return quickly and must not call back into the driver.
type StmtCacheEvent struct {
	// Kind is the event kind.
	Kind StmtCacheEventKind
	// Scope is the cache scope that produced the event.
	Scope StmtCacheScope
	// Key is the statement key. For bulk invalidations it is reported per
	// discarded entry; it is the zero value otherwise.
	Key StmtCacheKey
	// Reason is the discard reason for StmtCacheInvalidate / StmtCacheEvict.
	Reason InvalidationReason
	// Err is the invalid-statement error carried by a StmtCacheRebuild event.
	Err error
}

// EvictionPolicy selects a victim when the cache grows beyond its budget.
//
// A policy instance is owned by a single cache scope (driver, transaction or
// pinned connection); all its methods are invoked while the owning cache is
// serialized, so implementations do not need their own synchronization.
type EvictionPolicy interface {
	// Added reports a newly cached key.
	Added(StmtCacheKey)
	// Used reports a cache hit on the key.
	Used(StmtCacheKey)
	// Removed reports that a key is no longer cached.
	Removed(StmtCacheKey)
	// Victim returns the key to discard next. Returning false means the
	// cache is allowed to temporarily grow beyond the budget.
	Victim() (StmtCacheKey, bool)
}

// NewLRUPolicy creates a least-recently-used eviction policy.
func NewLRUPolicy() EvictionPolicy {
	return &lruPolicy{
		ll:      list.New(),
		entries: make(map[StmtCacheKey]*list.Element),
	}
}

// lruPolicy implements EvictionPolicy using a doubly linked list where the
// front is the least-recently-used key and the back is the most-recently-used.
type lruPolicy struct {
	ll      *list.List
	entries map[StmtCacheKey]*list.Element
}

// Added implements EvictionPolicy.
func (p *lruPolicy) Added(key StmtCacheKey) {
	if _, ok := p.entries[key]; ok {
		return
	}
	p.entries[key] = p.ll.PushBack(key)
}

// Used implements EvictionPolicy.
func (p *lruPolicy) Used(key StmtCacheKey) {
	if e, ok := p.entries[key]; ok {
		p.ll.MoveToBack(e)
	}
}

// Removed implements EvictionPolicy.
func (p *lruPolicy) Removed(key StmtCacheKey) {
	if e, ok := p.entries[key]; ok {
		p.ll.Remove(e)
		delete(p.entries, key)
	}
}

// Victim implements EvictionPolicy.
func (p *lruPolicy) Victim() (StmtCacheKey, bool) {
	if p.ll.Len() == 0 {
		return StmtCacheKey{}, false
	}
	return p.ll.Front().Value.(StmtCacheKey), true
}

// StmtCacheOption configures the statement reuse chain.
type StmtCacheOption func(*stmtCacheConfig)

// StmtCacheMaxEntries declares the reuse budget. A value <= 0 means the
// cache is unbounded. Defaults to 100 when the chain is enabled.
func StmtCacheMaxEntries(n int) StmtCacheOption {
	return func(c *stmtCacheConfig) {
		c.maxEntries = n
	}
}

// StmtCacheEvictionPolicy declares the policy used to select victims once the
// budget is exceeded. A fresh policy is created per cache scope (driver,
// transaction, pinned connection).
func StmtCacheEvictionPolicy(newPolicy func() EvictionPolicy) StmtCacheOption {
	return func(c *stmtCacheConfig) {
		if newPolicy != nil {
			c.newPolicy = newPolicy
		}
	}
}

// StmtCacheObserver registers a function invoked for every hit, miss,
// rebuild, invalidation and eviction. The function must return quickly and
// must not call back into the driver.
func StmtCacheObserver(fn func(StmtCacheEvent)) StmtCacheOption {
	return func(c *stmtCacheConfig) {
		if fn != nil {
			c.observer = fn
		}
	}
}

// stmtCacheConfig is the resolved configuration shared across scopes.
type stmtCacheConfig struct {
	maxEntries int
	newPolicy  func() EvictionPolicy
	observer   func(StmtCacheEvent)
}

// stmtStats holds the cumulative counters shared across all scopes.
type stmtStats struct {
	hits          atomic.Int64
	misses        atomic.Int64
	prepares      atomic.Int64
	rebuilds      atomic.Int64
	evictions     atomic.Int64
	invalidations atomic.Int64
	active        atomic.Int64
}

// snapshot returns the current cumulative counters.
func (s *stmtStats) snapshot() StmtCacheStats {
	return StmtCacheStats{
		Hits:          s.hits.Load(),
		Misses:        s.misses.Load(),
		Prepares:      s.prepares.Load(),
		Rebuilds:      s.rebuilds.Load(),
		Evictions:     s.evictions.Load(),
		Invalidations: s.invalidations.Load(),
		ActiveStmts:   s.active.Load(),
	}
}

// stmtCacheRuntime is shared by all cache scopes of one driver.
type stmtCacheRuntime struct {
	cfg   stmtCacheConfig
	stats *stmtStats
}

// preparer is implemented by *sql.DB, *sql.Tx and *sql.Conn.
type preparer interface {
	PrepareContext(ctx context.Context, query string) (*sql.Stmt, error)
}

// cacheEntry is a single cached statement.
type cacheEntry struct {
	key  StmtCacheKey
	stmt *sql.Stmt
}

// stmtCache caches prepared statements for one ExecQuerier.
type stmtCache struct {
	eq      ExecQuerier
	prep    preparer
	scope   StmtCacheScope
	max     int
	policy  EvictionPolicy
	rt      *stmtCacheRuntime
	parent  *stmtCache
	mu      sync.Mutex
	items   map[StmtCacheKey]*cacheEntry
	closed  bool
	changed atomic.Bool
}

// newStmtCache creates a cache for the given ExecQuerier. It returns nil
// when the ExecQuerier cannot prepare statements, in which case executions
// silently fall back to the raw (reuse-disabled) path.
func newStmtCache(scope StmtCacheScope, eq ExecQuerier, rt *stmtCacheRuntime, parent *stmtCache) *stmtCache {
	prep, ok := eq.(preparer)
	if !ok || rt == nil {
		return nil
	}
	policy := rt.cfg.newPolicy
	if policy == nil {
		policy = NewLRUPolicy
	}
	return &stmtCache{
		eq:     eq,
		prep:   prep,
		scope:  scope,
		max:    rt.cfg.maxEntries,
		policy: policy(),
		rt:     rt,
		parent: parent,
		items:  make(map[StmtCacheKey]*cacheEntry),
	}
}

// stats returns the shared cumulative counters.
func (c *stmtCache) stats() *stmtStats { return c.rt.stats }

// emit dispatches an observer event outside of the cache lock.
func (c *stmtCache) emit(kind StmtCacheEventKind, key StmtCacheKey, reason InvalidationReason, err error) {
	if c.rt.cfg.observer == nil {
		return
	}
	c.rt.cfg.observer(StmtCacheEvent{
		Kind:   kind,
		Scope:  c.scope,
		Key:    key,
		Reason: reason,
		Err:    err,
	})
}

// get returns the cached statement for key, if any.
func (c *stmtCache) get(key StmtCacheKey) (*sql.Stmt, bool) {
	c.mu.Lock()
	entry, ok := c.items[key]
	if ok {
		c.policy.Used(key)
	}
	c.mu.Unlock()
	if ok {
		c.stats().hits.Add(1)
		c.emit(StmtCacheHit, key, "", nil)
		return entry.stmt, true
	}
	c.stats().misses.Add(1)
	c.emit(StmtCacheMiss, key, "", nil)
	return nil, false
}

// add stores a freshly prepared statement. It returns the canonical
// statement to use and whether the given statement is the one stored:
//   - (stmt, true) when it was inserted;
//   - (existing, false) when an identical entry raced in (the caller must
//     close its own statement);
//   - (nil, false) when the cache is already closed (the caller must close
//     its own statement and take the raw path).
func (c *stmtCache) add(key StmtCacheKey, stmt *sql.Stmt) (*sql.Stmt, bool) {
	var (
		victims  []*sql.Stmt
		victimKs []StmtCacheKey
	)
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, false
	}
	if entry, ok := c.items[key]; ok {
		c.mu.Unlock()
		return entry.stmt, false
	}
	for c.max > 0 && len(c.items) >= c.max {
		vk, ok := c.policy.Victim()
		if !ok {
			// The declared policy refuses to pick a victim; let the cache
			// temporarily grow beyond its budget instead of failing calls.
			break
		}
		old := c.deleteLocked(vk)
		if old == nil {
			// Defensive: a misbehaving policy returned an untracked key.
			// Stop instead of spinning, and let the cache grow temporarily.
			break
		}
		victims = append(victims, old)
		victimKs = append(victimKs, vk)
	}
	c.items[key] = &cacheEntry{key: key, stmt: stmt}
	c.policy.Added(key)
	c.stats().active.Add(1)
	c.mu.Unlock()
	for i, st := range victims {
		_ = st.Close()
		c.stats().evictions.Add(1)
		c.stats().active.Add(-1)
		c.emit(StmtCacheEvict, victimKs[i], InvalidateEviction, nil)
	}
	return stmt, true
}

// deleteLocked removes an entry and returns its statement for closing
// outside the lock. Callers must hold c.mu.
func (c *stmtCache) deleteLocked(key StmtCacheKey) *sql.Stmt {
	entry, ok := c.items[key]
	if !ok {
		return nil
	}
	delete(c.items, key)
	c.policy.Removed(key)
	return entry.stmt
}

// invalidate discards a single entry.
func (c *stmtCache) invalidate(key StmtCacheKey, reason InvalidationReason) {
	c.mu.Lock()
	st := c.deleteLocked(key)
	c.mu.Unlock()
	if st == nil {
		return
	}
	_ = st.Close()
	c.stats().invalidations.Add(1)
	c.stats().active.Add(-1)
	c.emit(StmtCacheInvalidate, key, reason, nil)
}

// invalidateAll discards every entry and reports the given reason.
func (c *stmtCache) invalidateAll(reason InvalidationReason) {
	c.mu.Lock()
	keys := make([]StmtCacheKey, 0, len(c.items))
	stmts := make([]*sql.Stmt, 0, len(c.items))
	for key, entry := range c.items {
		keys = append(keys, key)
		stmts = append(stmts, entry.stmt)
		delete(c.items, key)
		c.policy.Removed(key)
	}
	c.mu.Unlock()
	for i, st := range stmts {
		_ = st.Close()
		c.emit(StmtCacheInvalidate, keys[i], reason, nil)
	}
	if n := int64(len(stmts)); n > 0 {
		c.stats().invalidations.Add(n)
		c.stats().active.Add(-n)
	}
}

// close marks the cache closed and discards every entry. It is invoked when
// the owning transaction ends, a pinned connection is released or the
// driver closes.
func (c *stmtCache) close(reason InvalidationReason) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	c.closed = true
	keys := make([]StmtCacheKey, 0, len(c.items))
	stmts := make([]*sql.Stmt, 0, len(c.items))
	for key, entry := range c.items {
		keys = append(keys, key)
		stmts = append(stmts, entry.stmt)
		delete(c.items, key)
	}
	c.mu.Unlock()
	for i, st := range stmts {
		_ = st.Close()
		c.emit(StmtCacheInvalidate, keys[i], reason, nil)
	}
	if n := int64(len(stmts)); n > 0 {
		c.stats().invalidations.Add(n)
		c.stats().active.Add(-n)
	}
}

// markSchemaChange remembers that a DDL statement ran in this scope.
func (c *stmtCache) markSchemaChange() { c.changed.Store(true) }

// consumeSchemaChange reports and clears the schema-change marker.
func (c *stmtCache) consumeSchemaChange() bool { return c.changed.Swap(false) }

// Exec runs an ExecContext through the reuse chain.
func (c *stmtCache) Exec(ctx context.Context, query string, args []any) (sql.Result, error) {
	return cacheRun(
		c, ctx, query, args,
		func() (sql.Result, error) { return c.eq.ExecContext(ctx, query, args...) },
		func(st *sql.Stmt) (sql.Result, error) { return st.ExecContext(ctx, args...) },
	)
}

// Query runs a QueryContext through the reuse chain.
func (c *stmtCache) Query(ctx context.Context, query string, args []any) (*sql.Rows, error) {
	return cacheRun(
		c, ctx, query, args,
		func() (*sql.Rows, error) { return c.eq.QueryContext(ctx, query, args...) },
		func(st *sql.Stmt) (*sql.Rows, error) { return st.QueryContext(ctx, args...) },
	)
}

// cacheRun implements the shared lookup / prepare / rebuild / raw-fallback
// flow for both Exec and Query.
func cacheRun[T any](
	c *stmtCache,
	ctx context.Context,
	query string,
	args []any,
	raw func() (T, error),
	execute func(*sql.Stmt) (T, error),
) (T, error) {
	key := StmtCacheKey{Query: query, NumArgs: len(args)}
	if stmt, ok := c.get(key); ok {
		v, err := execute(stmt)
		if err == nil || !isInvalidCachedStmtErr(err) {
			return v, err
		}
		// The cached statement is no longer usable server-side. Drop it
		// and rebuild below instead of surfacing the failure.
		c.invalidate(key, InvalidateStale)
		return cacheRebuild(c, ctx, key, query, raw, execute, err)
	}
	stmt, err := c.prep.PrepareContext(ctx, query)
	if err != nil {
		// Preparing is not possible right now (for example the server is
		// unreachable). Use the exact raw path so error types match the
		// reuse-disabled behavior.
		return raw()
	}
	c.stats().prepares.Add(1)
	v, err := execute(stmt)
	if err == nil {
		if _, stored := c.add(key, stmt); !stored {
			// A duplicate raced in, or the scope closed. The returned value
			// stays usable; database/sql defers the driver-side close until
			// active rows are released.
			_ = stmt.Close()
		}
		return v, nil
	}
	_ = stmt.Close()
	if !isInvalidCachedStmtErr(err) {
		return v, err
	}
	c.invalidate(key, InvalidateStale)
	return cacheRebuild(c, ctx, key, query, raw, execute, err)
}

// cacheRebuild prepares a fresh statement once and retries the execution. If
// the rebuild cannot succeed, it takes the exact raw execution path so that
// no invalid-statement failure ever escapes to the caller.
func cacheRebuild[T any](
	c *stmtCache,
	ctx context.Context,
	key StmtCacheKey,
	query string,
	raw func() (T, error),
	execute func(*sql.Stmt) (T, error),
	staleErr error,
) (T, error) {
	c.stats().rebuilds.Add(1)
	c.emit(StmtCacheRebuild, key, InvalidateStale, staleErr)
	stmt, err := c.prep.PrepareContext(ctx, query)
	if err == nil {
		c.stats().prepares.Add(1)
		v, verr := execute(stmt)
		if verr == nil {
			if _, stored := c.add(key, stmt); !stored {
				_ = stmt.Close()
			}
			return v, nil
		}
		_ = stmt.Close()
	}
	return raw()
}

// isInvalidCachedStmtErr reports whether err indicates that a cached
// statement can no longer be executed as-is and must be rebuilt.
//
// The classification is intentionally conservative: only client-side
// connection failures and server-side "unknown/closed prepared statement"
// signals match, i.e. failures of the bind/handle stage that happen before
// the statement executes and therefore cannot cause double writes.
func isInvalidCachedStmtErr(err error) bool {
	if errors.Is(err, driver.ErrBadConn) || errors.Is(err, sql.ErrConnDone) {
		return true
	}
	msg := strings.ToLower(err.Error())
	if strings.Contains(msg, "statement is closed") {
		return true
	}
	if strings.Contains(msg, "prepared statement") && (strings.Contains(msg, "does not exist") ||
		strings.Contains(msg, "unknown") ||
		strings.Contains(msg, "no longer") ||
		strings.Contains(msg, "gone") ||
		strings.Contains(msg, "closed")) {
		return true
	}
	return false
}

// isDDL reports whether query is a data-definition statement whose
// successful execution may render existing prepared statements invalid.
// Detection is intentionally conservative: only leading CREATE / ALTER /
// DROP / RENAME keywords match; callers can always flush explicitly through
// Driver.InvalidateStmtCache.
func isDDL(query string) bool {
	q := strings.ToUpper(strings.TrimLeft(query, " \t\r\n"))
	for _, prefix := range []string{"CREATE ", "ALTER ", "DROP ", "RENAME "} {
		if strings.HasPrefix(q, prefix) {
			return true
		}
	}
	return false
}
