// Copyright 2019-present Facebook Inc. All rights reserved.
// This source code is licensed under the Apache 2.0 license found
// in the LICENSE file in the root directory of this source tree.

package sql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"entgo.io/ent/dialect"
)

// Driver is a dialect.Driver implementation for SQL based databases.
type Driver struct {
	Conn
	dialect string
	stmts   *stmtCache
	rt      *stmtCacheRuntime
}

// DriverOption configures a Driver.
type DriverOption func(*driverOptions)

// driverOptions holds the optional Driver configuration.
type driverOptions struct {
	cache *stmtCacheConfig
}

// WithStmtCache enables the declarative statement reuse chain: statements
// are merged by their shape and parameter count, prepared once per scope,
// reused on a hit, evicted by the declared policy when over budget, and
// invalidated on connection failures, transaction end and schema changes.
//
// When the option is not applied, the execution path is identical to the
// one without it; return types, error types and result ordering never
// change when it is enabled.
func WithStmtCache(opts ...StmtCacheOption) DriverOption {
	return func(o *driverOptions) {
		if o.cache == nil {
			o.cache = &stmtCacheConfig{
				maxEntries: defaultStmtCacheEntries,
				newPolicy:  NewLRUPolicy,
			}
		}
		for _, opt := range opts {
			if opt != nil {
				opt(o.cache)
			}
		}
	}
}

// NewDriver creates a new Driver with the given Conn and dialect.
func NewDriver(dialectName string, c Conn, opts ...DriverOption) *Driver {
	o := &driverOptions{}
	for _, opt := range opts {
		if opt != nil {
			opt(o)
		}
	}
	d := &Driver{dialect: dialectName, Conn: c}
	if o.cache != nil {
		d.rt = &stmtCacheRuntime{cfg: *o.cache, stats: &stmtStats{}}
		d.stmts = newStmtCache(StmtCacheScopeDB, c.ExecQuerier, d.rt, nil)
		d.Conn.stmts = d.stmts
	}
	return d
}

// Open wraps the database/sql.Open method and returns a dialect.Driver that implements the an ent/dialect.Driver interface.
func Open(dialectName, source string, opts ...DriverOption) (*Driver, error) {
	db, err := sql.Open(dialectName, source)
	if err != nil {
		return nil, err
	}
	return NewDriver(dialectName, Conn{ExecQuerier: db, dialect: dialectName}, opts...), nil
}

// OpenDB wraps the given database/sql.DB method with a Driver.
func OpenDB(dialectName string, db *sql.DB, opts ...DriverOption) *Driver {
	return NewDriver(dialectName, Conn{ExecQuerier: db, dialect: dialectName}, opts...)
}

// StmtCacheStats returns the cumulative reuse counters and reports whether
// the statement reuse chain is enabled.
func (d Driver) StmtCacheStats() (StmtCacheStats, bool) {
	if d.rt == nil {
		return StmtCacheStats{}, false
	}
	return d.rt.stats.snapshot(), true
}

// InvalidateStmtCache discards every cached statement in the driver scope.
// Transaction and pinned-connection scopes are invalidated automatically at
// their lifecycle boundaries. It is a no-op when reuse is disabled.
func (d Driver) InvalidateStmtCache() {
	if d.stmts != nil {
		d.stmts.invalidateAll(InvalidateManual)
	}
}

// DB returns the underlying *sql.DB instance.
func (d Driver) DB() *sql.DB {
	return d.ExecQuerier.(*sql.DB)
}

// Dialect implements the dialect.Dialect method.
func (d Driver) Dialect() string {
	// If the underlying driver is wrapped with a telemetry driver.
	for _, name := range []string{dialect.MySQL, dialect.SQLite, dialect.Postgres} {
		if strings.HasPrefix(d.dialect, name) {
			return name
		}
	}
	return d.dialect
}

// Tx starts and returns a transaction.
func (d *Driver) Tx(ctx context.Context) (dialect.Tx, error) {
	return d.BeginTx(ctx, nil)
}

// BeginTx starts a transaction with options.
func (d *Driver) BeginTx(ctx context.Context, opts *TxOptions) (dialect.Tx, error) {
	tx, err := d.DB().BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	t := &Tx{
		Conn:   Conn{ExecQuerier: tx, dialect: d.dialect},
		Tx:     tx,
		parent: d.stmts,
	}
	if d.rt != nil {
		t.Conn.stmts = newStmtCache(StmtCacheScopeTx, tx, d.rt, d.stmts)
	}
	return t, nil
}

// Close closes the underlying connection.
func (d *Driver) Close() error {
	if d.stmts != nil {
		d.stmts.close(InvalidateClose)
	}
	return d.DB().Close()
}

// Tx implements dialect.Tx interface.
type Tx struct {
	Conn
	driver.Tx
	// parent references the driver-scope cache so that DDL successfully
	// committed inside the transaction invalidates it after commit.
	parent *stmtCache
}

// Commit commits the transaction and discards all statements prepared in
// its scope. DDL committed in the transaction invalidates the driver cache.
func (t *Tx) Commit() error {
	err := t.Tx.Commit()
	if t.stmts != nil {
		schemaChanged := t.stmts.consumeSchemaChange()
		t.stmts.close(InvalidateTransactionEnd)
		if err == nil && schemaChanged && t.parent != nil {
			t.parent.invalidateAll(InvalidateSchemaChange)
		}
	}
	return err
}

// Rollback aborts the transaction and discards all statements prepared in
// its scope. The driver-scope cache is kept intact because rolled-back DDL
// never changed the schema.
func (t *Tx) Rollback() error {
	err := t.Tx.Rollback()
	if t.stmts != nil {
		t.stmts.close(InvalidateTransactionEnd)
	}
	return err
}

// ctyVarsKey is the key used for attaching and reading the context variables.
type ctxVarsKey struct{}

// sessionVars holds sessions/transactions variables to set before every statement.
type sessionVars struct {
	vars []struct{ k, v string }
}

// WithVar returns a new context that holds the session variable to be executed before every query.
func WithVar(ctx context.Context, name, value string) context.Context {
	sv, _ := ctx.Value(ctxVarsKey{}).(sessionVars)
	sv.vars = append(sv.vars, struct {
		k, v string
	}{
		k: name,
		v: value,
	})
	return context.WithValue(ctx, ctxVarsKey{}, sv)
}

// VarFromContext returns the session variable value from the context.
func VarFromContext(ctx context.Context, name string) (string, bool) {
	sv, _ := ctx.Value(ctxVarsKey{}).(sessionVars)
	for _, s := range sv.vars {
		if s.k == name {
			return s.v, true
		}
	}
	return "", false
}

// WithIntVar calls WithVar with the string representation of the value.
func WithIntVar(ctx context.Context, name string, value int) context.Context {
	return WithVar(ctx, name, strconv.Itoa(value))
}

// ExecQuerier wraps the standard Exec and Query methods.
type ExecQuerier interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

// Conn implements dialect.ExecQuerier given ExecQuerier.
type Conn struct {
	ExecQuerier
	dialect string
	// stmts is the scope statement cache. A nil value means statement
	// reuse is disabled and Exec/Query take the raw path unchanged.
	stmts *stmtCache
}

// Exec implements the dialect.Exec method.
func (c Conn) Exec(ctx context.Context, query string, args, v any) (rerr error) {
	argv, ok := args.([]any)
	if !ok {
		return fmt.Errorf("dialect/sql: invalid type %T. expect []any for args", v)
	}
	ex, cf, cache, err := c.maySetVars(ctx)
	if err != nil {
		return err
	}
	if cf != nil {
		defer func() { rerr = errors.Join(rerr, cf()) }()
	}
	// DDL is never reused (not every dialect can prepare every DDL form)
	// and invalidates the scopes once it succeeds.
	viaCache := cache != nil && !isDDL(query)
	execute := func() (sql.Result, error) {
		if viaCache {
			return cache.Exec(ctx, query, argv)
		}
		return ex.ExecContext(ctx, query, argv...)
	}
	switch v := v.(type) {
	case nil:
		if _, err := execute(); err != nil {
			return err
		}
	case *sql.Result:
		res, err := execute()
		if err != nil {
			return err
		}
		*v = res
	default:
		return fmt.Errorf("dialect/sql: invalid type %T. expect *sql.Result", v)
	}
	if !viaCache && cache != nil && isDDL(query) {
		c.invalidateAfterDDL(cache)
	}
	return nil
}

// Query implements the dialect.Query method.
func (c Conn) Query(ctx context.Context, query string, args, v any) error {
	vr, ok := v.(*Rows)
	if !ok {
		return fmt.Errorf("dialect/sql: invalid type %T. expect *sql.Rows", v)
	}
	argv, ok := args.([]any)
	if !ok {
		return fmt.Errorf("dialect/sql: invalid type %T. expect []any for args", args)
	}
	ex, cf, cache, err := c.maySetVars(ctx)
	if err != nil {
		return err
	}
	var rows *sql.Rows
	if cache != nil {
		rows, err = cache.Query(ctx, query, argv)
	} else {
		rows, err = ex.QueryContext(ctx, query, argv...)
	}
	if err != nil {
		if cf != nil {
			err = errors.Join(err, cf())
		}
		return err
	}
	*vr = Rows{rows}
	if cf != nil {
		vr.ColumnScanner = rowsWithCloser{rows, cf}
	}
	return nil
}

// invalidateAfterDDL flushes statements that may reference the old schema
// after a successful DDL execution. Transaction scopes are flushed now and
// remembered for a parent flush at commit; pinned-connection scopes also
// flush the driver-scope cache immediately.
func (c Conn) invalidateAfterDDL(cache *stmtCache) {
	if cache == nil {
		return
	}
	cache.invalidateAll(InvalidateSchemaChange)
	if cache.scope == StmtCacheScopeTx {
		cache.markSchemaChange()
		return
	}
	if c.stmts != nil && c.stmts != cache {
		c.stmts.invalidateAll(InvalidateSchemaChange)
	}
}

// maySetVars sets the session variables before executing a query. It also
// returns the statement cache effective for the execution: the Conn-scope
// cache as-is, or a transient connection-scope cache when a *sql.Conn is
// pinned (its entries are discarded when the connection is released).
func (c Conn) maySetVars(ctx context.Context) (ExecQuerier, func() error, *stmtCache, error) {
	sv, _ := ctx.Value(ctxVarsKey{}).(sessionVars)
	if len(sv.vars) == 0 {
		return c.ExecQuerier, nil, c.stmts, nil
	}
	var (
		ex    ExecQuerier  // Underlying ExecQuerier.
		cf    func() error // Close function.
		cache = c.stmts    // Effective statement cache.
		reset []string     // Reset variables.
		seen  = make(map[string]struct{}, len(sv.vars))
	)
	switch e := c.ExecQuerier.(type) {
	case *sql.Tx:
		ex = e
	case *sql.DB:
		conn, err := e.Conn(ctx)
		if err != nil {
			return nil, nil, nil, err
		}
		ex, cf = conn, conn.Close
		if c.stmts != nil {
			// Statements prepared on the pinned connection must be closed
			// before it returns to the pool.
			connCache := newStmtCache(StmtCacheScopeConn, conn, c.stmts.rt, c.stmts)
			cache = connCache
			release := cf
			cf = func() error {
				connCache.close(InvalidateConnectionRelease)
				return release()
			}
		}
	}
	for _, s := range sv.vars {
		if _, ok := seen[s.k]; !ok {
			switch c.dialect {
			case dialect.Postgres:
				reset = append(reset, fmt.Sprintf("RESET %s", s.k))
			case dialect.MySQL:
				reset = append(reset, fmt.Sprintf("SET %s = NULL", s.k))
			}
			seen[s.k] = struct{}{}
		}
		if _, err := ex.ExecContext(ctx, fmt.Sprintf("SET %s = '%s'", s.k, s.v)); err != nil {
			if cf != nil {
				err = errors.Join(err, cf())
			}
			return nil, nil, nil, err
		}
	}
	// If there are variables to reset, and we need to return the
	// connection to the pool, we need to clean up the variables.
	if cls := cf; cf != nil && len(reset) > 0 {
		cf = func() error {
			for _, q := range reset {
				if _, err := ex.ExecContext(ctx, q); err != nil {
					return errors.Join(err, cls())
				}
			}
			return cls()
		}
	}
	return ex, cf, cache, nil
}

var _ dialect.Driver = (*Driver)(nil)

type (
	// Rows wraps the sql.Rows to avoid locks copy.
	Rows struct{ ColumnScanner }
	// Result is an alias to sql.Result.
	Result = sql.Result
	// NullBool is an alias to sql.NullBool.
	NullBool = sql.NullBool
	// NullInt64 is an alias to sql.NullInt64.
	NullInt64 = sql.NullInt64
	// NullString is an alias to sql.NullString.
	NullString = sql.NullString
	// NullFloat64 is an alias to sql.NullFloat64.
	NullFloat64 = sql.NullFloat64
	// NullTime represents a time.Time that may be null.
	NullTime = sql.NullTime
	// TxOptions holds the transaction options to be used in DB.BeginTx.
	TxOptions = sql.TxOptions
)

// NullScanner implements the sql.Scanner interface such that it
// can be used as a scan destination, similar to the types above.
type NullScanner struct {
	S     sql.Scanner
	Valid bool // Valid is true if the Scan value is not NULL.
}

// Scan implements the Scanner interface.
func (n *NullScanner) Scan(value any) error {
	n.Valid = value != nil
	if n.Valid {
		return n.S.Scan(value)
	}
	return nil
}

// ColumnScanner is the interface that wraps the standard
// sql.Rows methods used for scanning database rows.
type ColumnScanner interface {
	Close() error
	ColumnTypes() ([]*sql.ColumnType, error)
	Columns() ([]string, error)
	Err() error
	Next() bool
	NextResultSet() bool
	Scan(dest ...any) error
}

// rowsWithCloser wraps the ColumnScanner interface with a custom Close hook.
type rowsWithCloser struct {
	ColumnScanner
	closer func() error
}

// Close closes the underlying ColumnScanner and calls the custom closer.
func (r rowsWithCloser) Close() error {
	err := r.ColumnScanner.Close()
	return errors.Join(err, r.closer())
}
