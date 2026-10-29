// Copyright 2019-present Facebook Inc. All rights reserved.
// This source code is licensed under the Apache 2.0 license found
// in the LICENSE file in the root directory of this source tree.

package sql

import (
	"context"
	stdsql "database/sql"
	"fmt"
	"testing"

	"entgo.io/ent/dialect"

	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/require"
)

// TestStmtCacheSQLiteEndToEnd exercises the reuse chain against a real
// database driver: normal queries, repeated in-tx executions, DDL
// invalidation and result order/type equivalence.
func TestStmtCacheSQLiteEndToEnd(t *testing.T) {
	db, err := stdsql.Open(dialect.SQLite, fmt.Sprintf("file:%s?mode=memory&_fk=1", t.Name()))
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	drv := OpenDB(dialect.SQLite, db, WithStmtCache())
	ctx := context.Background()

	require.NoError(t, drv.Exec(ctx, "CREATE TABLE users (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL)", []any{}, nil))

	const insertQ = "INSERT INTO users (name) VALUES (?)"
	for _, name := range []string{"a8m", "alexsn", "a8m"} {
		var res Result
		require.NoError(t, drv.Exec(ctx, insertQ, []any{name}, &res))
		n, err := res.RowsAffected()
		require.NoError(t, err)
		require.Equal(t, int64(1), n)
	}
	stats, _ := drv.StmtCacheStats()
	require.Equal(t, int64(1), stats.Prepares, "same-shape inserts prepare once")
	require.Equal(t, int64(2), stats.Hits)

	// Batch-shaped preloads: same shapes repeat; IN-lists of different size
	// are different shapes and prepare independently.
	preload := func(ids ...int) []string {
		args := make([]any, len(ids))
		placeholders := make([]byte, 0, len(ids)*2)
		for i := range ids {
			args[i] = ids[i]
			if i > 0 {
				placeholders = append(placeholders, ',')
			}
			placeholders = append(placeholders, '?')
		}
		q := "SELECT name FROM users WHERE id IN (" + string(placeholders) + ") ORDER BY id"
		vr := &Rows{}
		require.NoError(t, drv.Query(ctx, q, args, vr))
		var names []string
		for vr.Next() {
			var name string
			require.NoError(t, vr.Scan(&name))
			names = append(names, name)
		}
		require.NoError(t, vr.Err())
		require.NoError(t, vr.Close())
		return names
	}
	hitsBefore, _ := drv.StmtCacheStats()
	require.Equal(t, []string{"a8m", "alexsn"}, preload(1, 2))
	require.Equal(t, []string{"alexsn", "a8m"}, preload(2, 3))
	require.Equal(t, []string{"a8m", "alexsn", "a8m"}, preload(1, 2, 3))
	require.Equal(t, []string{"a8m", "alexsn", "a8m"}, preload(3, 1, 2))
	stats, _ = drv.StmtCacheStats()
	require.Equal(t, int64(2), stats.Hits-hitsBefore.Hits) // 2-arg and 3-arg shapes reused once
	activeBeforeTx := stats.ActiveStmts

	// Transaction scope: reuse within the tx, discard on commit.
	tx, err := drv.Tx(ctx)
	require.NoError(t, err)
	const txQ = "SELECT name FROM users WHERE id = ?"
	queryTx := func(id int) string {
		vr := &Rows{}
		require.NoError(t, tx.Query(ctx, txQ, []any{id}, vr))
		require.True(t, vr.Next())
		var name string
		require.NoError(t, vr.Scan(&name))
		require.NoError(t, vr.Close())
		return name
	}
	require.Equal(t, "a8m", queryTx(1))
	require.Equal(t, "a8m", queryTx(3))
	require.NoError(t, tx.Commit())
	stats, _ = drv.StmtCacheStats()
	require.Equal(t, activeBeforeTx, stats.ActiveStmts) // tx entries discarded, driver entries remain

	// Schema change: DDL bypasses the cache and flushes existing entries.
	require.NoError(t, drv.Exec(ctx, "ALTER TABLE users ADD COLUMN age int DEFAULT 0", []any{}, nil))
	stats, _ = drv.StmtCacheStats()
	require.Greater(t, stats.Invalidations, int64(0))

	// The next execution rebuilds transparently against the new schema.
	vr := &Rows{}
	require.NoError(t, drv.Query(ctx, "SELECT name, age FROM users ORDER BY id", []any{}, vr))
	type user struct {
		name string
		age  int
	}
	var users []user
	for vr.Next() {
		var u user
		require.NoError(t, vr.Scan(&u.name, &u.age))
		users = append(users, u)
	}
	require.NoError(t, vr.Close())
	require.Equal(t, []user{{"a8m", 0}, {"alexsn", 0}, {"a8m", 0}}, users)
}
