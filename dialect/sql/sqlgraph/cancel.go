// Copyright 2019-present Facebook Inc. All rights reserved.
// This source code is licensed under the Apache 2.0 license found
// in the LICENSE file in the root directory of this source tree.

package sqlgraph

import (
	"context"
	"errors"
	"fmt"

	"entgo.io/ent/dialect"
	"entgo.io/ent/dialect/sql"
)

// ReadOp identifies the category of a logical read operation.
type ReadOp string

// Read operation categories.
const (
	// OpQueryNodes is a node read, such as a single or a batch lookup.
	OpQueryNodes ReadOp = "query nodes"
	// OpCountNodes is a count read (nodes or edges).
	OpCountNodes ReadOp = "count nodes"
	// OpQueryEdges is an edge read, such as the ones used by eager loading.
	OpQueryEdges ReadOp = "query edges"
)

// ReadStage identifies the stage of a logical read at which it was aborted.
type ReadStage string

// Read stages.
const (
	// StageEntry is the entry of the read, before any statement was issued.
	StageEntry ReadStage = "entry"
	// StageStatement is the stage immediately before a (possibly derived)
	// statement is sent to the driver.
	StageStatement ReadStage = "statement"
	// StageEagerLoad is the stage before a derived eager-loading statement.
	StageEagerLoad ReadStage = "eager-load"
	// StageScan is the stage while an already opened result set is consumed.
	StageScan ReadStage = "scan"
)

// AbortKind distinguishes an active cancellation from a deadline timeout.
type AbortKind uint8

// Abort kinds.
const (
	// KindCanceled reports that the caller canceled the context.
	KindCanceled AbortKind = iota + 1
	// KindDeadline reports that the context deadline was exceeded.
	KindDeadline
)

// AbortError is returned when a logical read is aborted because its context
// was canceled or its deadline was exceeded. It carries the operation
// category and the stage the read terminated at, and wraps the originating
// context error. Callers can distinguish between an active cancellation and
// a timeout using IsCanceled / IsDeadlineExceeded, or using errors.Is with
// context.Canceled and context.DeadlineExceeded.
type AbortError struct {
	// Kind is the abort kind (active cancellation or deadline exceeded).
	Kind AbortKind
	// Op is the category of the read operation.
	Op ReadOp
	// Stage is the stage the read terminated at.
	Stage ReadStage
	// Target is the optional table or edge-table the read targeted.
	Target string
	err    error
}

// Error implements the error interface.
func (e *AbortError) Error() string {
	msg := fmt.Sprintf("sqlgraph: %s aborted at %s", e.Op, e.Stage)
	if e.Target != "" {
		msg += " [" + e.Target + "]"
	}
	return msg + ": " + e.err.Error()
}

// Unwrap returns the originating context error, so errors.Is works with
// context.Canceled and context.DeadlineExceeded.
func (e *AbortError) Unwrap() error { return e.err }

// IsAborted reports if the error chain contains an *AbortError.
func IsAborted(err error) bool {
	var e *AbortError
	return errors.As(err, &e)
}

// IsCanceled reports if the error chain contains an *AbortError caused by an
// active context cancellation (as opposed to a deadline timeout).
func IsCanceled(err error) bool {
	var e *AbortError
	return errors.As(err, &e) && e.Kind == KindCanceled
}

// IsDeadlineExceeded reports if the error chain contains an *AbortError
// caused by an exceeded context deadline (as opposed to an active
// cancellation).
func IsDeadlineExceeded(err error) bool {
	var e *AbortError
	return errors.As(err, &e) && e.Kind == KindDeadline
}

// newAbortError creates an *AbortError from the given context error.
func newAbortError(cause error, op ReadOp, stage ReadStage, target string) *AbortError {
	e := &AbortError{Op: op, Stage: stage, Target: target, err: cause}
	if errors.Is(cause, context.DeadlineExceeded) {
		e.Kind = KindDeadline
	} else {
		e.Kind = KindCanceled
	}
	return e
}

// readExec bounds one logical read: a read starts (entry check) once, and
// every derived statement revalidates the context before touching the
// driver. On abort, the transactional driver (if any) is marked
// non-continuable.
type readExec struct {
	ctx    context.Context
	drv    dialect.Driver
	op     ReadOp
	target string
}

// beginRead starts a logical read and performs the entry cancellation and
// deadline checks, before any statement is issued.
func beginRead(ctx context.Context, drv dialect.Driver, op ReadOp, target string) (*readExec, error) {
	r := &readExec{ctx: ctx, drv: drv, op: op, target: target}
	if err := ctx.Err(); err != nil {
		return nil, r.abort(StageEntry, err)
	}
	return r, nil
}

// check revalidates the context before a derived statement is issued or an
// open result set is consumed further.
func (r *readExec) check(stage ReadStage) error {
	if err := r.ctx.Err(); err != nil {
		return r.abort(stage, err)
	}
	return nil
}

// fail normalizes a context cancellation/deadline error that originated
// from the driver or from an open result set into an *AbortError. Other
// errors are returned unchanged.
func (r *readExec) fail(stage ReadStage, err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return r.abort(stage, err)
	}
	return err
}

// abort reports the structured abort error and marks the transactional
// driver (if any) non-continuable. It never commits or rolls back.
func (r *readExec) abort(stage ReadStage, cause error) error {
	e := newAbortError(cause, r.op, stage, r.target)
	markAborted(r.drv, e)
	return e
}

// query revalidates the context and issues a row-returning statement only if
// the read is still alive. No statement is sent after an abort, and context
// errors reported by the driver are normalized into *AbortError.
func (r *readExec) query(stage ReadStage, query string, args []any, rows *sql.Rows) error {
	if err := r.check(stage); err != nil {
		return err
	}
	return r.fail(stage, r.drv.Query(r.ctx, query, args, rows))
}

// markAborted marks an abortable (transactional) driver non-continuable.
func markAborted(drv dialect.Driver, err error) {
	if a, ok := drv.(dialect.Abortable); ok {
		a.Abort(err)
	}
}

// CheckRead reports an *AbortError if the context is already canceled or
// past its deadline, and marks the transactional driver (if any)
// non-continuable. It is intended as an entry / pre-derived-statement check
// for read chains that issue statements outside of QueryNodes,
// CountNodes and QueryEdges (e.g. generated eager-loading boundaries).
func CheckRead(ctx context.Context, drv dialect.Driver, op ReadOp, stage ReadStage) error {
	if err := ctx.Err(); err != nil {
		return (&readExec{ctx: ctx, drv: drv, op: op}).abort(stage, err)
	}
	return nil
}

// QueryRows revalidates the context and executes a row-returning read
// statement through drv. Context cancellation and deadline errors reported
// by the driver are normalized into *AbortError and mark the transactional
// driver (if any) non-continuable. Other driver errors are returned
// unchanged.
func QueryRows(ctx context.Context, drv dialect.Driver, op ReadOp, query string, args []any, rows *sql.Rows) error {
	r, err := beginRead(ctx, drv, op, "")
	if err != nil {
		return err
	}
	return r.query(StageStatement, query, args, rows)
}
