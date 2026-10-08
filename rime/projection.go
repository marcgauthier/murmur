package rime

import "context"

// Projection constructs smaller result types from matches. Unlike normal
// queries — which return shared immutable native pointers — projection
// necessarily creates new values, so it is opt-in rather than the default
// query path. (Package-level functions: Go methods cannot introduce new type
// parameters, so projection lives outside the Query method set.)

// Map projects an already-fetched row slice.
func Map[T any, R any](rows []*T, fn func(*T) R) []R {
	out := make([]R, len(rows))
	for i, r := range rows {
		out[i] = fn(r)
	}
	return out
}

// Project executes q and maps each match through fn.
func Project[T any, R any](q *Query[T], fn func(*T) R) ([]R, error) {
	return projectContext(q.context(), q, fn)
}

// ProjectContext is Project with cancellation.
func ProjectContext[T any, R any](ctx context.Context, q *Query[T], fn func(*T) R) ([]R, error) {
	return projectContext(ctx, q, fn)
}

func projectContext[T any, R any](ctx context.Context, q *Query[T], fn func(*T) R) ([]R, error) {
	rows, err := q.findContext(ctx, q.tx)
	if err != nil {
		return nil, err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	out := make([]R, len(rows))
	for i, r := range rows {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		out[i] = fn(r)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return out, nil
}
