package murmur

import (
	"context"
	"fmt"

	"github.com/marcgauthier/murmur/q"
	"github.com/marcgauthier/murmur/rime"
)

// ItemCompiledQuery is a reusable item query with positional parameters.
// Compile it once from an ItemQuery, then bind concrete arguments per
// execution. The zero value is not usable; the handle is immutable and safe
// for concurrent use.
type ItemCompiledQuery struct {
	db       *DB
	binding  *itemBinding
	matchers []q.Matcher
	params   []q.Placeholder
	index    map[q.Placeholder]int
	orders   []itemOrder
	limit    int
	offset   int
	tx       *Tx
	read     *recordReadTx
	ctx      context.Context
	err      error
}

// Compile validates the query once and records its q.Placeholder placeholders as
// positional bind slots in matcher order (depth-first, left to right).
// Reusing one placeholder value in several positions binds a single argument
// to every position. Ordering, limit, and offset are fixed at compile time;
// transaction and context bindings carry over and can be rebound per copy
// with In, inRead, and WithContext.
func (iq *ItemQuery) Compile() *ItemCompiledQuery {
	cq := &ItemCompiledQuery{limit: -1}
	if err := iq.check(); err != nil {
		cq.err = err
		return cq
	}
	cq.db = iq.db
	cq.binding = iq.binding
	cq.index = make(map[q.Placeholder]int)
	for _, matcher := range iq.matchers {
		deref, err := derefItemMatcher(matcher)
		if err != nil {
			cq.err = err
			return cq
		}
		cq.matchers = append(cq.matchers, deref)
		if err := cq.collect(deref); err != nil {
			cq.err = err
			return cq
		}
	}
	cq.orders = append([]itemOrder(nil), iq.orders...)
	cq.limit, cq.offset = iq.limit, iq.offset
	cq.tx, cq.read, cq.ctx = iq.tx, iq.read, iq.ctx
	return cq
}

func (cq *ItemCompiledQuery) clone() *ItemCompiledQuery {
	if cq == nil {
		return &ItemCompiledQuery{limit: -1}
	}
	out := *cq
	out.matchers = append([]q.Matcher(nil), cq.matchers...)
	out.params = append([]q.Placeholder(nil), cq.params...)
	out.orders = append([]itemOrder(nil), cq.orders...)
	return &out
}

// In binds the query to a write transaction's snapshot and staged overlay,
// replacing any read-transaction binding.
func (cq *ItemCompiledQuery) In(tx *Tx) *ItemCompiledQuery {
	out := cq.clone()
	out.tx = tx
	out.read = nil
	return out
}

// inRead binds the query to a pinned read snapshot, replacing any write
// transaction binding.
func (cq *ItemCompiledQuery) inRead(tx *recordReadTx) *ItemCompiledQuery {
	out := cq.clone()
	out.read = tx
	out.tx = nil
	return out
}

// WithContext binds cancellation to the query.
func (cq *ItemCompiledQuery) WithContext(ctx context.Context) *ItemCompiledQuery {
	out := cq.clone()
	if ctx != nil {
		out.ctx = ctx
	}
	return out
}

func (cq *ItemCompiledQuery) check() error {
	if cq == nil {
		return rime.ErrBadView
	}
	if cq.err != nil {
		return cq.err
	}
	if cq.db == nil || cq.binding == nil {
		return rime.ErrBadView
	}
	return nil
}

// collect records placeholder slots in encounter order.
func (cq *ItemCompiledQuery) collect(matcher q.Matcher) error {
	switch m := matcher.(type) {
	case q.Comparison:
		if p, ok := m.Value.(q.Placeholder); ok {
			cq.addParam(p)
		}
		return nil
	case q.InMatcher:
		for _, value := range m.Values {
			if p, ok := value.(q.Placeholder); ok {
				cq.addParam(p)
			}
		}
		return nil
	case q.StringMatcher:
		return nil
	case q.AndMatcher:
		for _, child := range m.Matchers {
			child, err := derefItemMatcher(child)
			if err != nil {
				return err
			}
			if err := cq.collect(child); err != nil {
				return err
			}
		}
		return nil
	case q.OrMatcher:
		for _, child := range m.Matchers {
			child, err := derefItemMatcher(child)
			if err != nil {
				return err
			}
			if err := cq.collect(child); err != nil {
				return err
			}
		}
		return nil
	case q.NotMatcher:
		if m.Matcher == nil {
			return fmt.Errorf("murmur: Not requires a matcher")
		}
		child, err := derefItemMatcher(m.Matcher)
		if err != nil {
			return err
		}
		return cq.collect(child)
	default:
		return fmt.Errorf("murmur: unsupported matcher %T", matcher)
	}
}

func (cq *ItemCompiledQuery) addParam(p q.Placeholder) {
	if _, ok := cq.index[p]; ok {
		return
	}
	cq.index[p] = len(cq.params)
	cq.params = append(cq.params, p)
}

// bind substitutes args for placeholders and coerces each occurrence against
// its own field, so one shared placeholder can feed compatible fields while
// incompatible uses fail naming the field.
func (cq *ItemCompiledQuery) bind(args []any) ([]q.Matcher, error) {
	if len(args) != len(cq.params) {
		return nil, fmt.Errorf("murmur: compiled query wants %d args, got %d", len(cq.params), len(args))
	}
	out := make([]q.Matcher, len(cq.matchers))
	for i, matcher := range cq.matchers {
		bound, err := cq.substitute(matcher, args)
		if err != nil {
			return nil, err
		}
		out[i] = bound
	}
	return out, nil
}

func (cq *ItemCompiledQuery) substitute(matcher q.Matcher, args []any) (q.Matcher, error) {
	matcher, err := derefItemMatcher(matcher)
	if err != nil {
		return nil, err
	}
	switch m := matcher.(type) {
	case q.Comparison:
		p, ok := m.Value.(q.Placeholder)
		if !ok {
			return m, nil
		}
		value, err := cq.arg(m.Field, p, args)
		if err != nil {
			return nil, err
		}
		m.Value = value
		return m, nil
	case q.InMatcher:
		values := append([]any(nil), m.Values...)
		for i, value := range values {
			p, ok := value.(q.Placeholder)
			if !ok {
				continue
			}
			bound, err := cq.arg(m.Field, p, args)
			if err != nil {
				return nil, err
			}
			values[i] = bound
		}
		m.Values = values
		return m, nil
	case q.StringMatcher:
		return m, nil
	case q.AndMatcher:
		kids := make([]q.Matcher, len(m.Matchers))
		for i, child := range m.Matchers {
			bound, err := cq.substitute(child, args)
			if err != nil {
				return nil, err
			}
			kids[i] = bound
		}
		m.Matchers = kids
		return m, nil
	case q.OrMatcher:
		kids := make([]q.Matcher, len(m.Matchers))
		for i, child := range m.Matchers {
			bound, err := cq.substitute(child, args)
			if err != nil {
				return nil, err
			}
			kids[i] = bound
		}
		m.Matchers = kids
		return m, nil
	case q.NotMatcher:
		if m.Matcher == nil {
			return nil, fmt.Errorf("murmur: Not requires a matcher")
		}
		bound, err := cq.substitute(m.Matcher, args)
		if err != nil {
			return nil, err
		}
		m.Matcher = bound
		return m, nil
	default:
		return nil, fmt.Errorf("murmur: unsupported matcher %T", matcher)
	}
}

func (cq *ItemCompiledQuery) arg(field string, p q.Placeholder, args []any) (any, error) {
	fieldType, ok := cq.binding.fields[field]
	if !ok {
		return nil, fmt.Errorf("murmur: unknown field %q for %s", field, cq.binding.goType)
	}
	at, ok := cq.index[p]
	if !ok {
		return nil, fmt.Errorf("murmur: unknown query parameter")
	}
	coerced, err := coerceItemValue(fieldType, args[at])
	if err != nil {
		return nil, fmt.Errorf("murmur: param %d for field %q: %w", at+1, field, err)
	}
	return coerced.Interface(), nil
}

func (cq *ItemCompiledQuery) query(args []any) (*ItemQuery, error) {
	if err := cq.check(); err != nil {
		return nil, err
	}
	matchers, err := cq.bind(args)
	if err != nil {
		return nil, err
	}
	return &ItemQuery{
		db: cq.db, binding: cq.binding, matchers: matchers,
		orders: cq.orders, limit: cq.limit, offset: cq.offset,
		tx: cq.tx, read: cq.read, ctx: cq.ctx,
	}, nil
}

// FindInto binds args and assigns detached records to dest, which must be a
// *[]T or *[]*T for the queried struct type.
func (cq *ItemCompiledQuery) FindInto(dest any, args ...any) error {
	iq, err := cq.query(args)
	if err != nil {
		return err
	}
	return iq.FindInto(dest)
}

// FirstInto binds args and assigns the first match to dest, which must be a
// *T for the queried struct type. It returns rime.ErrNotFound when no record
// matches.
func (cq *ItemCompiledQuery) FirstInto(dest any, args ...any) error {
	iq, err := cq.query(args)
	if err != nil {
		return err
	}
	return iq.FirstInto(dest)
}

// Count binds args and returns the number of matching records.
func (cq *ItemCompiledQuery) Count(args ...any) (int, error) {
	iq, err := cq.query(args)
	if err != nil {
		return 0, err
	}
	return iq.Count()
}

// Exists binds args and reports whether at least one record matches.
func (cq *ItemCompiledQuery) Exists(args ...any) (bool, error) {
	iq, err := cq.query(args)
	if err != nil {
		return false, err
	}
	return iq.Exists()
}

// Explain binds args and returns the selected RIME plan without executing
// the query.
func (cq *ItemCompiledQuery) Explain(args ...any) (string, error) {
	iq, err := cq.query(args)
	if err != nil {
		return "", err
	}
	return iq.Explain()
}
