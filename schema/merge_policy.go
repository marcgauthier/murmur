package schema

import "fmt"

// MergePolicy defines the immutable merge semantics of a replicated column.
type MergePolicy uint8

const (
	LWW MergePolicy = iota
	PN_COUNTER
	OR_SET
	MAX
	MIN
)

func (p MergePolicy) String() string {
	switch p {
	case LWW:
		return "LWW"
	case PN_COUNTER:
		return "PN_COUNTER"
	case OR_SET:
		return "OR_SET"
	case MAX:
		return "MAX"
	case MIN:
		return "MIN"
	default:
		return fmt.Sprintf("UNKNOWN(%d)", p)
	}
}

func (c ColumnSchema) validateMergePolicy() error {
	switch c.MergePolicy {
	case LWW:
		return nil
	case PN_COUNTER, OR_SET:
		if c.Type == ColText {
			return nil
		}
	case MAX, MIN:
		if c.Type == ColInteger || c.Type == ColReal {
			return nil
		}
	}
	return fmt.Errorf("schema: column %q has incompatible merge policy %s/type %s: %w", c.Name, c.MergePolicy, c.Type, ErrUnsupportedSchema)
}

func hasMergePolicies(tables []TableSchema) bool {
	for _, t := range tables {
		for _, c := range t.Columns {
			if c.MergePolicy != LWW {
				return true
			}
		}
	}
	return false
}
