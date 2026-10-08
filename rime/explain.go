package rime

import (
	"fmt"
	"strings"
)

// Explain returns the query execution plan in human-readable form.
func (q *Query[T]) Explain() string {
	return formatPlan(q.tbl.cachedOrPlan(q, q.tbl.db.latest()))
}

func formatPlan[T any](p *plan[T]) string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "TABLE %s\n", p.table)
	fmt.Fprintf(&sb, "ROWS %d\n", p.rows)
	if p.stale {
		sb.WriteString("SNAPSHOT STALE — index seeks disabled\n")
	}
	sb.WriteString(p.strategy)
	if p.index != "" {
		fmt.Fprintf(&sb, " %s", p.index)
	}
	sb.WriteString("\n")
	for _, pr := range p.predicates {
		fmt.Fprintf(&sb, "    %s\n", pr)
	}
	fmt.Fprintf(&sb, "ESTIMATED CANDIDATES %d\n", p.estCandidates)
	if len(p.filters) > 0 {
		sb.WriteString("FILTER\n")
		for _, f := range p.filters {
			fmt.Fprintf(&sb, "    %s\n", f)
		}
	}
	if len(p.orderDesc) > 0 {
		sb.WriteString("ORDER BY " + strings.Join(p.orderDesc, ", "))
		if p.ordered {
			sb.WriteString(" (from index)")
		} else {
			sb.WriteString(" (sort)")
		}
		sb.WriteString("\n")
	}
	if p.hasLimit {
		fmt.Fprintf(&sb, "LIMIT %d\n", p.limit)
	}
	return sb.String()
}
