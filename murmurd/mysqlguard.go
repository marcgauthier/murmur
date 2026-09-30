package murmurd

import (
	"fmt"
	"strings"

	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/analyzer"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"
)

// This file is the MySQL-side Murmur schema guard. go-mysql-server
// executes CREATE TABLE by calling our TableCreator first (which
// commits the table via Migrate) and creating secondary indexes,
// checks, and foreign keys afterwards, so rejecting those in the
// provider would leave a committed ghost table behind. The guard
// below runs as a pre-analyze rule instead: it sees the parsed plan
// before execution and rejects anything outside Murmur's replicated
// schema before anything commits. Provider.CreateTable repeats the
// column-level checks as a second layer for paths that bypass
// analysis.

// murmurWireRuleID identifies the guard rule. Analyzer RuleIds are a
// small iota const block; this value sits far outside it.
const murmurWireRuleID analyzer.RuleId = 1000

// newMurmurEngine builds the MySQL engine over the provider with the
// wire guard installed ahead of the default analysis.
func newMurmurEngine(p *Provider) *sqle.Engine {
	a := analyzer.NewBuilder(p).AddPreAnalyzeRule(murmurWireRuleID, murmurWireGuard).Build()
	return sqle.New(a, nil)
}

// murmurWireGuard rejects DDL outside Murmur's replicated schema.
// It never transforms the tree; it only fails closed.
func murmurWireGuard(_ *sql.Context, _ *analyzer.Analyzer, n sql.Node, _ *plan.Scope, _ analyzer.RuleSelector, _ *sql.QueryFlags) (sql.Node, transform.TreeIdentity, error) {
	var guardErr error
	transform.Inspect(n, func(n sql.Node) bool {
		if guardErr != nil {
			return false
		}
		switch t := n.(type) {
		case *plan.CreateTable:
			guardErr = guardCreateTable(t)
		case *plan.AlterIndex:
			guardErr = guardAlterIndex(t)
		}
		return guardErr == nil
	})
	if guardErr != nil {
		return n, transform.SameTree, guardErr
	}
	return n, transform.SameTree, nil
}

// guardCreateTable rejects CREATE TABLE options Murmur cannot
// replicate: secondary indexes (unique or plain), foreign keys,
// checks, defaults, autoincrement, generated columns, and
// unsupported table options. Primary-key shape (single `id` blob)
// is validated in CreateTable after analysis.
func guardCreateTable(n *plan.CreateTable) error {
	name := n.Name()
	if n.Temporary() {
		return fmt.Errorf("murmurd: CREATE TABLE %s: temporary tables are not supported (single shared materialization)", name)
	}
	if n.Like() != nil || n.Select() != nil {
		// LIKE / AS SELECT resolve their schema during analysis;
		// CreateTable validates the resolved columns then.
		return nil
	}
	for _, c := range n.PkSchema().Schema {
		if err := guardColumnOption(name, c); err != nil {
			return err
		}
	}
	for _, idx := range n.Indexes() {
		if idx.IsPrimary() {
			continue
		}
		cols := strings.Join(idx.ColumnNames(), ", ")
		if idx.IsUnique() {
			return fmt.Errorf("murmurd: CREATE TABLE %s: secondary UNIQUE index on (%s) is not supported (murmur replicates only the primary-key index)", name, cols)
		}
		return fmt.Errorf("murmurd: CREATE TABLE %s: secondary index on (%s) is local-only in murmur and not managed over the wire (see LocalDDL)", name, cols)
	}
	if len(n.ForeignKeys()) > 0 {
		return fmt.Errorf("murmurd: CREATE TABLE %s: FOREIGN KEY constraints are not supported over the wire (murmur leaves foreign keys unenforced; express related mutations explicitly)", name)
	}
	if len(n.Checks()) > 0 {
		return fmt.Errorf("murmurd: CREATE TABLE %s: CHECK constraints are not supported (murmur does not replicate check constraints)", name)
	}
	if _, ok := n.TableOpts["auto_increment"]; ok {
		return fmt.Errorf("murmurd: CREATE TABLE %s: AUTO_INCREMENT table option is not supported (murmur forbids autoincrement; generate key values client-side)", name)
	}
	return nil
}

// guardAlterIndex rejects standalone CREATE INDEX / CREATE UNIQUE
// INDEX and index-bearing ALTER TABLE, which all plan as AlterIndex.
func guardAlterIndex(n *plan.AlterIndex) error {
	table := "table"
	if named, ok := n.Table.(sql.Nameable); ok && named.Name() != "" {
		table = named.Name()
	}
	if n.Action == plan.IndexAction_Create {
		if n.Constraint == sql.IndexConstraint_Unique {
			return fmt.Errorf("murmurd: secondary UNIQUE index %q on %s is not supported (murmur replicates only the primary-key index)", n.IndexName, table)
		}
		return fmt.Errorf("murmurd: secondary index %q on %s is local-only in murmur and not managed over the wire (see LocalDDL)", n.IndexName, table)
	}
	return fmt.Errorf("murmurd: index %q on %s is not managed over the wire (murmur keeps no secondary indexes)", n.IndexName, table)
}

// guardColumnOption rejects per-column options Murmur does not model.
// It is shared by the pre-analyze guard and Provider.CreateTable so
// both layers report identical reasons.
func guardColumnOption(table string, c *sql.Column) error {
	switch {
	case c.AutoIncrement:
		return fmt.Errorf("murmurd: CREATE TABLE %s column %s: AUTO_INCREMENT is not supported (murmur forbids autoincrement; generate key values client-side)", table, c.Name)
	case c.Default != nil:
		return fmt.Errorf("murmurd: CREATE TABLE %s column %s: DEFAULT is not supported (murmur columns have no defaults)", table, c.Name)
	case c.Generated != nil:
		return fmt.Errorf("murmurd: CREATE TABLE %s column %s: generated columns are not supported (murmur models no generated columns)", table, c.Name)
	case c.OnUpdate != nil:
		return fmt.Errorf("murmurd: CREATE TABLE %s column %s: ON UPDATE is not supported (murmur models no on-update clauses)", table, c.Name)
	default:
		return nil
	}
}
