package cmd

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/marcgauthier/murmur/tool/format"
)

type QueryCommand struct{}

func (c *QueryCommand) Name() string        { return "query" }
func (c *QueryCommand) Description() string { return "Execute one-shot SQL query or run SQL script" }
func (c *QueryCommand) Usage() string {
	return "murmur query <data-dir | node-url> [sql-statement] [--format=table|json|csv|markdown] [--timing]"
}

func init() {
	cmd := &QueryCommand{}
	Register(cmd)
	Register(&aliasCmd{Command: cmd, name: "exec"})
}

type aliasCmd struct {
	Command
	name string
}

func (a *aliasCmd) Name() string { return a.name }

func (c *QueryCommand) Run(ctx context.Context, globalOpts GlobalOptions, args []string, stdout, stderr io.Writer) error {
	var target, sqlStr, outputFmt string
	var timing bool

	for _, arg := range args {
		switch {
		case strings.HasPrefix(arg, "--format="):
			outputFmt = strings.TrimPrefix(arg, "--format=")
		case strings.HasPrefix(arg, "--file="):
			filePath := strings.TrimPrefix(arg, "--file=")
			fileData, err := os.ReadFile(filePath)
			if err != nil {
				return fmt.Errorf("read SQL file: %w", err)
			}
			sqlStr = string(fileData)
		case arg == "--timing":
			timing = true
		case !strings.HasPrefix(arg, "-"):
			if target == "" {
				target = arg
			} else if sqlStr == "" {
				sqlStr = arg
			} else {
				sqlStr += " " + arg
			}
		}
	}

	if target == "" {
		return fmt.Errorf("missing <data-dir | node-url>. Usage: %s", c.Usage())
	}

	if sqlStr == "" {
		data, err := io.ReadAll(os.Stdin)
		if err != nil {
			return fmt.Errorf("read query from stdin: %w", err)
		}
		sqlStr = strings.TrimSpace(string(data))
		if sqlStr == "" {
			return fmt.Errorf("empty SQL query")
		}
	}

	start := time.Now()

	// 1. Remote node connection
	if strings.HasPrefix(target, "http://") || strings.HasPrefix(target, "https://") {
		client, err := ClientFromOpts(target, globalOpts)
		if err != nil {
			return err
		}

		isSelect := strings.HasPrefix(strings.ToUpper(strings.TrimSpace(sqlStr)), "SELECT") ||
			strings.HasPrefix(strings.ToUpper(strings.TrimSpace(sqlStr)), "PRAGMA") ||
			strings.HasPrefix(strings.ToUpper(strings.TrimSpace(sqlStr)), "EXPLAIN")

		if isSelect {
			res, err := client.Query(ctx, sqlStr)
			if err != nil {
				return err
			}
			elapsed := time.Since(start)
			return renderOutput(stdout, res.Columns, res.Rows, outputFmt, globalOpts, timing, elapsed)
		}

		execRes, err := client.Exec(ctx, sqlStr)
		if err != nil {
			return err
		}
		elapsed := time.Since(start)
		if globalOpts.JSON || outputFmt == "json" {
			return format.RenderJSON(stdout, map[string]any{
				"rows_affected": execRes.RowsAffected,
				"elapsed_ms":    elapsed.Milliseconds(),
			}, true)
		}
		fmt.Fprintf(stdout, "Query OK, %d rows affected (%s)\n", execRes.RowsAffected, elapsed)
		return nil
	}

	// 2. Local database connection
	db, err := openLocalDB(ctx, target, globalOpts, false)
	if err != nil {
		return fmt.Errorf("open database: %w", err)
	}
	defer db.Close()

	statements := splitStatements(sqlStr)
	for idx, stmt := range statements {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" {
			continue
		}

		isSelect := strings.HasPrefix(strings.ToUpper(stmt), "SELECT") ||
			strings.HasPrefix(strings.ToUpper(stmt), "PRAGMA") ||
			strings.HasPrefix(strings.ToUpper(stmt), "EXPLAIN")

		stmtStart := time.Now()
		if isSelect {
			rows, err := db.QueryContext(ctx, stmt)
			if err != nil {
				return fmt.Errorf("query error at statement %d: %w", idx+1, err)
			}

			cols := rows.Columns()

			var rowData [][]string
			for rows.Next() {
				values := make([]any, len(cols))
				valuePtrs := make([]any, len(cols))
				for i := range values {
					valuePtrs[i] = &values[i]
				}

				if err := rows.Scan(valuePtrs...); err != nil {
					rows.Close()
					return err
				}

				row := make([]string, len(cols))
				for i, val := range values {
					if val == nil {
						row[i] = "NULL"
					} else if b, ok := val.([]byte); ok {
						row[i] = string(b)
					} else {
						row[i] = fmt.Sprintf("%v", val)
					}
				}
				rowData = append(rowData, row)
			}
			rows.Close()

			elapsed := time.Since(stmtStart)
			if err := renderOutput(stdout, cols, rowData, outputFmt, globalOpts, timing, elapsed); err != nil {
				return err
			}
		} else {
			affected, err := handleDDLOrExec(ctx, db, stmt)
			if err != nil {
				return fmt.Errorf("exec error at statement %d: %w", idx+1, err)
			}
			elapsed := time.Since(stmtStart)

			if globalOpts.JSON || outputFmt == "json" {
				_ = format.RenderJSON(stdout, map[string]any{
					"statement_index": idx + 1,
					"rows_affected":   affected,
					"elapsed_ms":      elapsed.Milliseconds(),
				}, true)
			} else {
				fmt.Fprintf(stdout, "Query OK, %d rows affected (%s)\n", affected, elapsed)
			}
		}
	}

	return nil
}

func renderOutput(stdout io.Writer, cols []string, rows [][]string, outputFmt string, globalOpts GlobalOptions, timing bool, elapsed time.Duration) error {
	if globalOpts.JSON || outputFmt == "json" {
		var records []map[string]any
		for _, row := range rows {
			rec := make(map[string]any)
			for i, col := range cols {
				if i < len(row) {
					rec[col] = row[i]
				}
			}
			records = append(records, rec)
		}
		return format.RenderJSON(stdout, records, true)
	}

	if outputFmt == "csv" {
		return format.RenderCSV(stdout, cols, rows)
	}

	isMarkdown := globalOpts.Markdown || outputFmt == "markdown"
	format.RenderTable(stdout, cols, rows, isMarkdown)

	if timing {
		fmt.Fprintf(stdout, "(%d rows in set, %s)\n", len(rows), elapsed)
	}
	return nil
}

func splitStatements(sqlText string) []string {
	var stmts []string
	var buf strings.Builder
	inQuote := rune(0)

	for _, r := range sqlText {
		if inQuote != 0 {
			buf.WriteRune(r)
			if r == inQuote {
				inQuote = 0
			}
			continue
		}

		if r == '\'' || r == '"' || r == '`' {
			inQuote = r
			buf.WriteRune(r)
			continue
		}

		if r == ';' {
			s := strings.TrimSpace(buf.String())
			if s != "" {
				stmts = append(stmts, s)
			}
			buf.Reset()
			continue
		}

		buf.WriteRune(r)
	}

	if s := strings.TrimSpace(buf.String()); s != "" {
		stmts = append(stmts, s)
	}
	return stmts
}

var _ = sql.ErrNoRows
