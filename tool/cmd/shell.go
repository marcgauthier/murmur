package cmd

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/marcgauthier/murmur"
	"github.com/marcgauthier/murmur/tool/client"
	"github.com/marcgauthier/murmur/tool/format"
)

type ShellCommand struct {
	In io.Reader
}

func (c *ShellCommand) Name() string        { return "shell" }
func (c *ShellCommand) Description() string { return "Start an interactive SQL shell (REPL)" }
func (c *ShellCommand) Usage() string {
	return "murmur shell <data-dir | node-url>"
}

func init() {
	Register(&ShellCommand{})
}

type shellState struct {
	mode        string
	showHeaders bool
	showTimer   bool
	client      *client.Client
	db          *murmur.DB
	target      string
}

func (c *ShellCommand) Run(ctx context.Context, globalOpts GlobalOptions, args []string, stdout, stderr io.Writer) error {
	var target string
	for _, arg := range args {
		if !strings.HasPrefix(arg, "-") && target == "" {
			target = arg
		}
	}

	if target == "" {
		return fmt.Errorf("missing <data-dir | node-url>. Usage: %s", c.Usage())
	}

	state := &shellState{
		mode:        "table",
		showHeaders: true,
		showTimer:   false,
		target:      target,
	}

	isRemote := strings.HasPrefix(target, "http://") || strings.HasPrefix(target, "https://")
	if isRemote {
		cli, err := ClientFromOpts(target, globalOpts)
		if err != nil {
			return fmt.Errorf("connect to remote node: %w", err)
		}
		state.client = cli
	} else {
		db, err := openLocalDB(ctx, target, globalOpts, false)
		if err != nil {
			return fmt.Errorf("open local database %q: %w", target, err)
		}
		state.db = db
		defer db.Close()
	}

	fmt.Fprintf(stdout, "Murmur SQL Interactive Shell (v%s)\n", Version)
	fmt.Fprintf(stdout, "Connected to: %s\n", target)
	fmt.Fprintf(stdout, "Type \".help\" for usage instructions. Terminate statements with \";\".\n\n")

	in := c.In
	if in == nil {
		in = os.Stdin
	}
	scanner := bufio.NewScanner(in)
	var queryBuffer strings.Builder

	for {
		if queryBuffer.Len() == 0 {
			fmt.Fprint(stdout, "murmur> ")
		} else {
			fmt.Fprint(stdout, "   ...> ")
		}

		if !scanner.Scan() {
			break
		}

		line := strings.TrimSpace(scanner.Text())
		if line == "" && queryBuffer.Len() == 0 {
			continue
		}

		if queryBuffer.Len() == 0 && strings.HasPrefix(line, ".") {
			if done := state.handleDotCommand(ctx, line, stdout, stderr); done {
				break
			}
			continue
		}

		queryBuffer.WriteString(line)
		queryBuffer.WriteString(" ")

		if strings.HasSuffix(line, ";") {
			sqlText := strings.TrimSpace(queryBuffer.String())
			queryBuffer.Reset()
			if sqlText != "" && sqlText != ";" {
				state.executeSQL(ctx, sqlText, stdout, stderr)
			}
		}
	}

	fmt.Fprintln(stdout, "\nGoodbye!")
	return nil
}

func (s *shellState) handleDotCommand(ctx context.Context, line string, stdout, stderr io.Writer) bool {
	fields := strings.Fields(line)
	cmd := fields[0]

	switch cmd {
	case ".quit", ".exit", ".q":
		return true

	case ".help", ".h", "?":
		fmt.Fprintf(stdout, `Available dot-commands:
  .help                  Show this help text
  .tables [pattern]      List tables in the database
  .schema [table]        Show CREATE TABLE statement(s)
  .mode <mode>           Set output mode (table, json, csv, markdown)
  .headers <on|off>      Toggle table headers
  .timer <on|off>        Toggle execution timer display
  .status                Show connected database / node status
  .read <file.sql>       Execute SQL script from file
  .dump [table]          Dump database or table structure and data
  .quit, .exit           Exit shell
`)

	case ".tables":
		pattern := ""
		if len(fields) > 1 {
			pattern = fields[1]
		}
		s.listTables(ctx, pattern, stdout, stderr)

	case ".schema":
		tableName := ""
		if len(fields) > 1 {
			tableName = fields[1]
		}
		s.showSchema(ctx, tableName, stdout, stderr)

	case ".mode":
		if len(fields) < 2 {
			fmt.Fprintf(stdout, "Current output mode: %s\n", s.mode)
		} else {
			mode := strings.ToLower(fields[1])
			switch mode {
			case "table", "json", "csv", "markdown":
				s.mode = mode
				fmt.Fprintf(stdout, "Output mode set to: %s\n", s.mode)
			default:
				fmt.Fprintf(stderr, "Invalid mode %q. Allowed: table, json, csv, markdown\n", mode)
			}
		}

	case ".headers":
		if len(fields) < 2 {
			fmt.Fprintf(stdout, "Headers: %t\n", s.showHeaders)
		} else {
			s.showHeaders = strings.ToLower(fields[1]) == "on" || fields[1] == "1" || fields[1] == "true"
			fmt.Fprintf(stdout, "Headers: %t\n", s.showHeaders)
		}

	case ".timer":
		if len(fields) < 2 {
			fmt.Fprintf(stdout, "Timer: %t\n", s.showTimer)
		} else {
			s.showTimer = strings.ToLower(fields[1]) == "on" || fields[1] == "1" || fields[1] == "true"
			fmt.Fprintf(stdout, "Timer: %t\n", s.showTimer)
		}

	case ".status":
		s.showStatus(ctx, stdout, stderr)

	case ".read":
		if len(fields) < 2 {
			fmt.Fprintf(stderr, "Usage: .read <filename.sql>\n")
		} else {
			sqlBytes, err := os.ReadFile(fields[1])
			if err != nil {
				fmt.Fprintf(stderr, "Error reading file %q: %v\n", fields[1], err)
			} else {
				s.executeSQL(ctx, string(sqlBytes), stdout, stderr)
			}
		}

	case ".dump":
		targetTable := ""
		if len(fields) > 1 {
			targetTable = fields[1]
		}
		s.dumpSQL(ctx, targetTable, stdout, stderr)

	default:
		fmt.Fprintf(stderr, "Unknown dot-command %q. Type \".help\" for usage.\n", cmd)
	}

	return false
}

func (s *shellState) executeSQL(ctx context.Context, sqlText string, stdout, stderr io.Writer) {
	start := time.Now()

	if s.client != nil {
		isSelect := strings.HasPrefix(strings.ToUpper(strings.TrimSpace(sqlText)), "SELECT") ||
			strings.HasPrefix(strings.ToUpper(strings.TrimSpace(sqlText)), "PRAGMA") ||
			strings.HasPrefix(strings.ToUpper(strings.TrimSpace(sqlText)), "EXPLAIN")

		if isSelect {
			res, err := s.client.Query(ctx, sqlText)
			if err != nil {
				fmt.Fprintf(stderr, "Error: %v\n", err)
				return
			}
			elapsed := time.Since(start)
			s.render(stdout, res.Columns, res.Rows, elapsed)
			return
		}

		res, err := s.client.Exec(ctx, sqlText)
		if err != nil {
			fmt.Fprintf(stderr, "Error: %v\n", err)
			return
		}
		elapsed := time.Since(start)
		fmt.Fprintf(stdout, "Query OK, %d rows affected (%s)\n", res.RowsAffected, elapsed)
		return
	}

	stmts := splitStatements(sqlText)
	for _, stmt := range stmts {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" {
			continue
		}

		isSelect := strings.HasPrefix(strings.ToUpper(stmt), "SELECT") ||
			strings.HasPrefix(strings.ToUpper(stmt), "PRAGMA") ||
			strings.HasPrefix(strings.ToUpper(stmt), "EXPLAIN")

		stmtStart := time.Now()
		if isSelect {
			rows, err := s.db.QueryContext(ctx, stmt)
			if err != nil {
				fmt.Fprintf(stderr, "Error: %v\n", err)
				return
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
					fmt.Fprintf(stderr, "Scan error: %v\n", err)
					return
				}
				row := make([]string, len(cols))
				for i, v := range values {
					if v == nil {
						row[i] = "NULL"
					} else if b, ok := v.([]byte); ok {
						row[i] = string(b)
					} else {
						row[i] = fmt.Sprintf("%v", v)
					}
				}
				rowData = append(rowData, row)
			}
			rows.Close()

			elapsed := time.Since(stmtStart)
			s.render(stdout, cols, rowData, elapsed)
		} else {
			affected, err := handleDDLOrExec(ctx, s.db, stmt)
			if err != nil {
				fmt.Fprintf(stderr, "Error: %v\n", err)
				return
			}
			elapsed := time.Since(stmtStart)
			fmt.Fprintf(stdout, "Query OK, %d rows affected", affected)
			if s.showTimer {
				fmt.Fprintf(stdout, " (%s)", elapsed)
			}
			fmt.Fprintln(stdout)
		}
	}
}

func (s *shellState) render(w io.Writer, cols []string, rows [][]string, elapsed time.Duration) {
	switch s.mode {
	case "json":
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
		_ = format.RenderJSON(w, records, true)
	case "csv":
		headers := cols
		if !s.showHeaders {
			headers = nil
		}
		_ = format.RenderCSV(w, headers, rows)
	case "markdown":
		format.RenderTable(w, cols, rows, true)
		if s.showTimer {
			fmt.Fprintf(w, "\n*(%d rows in set, %s)*\n", len(rows), elapsed)
		}
	default: // table
		format.RenderTable(w, cols, rows, false)
		if s.showTimer {
			fmt.Fprintf(w, "(%d rows in set, %s)\n", len(rows), elapsed)
		}
	}
}

func (s *shellState) listTables(ctx context.Context, pattern string, stdout, stderr io.Writer) {
	query := "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
	if pattern != "" {
		query += fmt.Sprintf(" AND name LIKE '%%%s%%'", pattern)
	}
	query += " ORDER BY name;"

	if s.client != nil {
		res, err := s.client.Query(ctx, query)
		if err != nil {
			fmt.Fprintf(stderr, "Error: %v\n", err)
			return
		}
		for _, row := range res.Rows {
			if len(row) > 0 {
				fmt.Fprintln(stdout, row[0])
			}
		}
		return
	}

	rows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		fmt.Fprintf(stderr, "Error: %v\n", err)
		return
	}
	defer rows.Close()

	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err == nil {
			fmt.Fprintln(stdout, name)
		}
	}
}

func (s *shellState) showSchema(ctx context.Context, tableName string, stdout, stderr io.Writer) {
	query := "SELECT sql FROM sqlite_master WHERE type IN ('table', 'index', 'view') AND name NOT LIKE 'sqlite_%'"
	if tableName != "" {
		query += fmt.Sprintf(" AND name = '%s'", tableName)
	}
	query += " ORDER BY type DESC, name;"

	if s.client != nil {
		res, err := s.client.Query(ctx, query)
		if err != nil {
			fmt.Fprintf(stderr, "Error: %v\n", err)
			return
		}
		for _, row := range res.Rows {
			if len(row) > 0 && row[0] != "" {
				fmt.Fprintf(stdout, "%s;\n\n", row[0])
			}
		}
		return
	}

	rows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		fmt.Fprintf(stderr, "Error: %v\n", err)
		return
	}
	defer rows.Close()

	for rows.Next() {
		var ddl string
		if err := rows.Scan(&ddl); err == nil && ddl != "" {
			fmt.Fprintf(stdout, "%s;\n\n", ddl)
		}
	}
}

func (s *shellState) showStatus(ctx context.Context, stdout, stderr io.Writer) {
	if s.client != nil {
		st, err := s.client.Status(ctx)
		if err != nil {
			fmt.Fprintf(stderr, "Status error: %v\n", err)
			return
		}
		_ = format.RenderJSON(stdout, st, true)
		return
	}

	st := s.db.Status()
	format.RenderKV(stdout, [][2]string{
		{"Node ID", st.NodeID.String()},
		{"Cluster DB ID", st.DBID.String()},
		{"State", st.State.String()},
		{"Writes Allowed", fmt.Sprintf("%t", st.State.WritesAllowed())},
		{"Generation", fmt.Sprintf("%d", st.StateGeneration)},
		{"HLC Watermark", fmt.Sprintf("%d", st.HLC)},
		{"Schema Epoch", fmt.Sprintf("%d", st.SchemaEpoch)},
		{"Schema Hash", fmt.Sprintf("%x", st.SchemaHash)},
		{"Pebble Size", fmt.Sprintf("%d bytes", st.PebbleSizeBytes)},
		{"Uptime", st.Uptime.String()},
	})
}

func (s *shellState) dumpSQL(ctx context.Context, targetTable string, stdout, stderr io.Writer) {
	cmd := &ExportCommand{}
	args := []string{s.target, "--format=sql"}
	if targetTable != "" {
		args = append(args, "--table="+targetTable)
	}
	_ = cmd.Run(ctx, GlobalOptions{}, args, stdout, stderr)
}
