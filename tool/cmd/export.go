package cmd

import (
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/marcgauthier/murmur/tool/format"
)

type ExportCommand struct{}

func (c *ExportCommand) Name() string        { return "export" }
func (c *ExportCommand) Description() string { return "Export table data or full database dump" }
func (c *ExportCommand) Usage() string {
	return "murmur export <data-dir | node-url> [--table=NAME] [--format=csv|json|sql]"
}

func init() {
	cmd := &ExportCommand{}
	Register(cmd)
	Register(&aliasCmd{Command: cmd, name: "dump"})
}

func (c *ExportCommand) Run(ctx context.Context, globalOpts GlobalOptions, args []string, stdout, stderr io.Writer) error {
	var target, tableName, outputPath string
	formatType := ""

	for _, arg := range args {
		switch {
		case strings.HasPrefix(arg, "--table="):
			tableName = strings.TrimPrefix(arg, "--table=")
		case strings.HasPrefix(arg, "--format="):
			formatType = strings.ToLower(strings.TrimPrefix(arg, "--format="))
		case strings.HasPrefix(arg, "--output="):
			outputPath = strings.TrimPrefix(arg, "--output=")
		case !strings.HasPrefix(arg, "-"):
			if target == "" {
				target = arg
			} else if tableName == "" {
				tableName = arg
			}
		}
	}

	if target == "" {
		return fmt.Errorf("missing <data-dir | node-url>. Usage: %s", c.Usage())
	}

	if formatType == "" {
		if tableName == "" {
			formatType = "sql"
		} else {
			formatType = "csv"
		}
	}

	if outputPath != "" {
		outFile, err := os.Create(outputPath)
		if err != nil {
			return fmt.Errorf("create output file: %w", err)
		}
		defer outFile.Close()
		stdout = outFile
	}

	isRemote := strings.HasPrefix(target, "http://") || strings.HasPrefix(target, "https://")

	if isRemote {
		client, err := ClientFromOpts(target, globalOpts)
		if err != nil {
			return err
		}

		if tableName == "" && formatType == "sql" {
			tablesRes, err := client.Query(ctx, "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name;")
			if err != nil {
				return err
			}
			for _, tRow := range tablesRes.Rows {
				if len(tRow) == 0 {
					continue
				}
				tbl := tRow[0]
				schemaRes, err := client.Query(ctx, fmt.Sprintf("SELECT sql FROM sqlite_master WHERE type='table' AND name='%s';", tbl))
				if err == nil && len(schemaRes.Rows) > 0 && len(schemaRes.Rows[0]) > 0 {
					fmt.Fprintf(stdout, "%s;\n", schemaRes.Rows[0][0])
				}
				dataRes, err := client.Query(ctx, fmt.Sprintf("SELECT * FROM %s;", tbl))
				if err == nil {
					for _, dRow := range dataRes.Rows {
						var escaped []string
						for _, val := range dRow {
							escaped = append(escaped, fmt.Sprintf("'%s'", strings.ReplaceAll(val, "'", "''")))
						}
						fmt.Fprintf(stdout, "INSERT INTO %s VALUES (%s);\n", tbl, strings.Join(escaped, ", "))
					}
				}
				fmt.Fprintln(stdout)
			}
			return nil
		}

		if tableName == "" {
			return fmt.Errorf("--table=NAME required for CSV/JSON export")
		}

		res, err := client.Query(ctx, fmt.Sprintf("SELECT * FROM %s;", tableName))
		if err != nil {
			return err
		}

		if formatType == "json" || globalOpts.JSON {
			var records []map[string]any
			for _, row := range res.Rows {
				rec := make(map[string]any)
				for i, col := range res.Columns {
					if i < len(row) {
						rec[col] = row[i]
					}
				}
				records = append(records, rec)
			}
			return format.RenderJSON(stdout, records, true)
		}

		return format.RenderCSV(stdout, res.Columns, res.Rows)
	}

	// Local database
	db, err := openLocalDB(ctx, target, globalOpts, true)
	if err != nil {
		return fmt.Errorf("open database: %w", err)
	}
	defer db.Close()

	if tableName == "" && formatType == "sql" {
		rows, err := db.QueryContext(ctx, "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name;")
		if err != nil {
			return err
		}
		defer rows.Close()

		var tableList []string
		for rows.Next() {
			var t string
			if err := rows.Scan(&t); err == nil {
				tableList = append(tableList, t)
			}
		}
		rows.Close()

		for _, tbl := range tableList {
			var ddl string
			_ = db.QueryRowContext(ctx, fmt.Sprintf("SELECT sql FROM sqlite_master WHERE type='table' AND name='%s';", tbl)).Scan(&ddl)
			if ddl != "" {
				fmt.Fprintf(stdout, "%s;\n", ddl)
			}

			tRows, err := db.QueryContext(ctx, fmt.Sprintf("SELECT * FROM %s;", tbl))
			if err != nil {
				continue
			}
			cols := tRows.Columns()
			for tRows.Next() {
				vals := make([]any, len(cols))
				valPtrs := make([]any, len(cols))
				for i := range vals {
					valPtrs[i] = &vals[i]
				}
				if err := tRows.Scan(valPtrs...); err != nil {
					continue
				}
				var escaped []string
				for _, v := range vals {
					if v == nil {
						escaped = append(escaped, "NULL")
					} else if b, ok := v.([]byte); ok {
						escaped = append(escaped, fmt.Sprintf("x'%s'", hex.EncodeToString(b)))
					} else {
						escaped = append(escaped, fmt.Sprintf("'%s'", strings.ReplaceAll(fmt.Sprintf("%v", v), "'", "''")))
					}
				}
				fmt.Fprintf(stdout, "INSERT INTO %s VALUES (%s);\n", tbl, strings.Join(escaped, ", "))
			}
			tRows.Close()
			fmt.Fprintln(stdout)
		}
		return nil
	}

	if tableName == "" {
		return fmt.Errorf("--table=NAME required for CSV/JSON export")
	}

	rows, err := db.QueryContext(ctx, fmt.Sprintf("SELECT * FROM %s;", tableName))
	if err != nil {
		return fmt.Errorf("query table %s: %w", tableName, err)
	}
	defer rows.Close()

	cols := rows.Columns()

	var rowData [][]string
	for rows.Next() {
		vals := make([]any, len(cols))
		valPtrs := make([]any, len(cols))
		for i := range vals {
			valPtrs[i] = &vals[i]
		}
		if err := rows.Scan(valPtrs...); err != nil {
			return err
		}
		row := make([]string, len(cols))
		for i, v := range vals {
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

	if formatType == "sql" {
		for _, row := range rowData {
			var escaped []string
			for _, v := range row {
				if v == "NULL" {
					escaped = append(escaped, "NULL")
				} else {
					escaped = append(escaped, fmt.Sprintf("'%s'", strings.ReplaceAll(v, "'", "''")))
				}
			}
			fmt.Fprintf(stdout, "INSERT INTO %s VALUES (%s);\n", tableName, strings.Join(escaped, ", "))
		}
		return nil
	}

	if formatType == "json" || globalOpts.JSON {
		var records []map[string]any
		for _, row := range rowData {
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

	return format.RenderCSV(stdout, cols, rowData)
}
