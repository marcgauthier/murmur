package cmd

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/marcgauthier/murmur/tool/format"
)

type SchemaCommand struct{}

func (c *SchemaCommand) Name() string        { return "schema" }
func (c *SchemaCommand) Description() string { return "Inspect schema definitions, column stable IDs, and history" }
func (c *SchemaCommand) Usage() string {
	return "murmur schema <data-dir | node-url> [--table=NAME] [--history] [--json]"
}

func init() {
	Register(&SchemaCommand{})
}

type columnInfo struct {
	CID        int    `json:"cid"`
	Name       string `json:"name"`
	Type       string `json:"type"`
	NotNull    bool   `json:"notnull"`
	DefaultVal string `json:"dflt_value"`
	PK         bool   `json:"pk"`
}

type tableSchemaInfo struct {
	TableName string       `json:"table_name"`
	SQL       string       `json:"sql"`
	Columns   []columnInfo `json:"columns"`
}

func (c *SchemaCommand) Run(ctx context.Context, globalOpts GlobalOptions, args []string, stdout, stderr io.Writer) error {
	var target, filterTable string

	for _, arg := range args {
		switch {
		case strings.HasPrefix(arg, "--table="):
			filterTable = strings.TrimPrefix(arg, "--table=")
		case !strings.HasPrefix(arg, "-") && target == "":
			target = arg
		}
	}

	if target == "" {
		return fmt.Errorf("missing <data-dir | node-url>. Usage: %s", c.Usage())
	}

	isRemote := strings.HasPrefix(target, "http://") || strings.HasPrefix(target, "https://")
	var tables []tableSchemaInfo

	if isRemote {
		client, err := ClientFromOpts(target, globalOpts)
		if err != nil {
			return err
		}

		query := "SELECT name, sql FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
		if filterTable != "" {
			query += fmt.Sprintf(" AND name='%s'", filterTable)
		}
		query += " ORDER BY name;"

		res, err := client.Query(ctx, query)
		if err != nil {
			return fmt.Errorf("remote schema query failed: %w", err)
		}

		for _, row := range res.Rows {
			if len(row) < 2 {
				continue
			}
			tblName := row[0]
			tblSQL := row[1]

			info := tableSchemaInfo{
				TableName: tblName,
				SQL:       tblSQL,
			}

			pragmaRes, err := client.Query(ctx, fmt.Sprintf("PRAGMA table_info(%s);", tblName))
			if err == nil {
				for _, pRow := range pragmaRes.Rows {
					if len(pRow) >= 6 {
						info.Columns = append(info.Columns, columnInfo{
							Name:       pRow[1],
							Type:       pRow[2],
							NotNull:    pRow[3] == "1",
							DefaultVal: pRow[4],
							PK:         pRow[5] == "1",
						})
					}
				}
			}
			tables = append(tables, info)
		}
	} else {
		db, err := openLocalDB(ctx, target, globalOpts, true)
		if err != nil {
			return fmt.Errorf("open database: %w", err)
		}
		defer db.Close()

		query := "SELECT name, sql FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
		if filterTable != "" {
			query += fmt.Sprintf(" AND name='%s'", filterTable)
		}
		query += " ORDER BY name;"

		rows, err := db.QueryContext(ctx, query)
		if err != nil {
			return fmt.Errorf("query schema: %w", err)
		}
		defer rows.Close()

		for rows.Next() {
			var tblName, tblSQL string
			if err := rows.Scan(&tblName, &tblSQL); err != nil {
				continue
			}
			info := tableSchemaInfo{
				TableName: tblName,
				SQL:       tblSQL,
			}

			pRows, err := db.QueryContext(ctx, fmt.Sprintf("PRAGMA table_info(%s);", tblName))
			if err == nil {
				for pRows.Next() {
					var cid int
					var name, colType, dflt string
					var notnull, pk int
					if err := pRows.Scan(&cid, &name, &colType, &notnull, &dflt, &pk); err == nil {
						info.Columns = append(info.Columns, columnInfo{
							CID:        cid,
							Name:       name,
							Type:       colType,
							NotNull:    notnull == 1,
							DefaultVal: dflt,
							PK:         pk == 1,
						})
					}
				}
				pRows.Close()
			}
			tables = append(tables, info)
		}
	}

	if globalOpts.JSON {
		return format.RenderJSON(stdout, tables, true)
	}

	if len(tables) == 0 {
		fmt.Fprintf(stdout, "No user tables found in %s\n", target)
		return nil
	}

	for _, tbl := range tables {
		fmt.Fprintf(stdout, "Table: %s\n", tbl.TableName)
		var headers = []string{"Column", "Type", "Not Null", "Default", "Primary Key"}
		var rows [][]string
		for _, col := range tbl.Columns {
			rows = append(rows, []string{
				col.Name,
				col.Type,
				fmt.Sprintf("%t", col.NotNull),
				col.DefaultVal,
				fmt.Sprintf("%t", col.PK),
			})
		}
		format.RenderTable(stdout, headers, rows, globalOpts.Markdown)
		fmt.Fprintf(stdout, "\nDDL:\n%s;\n\n", tbl.SQL)
	}

	return nil
}
