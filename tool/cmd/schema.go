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
func (c *SchemaCommand) Description() string { return "Inspect table definitions and stable IDs" }
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
	TableID   uint32       `json:"table_id"`
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
		manifestTables, err := client.SchemaTables(ctx)
		if err != nil {
			return fmt.Errorf("remote schema request failed: %w", err)
		}
		for _, table := range manifestTables {
			if filterTable != "" && !strings.EqualFold(table.Name, filterTable) {
				continue
			}
			info := tableSchemaInfo{
				TableName: table.Name,
				TableID:   table.ID,
			}
			for i, column := range table.Columns {
				info.Columns = append(info.Columns, columnInfo{
					CID:     i,
					Name:    column.Name,
					Type:    column.Type.String(),
					NotNull: !column.Nullable,
					PK:      column.ID == table.PK,
				})
			}
			tables = append(tables, info)
		}
	} else {
		if err := ctx.Err(); err != nil {
			return err
		}
		store, err := openOfflineStore(target, globalOpts)
		if err != nil {
			return fmt.Errorf("open durable state: %w", err)
		}
		defer store.Close()

		manifest, err := store.LoadSchemaManifest()
		if err != nil {
			return fmt.Errorf("read schema manifest: %w", err)
		}
		if manifest != nil {
			for _, table := range manifest.Tables {
				if filterTable != "" && !strings.EqualFold(table.Name, filterTable) {
					continue
				}
				info := tableSchemaInfo{
					TableName: table.Name,
					TableID:   table.ID,
				}
				for i, column := range table.Columns {
					info.Columns = append(info.Columns, columnInfo{
						CID:     i,
						Name:    column.Name,
						Type:    column.Type.String(),
						NotNull: !column.Nullable,
						PK:      column.ID == table.PK,
					})
				}
				tables = append(tables, info)
			}
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
		fmt.Fprintf(stdout, "Table: %s (ID %d)\n", tbl.TableName, tbl.TableID)
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
		fmt.Fprintln(stdout)
	}

	return nil
}
