package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/marcgauthier/murmur"
	"github.com/marcgauthier/murmur/schema"
)

// handleDDLOrExec executes DDL via db.Migrate if it is a CREATE TABLE statement, or ExecContext otherwise.
func handleDDLOrExec(ctx context.Context, db *murmur.DB, stmt string) (int64, error) {
	stmtTrim := strings.TrimSpace(stmt)
	upper := strings.ToUpper(stmtTrim)

	if strings.HasPrefix(upper, "CREATE TABLE") {
		tbl, err := parseCreateTableDDL(stmtTrim)
		if err != nil {
			return 0, err
		}

		currentTables, err := db.SchemaTables()
		if err != nil {
			return 0, fmt.Errorf("load current tables: %w", err)
		}

		// Check if table already exists
		found := false
		for i, t := range currentTables {
			if strings.EqualFold(t.Name, tbl.Name) {
				currentTables[i] = tbl
				found = true
				break
			}
		}
		if !found {
			currentTables = append(currentTables, tbl)
		}

		if err := db.Migrate(ctx, currentTables); err != nil {
			return 0, fmt.Errorf("schema migration failed: %w", err)
		}

		// Update schema.json on disk
		epoch, _, _ := db.SchemaInfo()
		schemaFile := filepath.Join(filepath.Dir(db.DataDir()), "schema.json")
		data, _ := json.MarshalIndent(murmur.SchemaConfig{
			Version: epoch,
			Tables:  currentTables,
		}, "", "  ")
		_ = os.WriteFile(schemaFile, data, 0o600)

		return 0, nil
	}

	res, err := db.ExecContext(ctx, stmt)
	if err != nil {
		return 0, err
	}
	affected, _ := res.RowsAffected()
	return affected, nil
}

func parseCreateTableDDL(ddl string) (schema.TableSchema, error) {
	clean := strings.TrimSpace(ddl)
	clean = strings.TrimSuffix(clean, ";")

	openIdx := strings.Index(clean, "(")
	closeIdx := strings.LastIndex(clean, ")")
	if openIdx == -1 || closeIdx == -1 || closeIdx <= openIdx {
		return schema.TableSchema{}, fmt.Errorf("invalid CREATE TABLE statement syntax: missing parentheses")
	}

	head := strings.TrimSpace(clean[:openIdx])
	body := strings.TrimSpace(clean[openIdx+1 : closeIdx])

	headWords := strings.Fields(head)
	if len(headWords) < 3 || strings.ToUpper(headWords[0]) != "CREATE" || strings.ToUpper(headWords[1]) != "TABLE" {
		return schema.TableSchema{}, fmt.Errorf("invalid CREATE TABLE prefix")
	}

	tableName := headWords[len(headWords)-1]
	tableName = strings.Trim(tableName, "\"`'[]")

	// Parse column definitions
	colDefs := splitColDefs(body)
	var columns []schema.ColumnSchema

	for _, colDef := range colDefs {
		colDef = strings.TrimSpace(colDef)
		if colDef == "" {
			continue
		}

		// Ignore standalone PRIMARY KEY (id) constraints for now
		if strings.HasPrefix(strings.ToUpper(colDef), "PRIMARY KEY") || strings.HasPrefix(strings.ToUpper(colDef), "CONSTRAINT") {
			continue
		}

		words := strings.Fields(colDef)
		if len(words) < 1 {
			continue
		}

		colName := strings.Trim(words[0], "\"`'[]")
		colTypeStr := "TEXT"
		if len(words) > 1 {
			colTypeStr = strings.ToUpper(words[1])
		}

		isPK := strings.Contains(strings.ToUpper(colDef), "PRIMARY KEY") || len(columns) == 0 && strings.EqualFold(colName, "id")
		nullable := !strings.Contains(strings.ToUpper(colDef), "NOT NULL")
		if isPK {
			nullable = false
		}

		colType, err := schema.ParseColumnType(colTypeStr)
		if err != nil {
			colType = schema.ColText
		}
		if isPK {
			colType = schema.ColBlob
		}

		columns = append(columns, schema.ColumnSchema{
			Name:     colName,
			Type:     colType,
			Nullable: nullable,
		})
	}

	if len(columns) == 0 {
		return schema.TableSchema{}, fmt.Errorf("table %s must define at least one column", tableName)
	}

	return schema.TableSchema{
		Name:    tableName,
		Columns: columns,
	}, nil
}

func splitColDefs(body string) []string {
	var parts []string
	var buf strings.Builder
	parenDepth := 0

	for _, r := range body {
		if r == '(' {
			parenDepth++
		} else if r == ')' {
			parenDepth--
		}

		if r == ',' && parenDepth == 0 {
			if s := strings.TrimSpace(buf.String()); s != "" {
				parts = append(parts, s)
			}
			buf.Reset()
			continue
		}

		buf.WriteRune(r)
	}

	if s := strings.TrimSpace(buf.String()); s != "" {
		parts = append(parts, s)
	}
	return parts
}
