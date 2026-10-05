package cmd

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/marcgauthier/murmur/ids"
	"github.com/marcgauthier/murmur/tool/format"
)

type ImportCommand struct{}

func (c *ImportCommand) Name() string        { return "import" }
func (c *ImportCommand) Description() string { return "Bulk import CSV or JSON records into a table" }
func (c *ImportCommand) Usage() string {
	return "murmur import <data-dir | node-url> --table=NAME <csv-file> [--delimiter=,] [--skip-header=true] [--batch=500]"
}

func init() {
	Register(&ImportCommand{})
}

func (c *ImportCommand) Run(ctx context.Context, globalOpts GlobalOptions, args []string, stdout, stderr io.Writer) error {
	var target, tableName, filePath string
	delimiter := ','
	skipHeader := true
	batchSize := 500

	for _, arg := range args {
		switch {
		case strings.HasPrefix(arg, "--table="):
			tableName = strings.TrimPrefix(arg, "--table=")
		case strings.HasPrefix(arg, "--delimiter="):
			dStr := strings.TrimPrefix(arg, "--delimiter=")
			if len(dStr) > 0 {
				delimiter = rune(dStr[0])
			}
		case strings.HasPrefix(arg, "--skip-header="):
			skipHeader = strings.ToLower(strings.TrimPrefix(arg, "--skip-header=")) != "false"
		case strings.HasPrefix(arg, "--batch="):
			if b, err := strconv.Atoi(strings.TrimPrefix(arg, "--batch=")); err == nil && b > 0 {
				batchSize = b
			}
		case !strings.HasPrefix(arg, "-"):
			if target == "" {
				target = arg
			} else if tableName == "" && filePath == "" {
				tableName = arg
			} else if filePath == "" {
				filePath = arg
			}
		}
	}

	if target == "" || tableName == "" || filePath == "" {
		return fmt.Errorf("missing required arguments. Usage: %s", c.Usage())
	}

	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("open import file: %w", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	reader.Comma = delimiter
	reader.LazyQuotes = true

	headers, err := reader.Read()
	if err != nil {
		return fmt.Errorf("read header: %w", err)
	}

	var rows [][]string
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("read CSV record: %w", err)
		}
		rows = append(rows, record)
	}

	if !skipHeader {
		rows = append([][]string{headers}, rows...)
	}

	totalRows := len(rows)
	if totalRows == 0 {
		fmt.Fprintf(stdout, "No rows to import.\n")
		return nil
	}

	start := time.Now()

	// Check if target is remote node
	if strings.HasPrefix(target, "http://") || strings.HasPrefix(target, "https://") {
		client, err := ClientFromOpts(target, globalOpts)
		if err != nil {
			return err
		}

		imported := 0
		for i := 0; i < totalRows; i += batchSize {
			end := i + batchSize
			if end > totalRows {
				end = totalRows
			}
			chunk := rows[i:end]

			var batchSQL strings.Builder
			for _, row := range chunk {
				batchSQL.WriteString(fmt.Sprintf("INSERT INTO %s VALUES (", tableName))
				for colIdx, val := range row {
					if colIdx > 0 {
						batchSQL.WriteString(", ")
					}
					batchSQL.WriteString(fmt.Sprintf("'%s'", strings.ReplaceAll(val, "'", "''")))
				}
				batchSQL.WriteString(");\n")
			}

			if _, err := client.Exec(ctx, batchSQL.String()); err != nil {
				return fmt.Errorf("remote batch import failed at row %d: %w", i, err)
			}
			imported += len(chunk)
		}

		elapsed := time.Since(start)
		if globalOpts.JSON {
			return format.RenderJSON(stdout, map[string]any{
				"imported": totalRows,
				"table":    tableName,
				"elapsed":  elapsed.String(),
			}, true)
		}
		fmt.Fprintf(stdout, "Successfully imported %d rows into %s in %s\n", totalRows, tableName, elapsed)
		return nil
	}

	// Local database execution
	db, err := openLocalDB(ctx, target, globalOpts, false)
	if err != nil {
		return fmt.Errorf("open database: %w", err)
	}
	defer db.Close()

	for i := 0; i < totalRows; i += batchSize {
		end := i + batchSize
		if end > totalRows {
			end = totalRows
		}
		chunk := rows[i:end]

		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return fmt.Errorf("begin transaction: %w", err)
		}

		for _, row := range chunk {
			var placeholders []string
			var args []any
			for colIdx, v := range row {
				placeholders = append(placeholders, "?")
				if colIdx == 0 {
					if u, err := ids.ParseNodeID(v); err == nil {
						args = append(args, u[:])
					} else if len(v) == 16 {
						args = append(args, []byte(v))
					} else {
						var pk [16]byte
						copy(pk[:], []byte(v))
						args = append(args, pk[:])
					}
				} else {
					args = append(args, v)
				}
			}
			query := fmt.Sprintf("INSERT INTO %s VALUES (%s)", tableName, strings.Join(placeholders, ", "))
			if _, err := tx.ExecContext(ctx, query, args...); err != nil {
				_ = tx.Rollback()
				return fmt.Errorf("insert row error: %w", err)
			}
		}

		if err := tx.Commit(); err != nil {
			return fmt.Errorf("commit batch transaction: %w", err)
		}
	}

	elapsed := time.Since(start)
	if globalOpts.JSON {
		return format.RenderJSON(stdout, map[string]any{
			"imported": totalRows,
			"table":    tableName,
			"elapsed":  elapsed.String(),
		}, true)
	}
	fmt.Fprintf(stdout, "Successfully imported %d rows into %s in %s\n", totalRows, tableName, elapsed)
	return nil
}
