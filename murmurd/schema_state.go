package murmurd

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"

	db "github.com/marcgauthier/spedsql"
	murmurSchema "github.com/marcgauthier/spedsql/schema"
)

// liveSchemaFile is the daemon-owned sidecar next to node.json. It stores
// the last live schema declaration (epoch + resolved tables) exported
// from the engine. Murmur reopens require the configuration to match the
// stored manifest exactly, and the stored schema advances on every
// CREATE TABLE and on every replicated schema adoption, so the daemon
// reopens from this sidecar — never from config.toml alone — and
// refreshes it after each local migration, periodically while running,
// and before shutdown.
const liveSchemaFile = "schema.json"

// liveSchemaState is the sidecar content. Tables carry resolved IDs so
// reopening reproduces the stored registry exactly.
type liveSchemaState struct {
	Version uint64                     `json:"version"`
	Tables  []murmurSchema.TableSchema `json:"tables"`
}

func liveSchemaPath(dataDir string) string {
	return filepath.Join(dataDir, liveSchemaFile)
}

// loadLiveSchema reads the sidecar. It returns (nil, nil) when the data
// dir is fresh (no sidecar yet).
func loadLiveSchema(dataDir string) (*liveSchemaState, error) {
	raw, err := os.ReadFile(liveSchemaPath(dataDir))
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("murmurd: read %s: %w", liveSchemaFile, err)
	}
	var st liveSchemaState
	if err := json.Unmarshal(raw, &st); err != nil {
		return nil, fmt.Errorf("murmurd: parse %s: %w (delete it only if data_dir is disposable, else re-declare tables in config)", liveSchemaFile, err)
	}
	if len(st.Tables) == 0 {
		return nil, fmt.Errorf("murmurd: %s declares no tables (delete it only if data_dir is disposable, else re-declare tables in config)", liveSchemaFile)
	}
	return &st, nil
}

// persistLiveSchema exports the engine's live declaration and writes the
// sidecar atomically. It returns the persisted epoch.
func persistLiveSchema(database *db.DB, dataDir string) (uint64, error) {
	epoch, tables, err := database.LiveSchema()
	if err != nil {
		return 0, fmt.Errorf("murmurd: export live schema: %w", err)
	}
	st := liveSchemaState{Version: epoch, Tables: tables}
	out, err := json.MarshalIndent(st, "", "  ")
	if err != nil {
		return 0, fmt.Errorf("murmurd: encode %s: %w", liveSchemaFile, err)
	}
	path := liveSchemaPath(dataDir)
	tmp, err := os.CreateTemp(dataDir, liveSchemaFile+".tmp-*")
	if err != nil {
		return 0, fmt.Errorf("murmurd: write %s: %w", liveSchemaFile, err)
	}
	tmpName := tmp.Name()
	if _, err := tmp.Write(append(out, '\n')); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return 0, fmt.Errorf("murmurd: write %s: %w", liveSchemaFile, err)
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpName)
		return 0, fmt.Errorf("murmurd: write %s: %w", liveSchemaFile, err)
	}
	if err := os.Rename(tmpName, path); err != nil {
		os.Remove(tmpName)
		return 0, fmt.Errorf("murmurd: write %s: %w", liveSchemaFile, err)
	}
	return epoch, nil
}

// reconcileDesiredTables treats schema-file tables as desired additive
// state: tables/columns present in config but missing from the live
// schema migrate in; tables the config no longer mentions stay (murmur
// schemas are additive-only); conflicting redefinitions fail loudly.
func reconcileDesiredTables(ctx context.Context, database *db.DB, desired []murmurSchema.TableSchema, logger *slog.Logger) error {
	if len(desired) == 0 {
		return nil
	}
	// Resolve file IDs the same deterministic way the engine does so
	// the union compares resolved against resolved.
	resolved, err := murmurSchema.BuildRegistry(0, desired)
	if err != nil {
		return fmt.Errorf("murmurd: schema file: %w", err)
	}
	want := make([]murmurSchema.TableSchema, 0, len(resolved.Tables))
	for _, t := range resolved.Tables {
		cp := *t
		cp.Columns = append([]murmurSchema.ColumnSchema(nil), t.Columns...)
		want = append(want, cp)
	}
	epoch, live, err := database.LiveSchema()
	if err != nil {
		return err
	}
	next, err := murmurSchema.UnionTables(live, want)
	if err != nil {
		return fmt.Errorf("murmurd: schema file conflicts with live schema (epoch %d): %w", epoch, err)
	}
	if sameDeclaration(live, next) {
		return nil
	}
	if err := database.Migrate(ctx, next); err != nil {
		return fmt.Errorf("murmurd: migrate schema-file tables: %w", err)
	}
	logger.Info("murmurd: migrated schema-file tables into live schema", "epoch", epoch+1)
	return nil
}

// sameDeclaration reports whether two declarations carry the same tables
// and columns. The union never drops, so count equality means no change.
func sameDeclaration(a, b []murmurSchema.TableSchema) bool {
	if len(a) != len(b) {
		return false
	}
	cols := func(ts []murmurSchema.TableSchema) int {
		n := 0
		for _, t := range ts {
			n += len(t.Columns)
		}
		return n
	}
	return cols(a) == cols(b)
}
