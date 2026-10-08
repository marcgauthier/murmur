package state

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/marcgauthier/murmur/codec"
	"github.com/marcgauthier/murmur/ids"
	"github.com/marcgauthier/murmur/spool"
)

// TestFormatMarkersWrittenFresh proves new stores record the format and
// minimum reader/writer versions.
func TestFormatMarkersWrittenFresh(t *testing.T) {
	s := openTestStore(t, ids.NewNodeID())
	format, minR, minW, err := s.FormatInfo()
	if err != nil {
		t.Fatal(err)
	}
	if format != FormatVersion || minR != MinReaderVersion || minW != MinWriterVersion {
		t.Fatalf("FormatInfo = %d/%d/%d", format, minR, minW)
	}
}

// TestMinReaderWriterGateOpen proves stores demanding a newer reader or
// writer fail closed, while pre-marker v2 stores still open (minima
// defaulting to the format version).
func TestMinReaderWriterGateOpen(t *testing.T) {
	tamper := func(t *testing.T, mutate func(s *Store)) error {
		t.Helper()
		dir := t.TempDir()
		node := ids.NewNodeID()
		s, err := openSignedFixture(dir, node, ids.DBID{}, Options{Limits: codec.DefaultLimits()})
		if err != nil {
			t.Fatal(err)
		}
		mutate(s)
		_ = s.Close()
		_, err = openSignedFixture(dir, node, ids.DBID{}, Options{})
		return err
	}

	if err := tamper(t, func(s *Store) {
		if err := s.dbSet(SysKey(sysMinReader), encodeU64(999), true); err != nil {
			t.Fatal(err)
		}
	}); err == nil || !strings.Contains(err.Error(), "minimum reader") {
		t.Fatalf("minReader=999 open err = %v", err)
	}

	if err := tamper(t, func(s *Store) {
		if err := s.dbSet(SysKey(sysMinWriter), encodeU64(999), true); err != nil {
			t.Fatal(err)
		}
	}); err == nil || !strings.Contains(err.Error(), "minimum writer") {
		t.Fatalf("minWriter=999 open err = %v", err)
	}

	// Legacy pre-marker store: deleting the minima still opens, and the
	// markers are rewritten at the format version.
	dir := t.TempDir()
	node := ids.NewNodeID()
	s, err := openSignedFixture(dir, node, ids.DBID{}, Options{Limits: codec.DefaultLimits()})
	if err != nil {
		t.Fatal(err)
	}
	if err := deleteSync(s, SysKey(sysMinReader)); err != nil {
		t.Fatal(err)
	}
	if err := deleteSync(s, SysKey(sysMinWriter)); err != nil {
		t.Fatal(err)
	}
	_ = s.Close()
	s2, err := openSignedFixture(dir, node, ids.DBID{}, Options{})
	if err != nil {
		t.Fatalf("pre-marker store failed to open: %v", err)
	}
	defer s2.Close()
	format, minR, minW, err := s2.FormatInfo()
	if err != nil {
		t.Fatal(err)
	}
	if format != FormatVersion || minR != FormatVersion || minW != FormatVersion {
		t.Fatalf("reopened FormatInfo = %d/%d/%d", format, minR, minW)
	}
}

// prevReleaseFormat is the persistent format the pinned previous release
// writes and requires with strict equality. Fresh stores must record a
// different version so older binaries refuse them (downgrade guard).
const prevReleaseFormat uint64 = 2

// TestFreshStoreExceedsPreviousRelease pins the downgrade guard: a store
// first written by this binary carries a format the previous release
// rejects, so a downgraded binary fails closed instead of misreading it.
func TestFreshStoreExceedsPreviousRelease(t *testing.T) {
	s := openTestStore(t, ids.NewNodeID())
	format, _, _, err := s.FormatInfo()
	if err != nil {
		t.Fatal(err)
	}
	if format == prevReleaseFormat {
		t.Fatalf("fresh format = %d, want != previous release %d", format, prevReleaseFormat)
	}
	if format != FormatVersion {
		t.Fatalf("fresh format = %d, want FormatVersion %d", format, FormatVersion)
	}
}

func TestFormatFiveStoreFailsClosedWithoutRewrite(t *testing.T) {
	dir := t.TempDir()
	node := ids.NewNodeID()
	s, err := openSignedFixture(dir, node, ids.DBID{}, Options{})
	if err != nil {
		t.Fatal(err)
	}
	for _, marker := range []string{sysFormat, sysMinReader, sysMinWriter} {
		if err := s.dbSet(SysKey(marker), encodeU64(5), true); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := openSignedFixture(dir, node, ids.DBID{}, Options{}); err == nil || !strings.Contains(err.Error(), "removed SQL runtime") {
		t.Fatalf("format-5 open error = %v", err)
	}
}

// TestPreviousReleaseV2StoreRequiresBaselineMigration proves backward compatibility: a store
// carrying previous-release v2 markers opens read-write on this binary
// with its markers left at v2 (no eager upgrade).
func TestPreviousReleaseV2StoreRequiresBaselineMigration(t *testing.T) {
	dir := t.TempDir()
	node := ids.NewNodeID()
	// Build the legacy-marker store with raw Spool, as the previous
	// release would have persisted it (markers, node identity, and the
	// stored cluster identity the migration path expects).
	spopt := spool.DefaultOptions(dir)
	spopt.MasterKey = append([]byte(nil), testMasterKey...)
	spopt.ContextID = append([]byte(nil), fixtureDBID[:]...)
	sp, err := spool.Open(spopt)
	if err != nil {
		t.Fatal(err)
	}
	for _, mk := range []struct {
		name string
		val  uint64
	}{
		{sysFormat, prevReleaseFormat},
		{sysMinReader, prevReleaseFormat},
		{sysMinWriter, prevReleaseFormat},
	} {
		if err := sp.Put(SysKey(mk.name), encodeU64(mk.val)); err != nil {
			t.Fatal(err)
		}
	}
	if err := sp.Put(SysKey(sysLocalNode), node[:]); err != nil {
		t.Fatal(err)
	}
	if err := sp.Put(SysKey(sysDBID), fixtureDBID[:]); err != nil {
		t.Fatal(err)
	}
	if err := sp.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := sp.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := openSignedFixture(dir, node, ids.DBID{}, Options{}); err == nil || !strings.Contains(err.Error(), "MigrateOriginBaseline") {
		t.Fatalf("legacy ordinary open: %v", err)
	}
	s, err := openSignedFixture(dir, node, ids.DBID{}, Options{Limits: codec.DefaultLimits(), MigrateUnsignedBaseline: true})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	format, minR, minW, err := s.FormatInfo()
	if err != nil {
		t.Fatal(err)
	}
	if format != FormatVersion || minR != MinReaderVersion || minW != MinWriterVersion {
		t.Fatalf("v2 FormatInfo = %d/%d/%d, want signed format markers", format, minR, minW)
	}
	ctx := context.Background()
	row := ids.NewRowID()
	if _, err := s.CommitLocal(ctx, localBatch(s, s.ClockNow(),
		codec.Mutation{TableID: 7, RowID: row, ColumnID: 2, Value: codec.Text("v2-write")})); err != nil {
		t.Fatalf("write to v2 store: %v", err)
	}
	st, ok, err := s.GetCell(7, row, 2)
	if err != nil || !ok || st.Value.S != "v2-write" {
		t.Fatalf("v2 store readback: %v %v", st, err)
	}
}

// TestOpenRejectsLegacyPebbleDir proves a directory holding a legacy
// Pebble database fails open without modification, while a bare LOCK
// file (which Spool also uses) is not treated as Pebble evidence.
func TestOpenRejectsLegacyPebbleDir(t *testing.T) {
	node := ids.NewNodeID()
	for _, name := range []string{"CURRENT", "OPTIONS", "MANIFEST-000001", "OPTIONS-000003", "000005.sst", "000008.log"} {
		dir := t.TempDir()
		if err := os.WriteFile(filepath.Join(dir, name), []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
		_, err := openSignedFixture(dir, node, ids.DBID{}, Options{Limits: codec.DefaultLimits()})
		if err == nil || !strings.Contains(err.Error(), "legacy Pebble") {
			t.Fatalf("%s: err = %v, want legacy Pebble rejection", name, err)
		}
	}
	// A lone LOCK file opens normally.
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "LOCK"), []byte("stale"), 0o600); err != nil {
		t.Fatal(err)
	}
	s, err := openSignedFixture(dir, node, ids.DBID{}, Options{Limits: codec.DefaultLimits()})
	if err != nil {
		t.Fatalf("LOCK-only dir: %v", err)
	}
	_ = s.Close()
}
