package state

import (
	"strings"
	"testing"

	"github.com/cockroachdb/pebble/v2"

	"github.com/nomadsql/replicateddb/codec"
	"github.com/nomadsql/replicateddb/ids"
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
		s, err := Open(dir, node, ids.DBID{}, Options{Limits: codec.DefaultLimits()})
		if err != nil {
			t.Fatal(err)
		}
		mutate(s)
		_ = s.Close()
		_, err = Open(dir, node, ids.DBID{}, Options{})
		return err
	}

	if err := tamper(t, func(s *Store) {
		if err := s.db.Set(SysKey(sysMinReader), encodeU64(999), pebble.Sync); err != nil {
			t.Fatal(err)
		}
	}); err == nil || !strings.Contains(err.Error(), "minimum reader") {
		t.Fatalf("minReader=999 open err = %v", err)
	}

	if err := tamper(t, func(s *Store) {
		if err := s.db.Set(SysKey(sysMinWriter), encodeU64(999), pebble.Sync); err != nil {
			t.Fatal(err)
		}
	}); err == nil || !strings.Contains(err.Error(), "minimum writer") {
		t.Fatalf("minWriter=999 open err = %v", err)
	}

	// Legacy pre-marker store: deleting the minima still opens, and the
	// markers are rewritten at the format version.
	dir := t.TempDir()
	node := ids.NewNodeID()
	s, err := Open(dir, node, ids.DBID{}, Options{Limits: codec.DefaultLimits()})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.db.Delete(SysKey(sysMinReader), pebble.Sync); err != nil {
		t.Fatal(err)
	}
	if err := s.db.Delete(SysKey(sysMinWriter), pebble.Sync); err != nil {
		t.Fatal(err)
	}
	_ = s.Close()
	s2, err := Open(dir, node, ids.DBID{}, Options{})
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
