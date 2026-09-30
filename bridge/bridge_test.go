package bridge

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/marcgauthier/murmur/codec"
	"github.com/marcgauthier/murmur/ids"
)

type stubDB struct{ id ids.DBID }

func (s stubDB) DBID() ids.DBID { return s.id }

type stubPublisher struct{ got []Artifact }

func (p *stubPublisher) Publish(_ context.Context, a Artifact) error {
	p.got = append(p.got, a)
	return nil
}

type stubSource struct {
	queue []Artifact
	err   error
}

func (s *stubSource) Next(_ context.Context) (Artifact, error) {
	if s.err != nil {
		return Artifact{}, s.err
	}
	if len(s.queue) == 0 {
		return Artifact{}, context.DeadlineExceeded
	}
	a := s.queue[0]
	s.queue = s.queue[1:]
	return a, nil
}

func TestRoleGating(t *testing.T) {
	lowDB, highDB := stubDB{ids.NewDBID()}, stubDB{ids.NewDBID()}
	lowCfg := Config{Role: RoleLowExporter, Domain: lowDB.id, Stream: "s1"}
	highCfg := Config{Role: RoleHighReceiver, Domain: highDB.id}

	if _, err := NewExporter(lowDB, Config{Role: RoleDisabled}, &stubPublisher{}); !errors.Is(err, ErrDisabled) {
		t.Fatalf("disabled exporter err = %v", err)
	}
	if _, err := NewReceiver(highDB, Config{Role: RoleDisabled}, &stubSource{}); !errors.Is(err, ErrDisabled) {
		t.Fatalf("disabled receiver err = %v", err)
	}
	// Cross-role construction is rejected in both directions.
	if _, err := NewExporter(highDB, highCfg, &stubPublisher{}); !errors.Is(err, ErrRoleMismatch) {
		t.Fatalf("exporter-as-high err = %v", err)
	}
	if _, err := NewReceiver(lowDB, lowCfg, &stubSource{}); !errors.Is(err, ErrRoleMismatch) {
		t.Fatalf("receiver-as-low err = %v", err)
	}
	// Invalid configurations fail fast.
	for _, tc := range []struct {
		name string
		cfg  Config
	}{
		{"unknown role", Config{Role: Role(99)}},
		{"exporter without domain", Config{Role: RoleLowExporter, Stream: "s"}},
		{"exporter without stream", Config{Role: RoleLowExporter, Domain: lowDB.id}},
		{"receiver without domain", Config{Role: RoleHighReceiver}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := NewExporter(lowDB, tc.cfg, &stubPublisher{}); err == nil {
				t.Fatal("exporter accepted invalid config")
			}
			if _, err := NewReceiver(highDB, tc.cfg, &stubSource{}); err == nil {
				t.Fatal("receiver accepted invalid config")
			}
		})
	}
	if _, err := NewExporter(nil, lowCfg, &stubPublisher{}); err == nil {
		t.Fatal("nil database accepted")
	}
	if _, err := NewExporter(lowDB, lowCfg, nil); err == nil {
		t.Fatal("nil publisher accepted")
	}
	if _, err := NewReceiver(highDB, highCfg, nil); err == nil {
		t.Fatal("nil source accepted")
	}
}

func TestDomainBinding(t *testing.T) {
	lowDB, highDB := stubDB{ids.NewDBID()}, stubDB{ids.NewDBID()}

	// A Low exporter cannot be wired to the High database and vice versa.
	_, err := NewExporter(highDB, Config{Role: RoleLowExporter, Domain: lowDB.id, Stream: "s"}, &stubPublisher{})
	if !errors.Is(err, ErrDomainMismatch) {
		t.Fatalf("cross-domain exporter err = %v", err)
	}
	_, err = NewReceiver(lowDB, Config{Role: RoleHighReceiver, Domain: highDB.id}, &stubSource{})
	if !errors.Is(err, ErrDomainMismatch) {
		t.Fatalf("cross-domain receiver err = %v", err)
	}

	exp, err := NewExporter(lowDB, Config{Role: RoleLowExporter, Domain: lowDB.id, Stream: "s1"}, &stubPublisher{})
	if err != nil {
		t.Fatal(err)
	}
	if exp.Role() != RoleLowExporter || exp.Domain() != lowDB.id {
		t.Fatal("exporter role/domain mismatch")
	}
	st := exp.Status()
	if st.Role != "low-exporter" || st.Domain != lowDB.id.String() || st.Stream != "s1" {
		t.Fatalf("exporter status = %+v", st)
	}
	rec, err := NewReceiver(highDB, Config{Role: RoleHighReceiver, Domain: highDB.id}, &stubSource{})
	if err != nil {
		t.Fatal(err)
	}
	if rst := rec.Status(); rst.Role != "high-receiver" || rst.Domain != highDB.id.String() {
		t.Fatalf("receiver status = %+v", rst)
	}
	if Role(99).String() != "unknown" || RoleDisabled.String() != "disabled" {
		t.Fatal("role strings")
	}
}

func TestOneWayTransfer(t *testing.T) {
	ctx := context.Background()
	lowDB := stubDB{ids.NewDBID()}
	out := &stubPublisher{}
	exp, err := NewExporter(lowDB, Config{Role: RoleLowExporter, Domain: lowDB.id, Stream: "s"}, out)
	if err != nil {
		t.Fatal(err)
	}
	a := Artifact{Name: "bundle-000001.dat", Data: []byte("opaque-bytes")}
	if err := exp.PublishArtifact(ctx, a); err != nil {
		t.Fatal(err)
	}
	if len(out.got) != 1 || out.got[0].Name != a.Name {
		t.Fatalf("published = %v", out.got)
	}

	highDB := stubDB{ids.NewDBID()}
	src := &stubSource{queue: []Artifact{a}}
	rec, err := NewReceiver(highDB, Config{Role: RoleHighReceiver, Domain: highDB.id}, src)
	if err != nil {
		t.Fatal(err)
	}
	got, err := rec.NextArtifact(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got.Name != a.Name || string(got.Data) != string(a.Data) {
		t.Fatalf("received = %+v", got)
	}

	// Malicious names and oversized payloads are rejected on both ends.
	bads := []Artifact{
		{Name: "", Data: []byte("x")},
		{Name: "../escape", Data: []byte("x")},
		{Name: "sub/dir", Data: []byte("x")},
		{Name: `back\slash`, Data: []byte("x")},
		{Name: ".hidden", Data: []byte("x")},
		{Name: "has space", Data: []byte("x")},
		{Name: strings.Repeat("a", 257), Data: []byte("x")},
		{Name: "empty.dat"},
		{Name: "big.dat", Data: make([]byte, exp.Limits().MaxBundleBytes+1)},
	}
	for _, bad := range bads {
		if err := exp.PublishArtifact(ctx, bad); err == nil {
			t.Fatalf("publish accepted %q", bad.Name)
		}
		src.queue = []Artifact{bad}
		if _, err := rec.NextArtifact(ctx); err == nil {
			t.Fatalf("receive accepted %q", bad.Name)
		}
	}
}

func TestBatchValidate(t *testing.T) {
	limits := Limits{}.withDefaults()
	put := Record{Table: "t", Row: ids.NewRowID(), Op: RecordPut,
		Columns: []ColumnValue{{Column: "c", Value: codec.Text("v")}}}
	del := Record{Table: "t", Row: ids.NewRowID(), Op: RecordDelete}
	good := Batch{TxID: ids.NewTxID(), Origin: ids.NewNodeID(), Sequence: 7, HLC: 9,
		Records: []Record{put, del}}
	if err := good.Validate(limits); err != nil {
		t.Fatalf("valid batch: %v", err)
	}

	cases := []struct {
		name   string
		mutate func(*Batch)
	}{
		{"zero tx", func(b *Batch) { b.TxID = ids.TxID{} }},
		{"zero origin", func(b *Batch) { b.Origin = ids.NodeID{} }},
		{"no records", func(b *Batch) { b.Records = nil }},
		{"no table", func(b *Batch) { b.Records[0].Table = "" }},
		{"zero row", func(b *Batch) { b.Records[0].Row = ids.RowID{} }},
		{"put without columns", func(b *Batch) { b.Records[0].Columns = nil }},
		{"unnamed column", func(b *Batch) { b.Records[0].Columns[0].Column = "" }},
		{"delete with columns", func(b *Batch) {
			b.Records[1].Columns = []ColumnValue{{Column: "c"}}
		}},
		{"unknown op", func(b *Batch) { b.Records[0].Op = RecordOp(99) }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b := good
			b.Records = append([]Record(nil), good.Records...)
			b.Records[0].Columns = append([]ColumnValue(nil), good.Records[0].Columns...)
			tc.mutate(&b)
			if err := b.Validate(limits); err == nil {
				t.Fatal("invalid batch accepted")
			}
		})
	}
	tight := Limits{MaxBundleBytes: 1 << 20, MaxPayloadBytes: 1 << 20,
		MaxTransactions: 1, MaxStagingBytes: 1 << 20, MaxQueue: 1}
	if err := good.Validate(tight); err == nil {
		t.Fatal("over-limit batch accepted")
	}
	if d := (Limits{}).withDefaults(); d.MaxBundleBytes <= 0 || d.MaxPayloadBytes <= 0 ||
		d.MaxTransactions <= 0 || d.MaxStagingBytes <= 0 || d.MaxStagingEntries <= 0 || d.MaxQueue <= 0 {
		t.Fatalf("bad defaults: %+v", d)
	}
}
