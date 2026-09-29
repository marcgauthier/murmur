package bridge

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/marcgauthier/spedsql/codec"
	"github.com/marcgauthier/spedsql/ids"
)

func appendBatch(t *testing.T, o *Outbox, origin ids.NodeID, originSeq uint64) uint64 {
	t.Helper()
	seq, err := o.Append(putBatch(originSeq, ids.NewRowID(),
		ColumnValue{Column: "name", Value: codec.Text("s3cr3t-name")}), origin, originSeq, 1, [32]byte{0x1})
	if err != nil {
		t.Fatal(err)
	}
	return seq
}

func TestExportStatusBounded(t *testing.T) {
	o, err := OpenOutbox(t.TempDir(), Limits{}.withDefaults())
	if err != nil {
		t.Fatal(err)
	}
	origin := ids.NewNodeID()
	if got := o.ExportStatus(StatusOptions{}); got.HasBacklog || got.PendingTotal != 0 || got.FailedTotal != 0 {
		t.Fatalf("empty outbox = %+v", got)
	}
	p1, p2, p3 := appendBatch(t, o, origin, 1), appendBatch(t, o, origin, 2), appendBatch(t, o, origin, 3)
	f1, f2 := appendBatch(t, o, origin, 4), appendBatch(t, o, origin, 5)
	pub1, pub2 := appendBatch(t, o, origin, 6), appendBatch(t, o, origin, 7)
	_, _, _ = p1, p2, p3
	if err := o.MarkFailed(f1, errors.New("dial refused")); err != nil {
		t.Fatal(err)
	}
	if err := o.MarkFailed(f2, errors.New("timeout")); err != nil {
		t.Fatal(err)
	}
	if err := o.MarkPublished(pub1); err != nil {
		t.Fatal(err)
	}
	if err := o.MarkPublished(pub2); err != nil {
		t.Fatal(err)
	}
	got := o.ExportStatus(StatusOptions{MaxEntries: 2})
	if len(got.Pending) != 2 || got.PendingTotal != 3 {
		t.Fatalf("pending = %+v total %d", got.Pending, got.PendingTotal)
	}
	if got.Pending[0].Seq != p1 || got.Pending[1].Seq != p2 {
		t.Fatalf("pending order = %+v", got.Pending)
	}
	if len(got.Failed) != 2 || got.FailedTotal != 2 {
		t.Fatalf("failed = %+v total %d", got.Failed, got.FailedTotal)
	}
	if got.Failed[0].LastError != "dial refused" || got.Failed[0].Attempts != 1 {
		t.Fatalf("failed[0] = %+v", got.Failed[0])
	}
	if got.PublishedSeq != pub2 {
		t.Fatalf("published = %d", got.PublishedSeq)
	}
	if !got.HasBacklog || got.OldestBacklogAt.IsZero() {
		t.Fatalf("backlog = %+v", got)
	}
	if age := time.Since(got.OldestBacklogAt); age < 0 || age > time.Minute {
		t.Fatalf("backlog age = %v", age)
	}
}

func TestMissingBoundedFarAhead(t *testing.T) {
	signer, recip, trust := inboxKeys(t, "s")
	inbox, err := OpenInbox(t.TempDir(), trust, Limits{}.withDefaults())
	if err != nil {
		t.Fatal(err)
	}
	// A bundle staged a trillion sequences ahead must not hang gap listing.
	const far = uint64(1000000000000)
	a := sealForInbox(t, signer, recip, "s", far, []Batch{
		putBatch(far, ids.NewRowID(), ColumnValue{Column: "name", Value: codec.Text("ann")}),
	})
	if err := inbox.Receive(a); err != nil {
		t.Fatal(err)
	}
	done := make(chan []StreamProgress, 1)
	go func() { done <- inbox.Progress() }()
	select {
	case prog := <-done:
		if len(prog) != 1 {
			t.Fatalf("progress = %+v", prog)
		}
		sp := prog[0]
		if len(sp.Missing) != inboxMissingEnumCap || sp.Missing[0] != 1 {
			t.Fatalf("missing prefix len = %d first = %d", len(sp.Missing), sp.Missing[0])
		}
		if sp.MissingTotal != far-1 { // range [1..far] minus the staged bundle
			t.Fatalf("missing total = %d", sp.MissingTotal)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("Progress hung on a far-ahead bundle")
	}
	prog := inbox.ProgressBounded(2)
	if len(prog[0].Missing) != 2 || prog[0].MissingTotal != far-1 {
		t.Fatalf("bounded = %+v", prog[0].Missing)
	}
	if len(prog[0].Staged) != 1 || prog[0].StagedTotal != 1 {
		t.Fatalf("staged = %+v", prog[0].Staged)
	}
}

func TestProgressTotals(t *testing.T) {
	signer, recip, trust := inboxKeys(t, "s")
	inbox, err := OpenInbox(t.TempDir(), trust, Limits{}.withDefaults())
	if err != nil {
		t.Fatal(err)
	}
	a1 := sealForInbox(t, signer, recip, "s", 1, []Batch{
		putBatch(1, ids.NewRowID(), ColumnValue{Column: "name", Value: codec.Text("ann")}),
	})
	a3 := sealForInbox(t, signer, recip, "s", 3, []Batch{
		putBatch(3, ids.NewRowID(), ColumnValue{Column: "name", Value: codec.Text("bob")}),
	})
	// Conflicting content under seq 1: quarantined, original stays staged.
	a1conflict := sealForInbox(t, signer, recip, "s", 1, []Batch{
		putBatch(1, ids.NewRowID(), ColumnValue{Column: "name", Value: codec.Text("mallory")}),
	})
	if err := inbox.Receive(a1); err != nil {
		t.Fatal(err)
	}
	if err := inbox.Receive(a3); err != nil {
		t.Fatal(err)
	}
	if err := inbox.Receive(a1conflict); !errors.Is(err, ErrQuarantined) {
		t.Fatalf("conflict err = %v", err)
	}
	prog := inbox.Progress()
	if len(prog) != 1 {
		t.Fatalf("progress = %+v", prog)
	}
	sp := prog[0]
	if sp.StagedTotal != 2 || len(sp.Staged) != 2 {
		t.Fatalf("staged = %+v total %d", sp.Staged, sp.StagedTotal)
	}
	// Range [1..3]: seqs 1 and 3 staged (the conflict never un-stages the
	// original), so only seq 2 is missing.
	if len(sp.Missing) != 1 || sp.Missing[0] != 2 || sp.MissingTotal != 1 {
		t.Fatalf("missing = %v total %d", sp.Missing, sp.MissingTotal)
	}
	if sp.QuarantineTotal != 1 || len(sp.Quarantine) != 1 {
		t.Fatalf("quarantine = %+v", sp.Quarantine)
	}
	bounded := inbox.ProgressBounded(1)
	if len(bounded[0].Staged) != 1 || bounded[0].StagedTotal != 2 {
		t.Fatalf("bounded staged = %+v", bounded[0].Staged)
	}
}

func TestKeyStatusNoPrivateMaterial(t *testing.T) {
	signer, recip, trust := inboxKeys(t, "s")
	ks := trust.KeyStatus()
	if len(ks.Signers) != 1 || ks.Signers[0].ID != hex.EncodeToString(signer.ID[:]) {
		t.Fatalf("signers = %+v", ks.Signers)
	}
	if len(ks.Signers[0].Streams) != 1 || ks.Signers[0].Streams[0] != "s" {
		t.Fatalf("streams = %+v", ks.Signers[0].Streams)
	}
	if len(ks.Recipients) != 1 || ks.Recipients[0] != hex.EncodeToString(recip.ID[:]) {
		t.Fatalf("recipients = %+v", ks.Recipients)
	}
	raw, err := json.Marshal(ks)
	if err != nil {
		t.Fatal(err)
	}
	js := string(raw)
	// The signing seed and the recipient private key must never appear;
	// public IDs (which equal the raw public keys) are the identities.
	if strings.Contains(js, hex.EncodeToString(signer.priv.Seed())) {
		t.Fatal("signing seed leaked into key status")
	}
	if strings.Contains(js, hex.EncodeToString(recip.priv[:])) {
		t.Fatal("recipient private key leaked into key status")
	}
}

func TestReplayStatusAndRetryAll(t *testing.T) {
	signer, recip, trust := inboxKeys(t, "s")
	inbox, err := OpenInbox(t.TempDir(), trust, Limits{}.withDefaults())
	if err != nil {
		t.Fatal(err)
	}
	good := sealForInbox(t, signer, recip, "s", 1, []Batch{
		putBatch(1, ids.NewRowID(), ColumnValue{Column: "name", Value: codec.Text("ann")}),
	})
	clash := sealForInbox(t, signer, recip, "s", 3, []Batch{
		putBatch(3, ids.NewRowID(), ColumnValue{Column: "name", Value: codec.Text("bob")}),
	})
	clashConflict := sealForInbox(t, signer, recip, "s", 3, []Batch{
		putBatch(3, ids.NewRowID(), ColumnValue{Column: "name", Value: codec.Text("mallory")}),
	})
	if err := inbox.Receive(good); err != nil {
		t.Fatal(err)
	}
	if err := inbox.Receive(clash); err != nil {
		t.Fatal(err)
	}
	if err := inbox.Receive(clashConflict); !errors.Is(err, ErrQuarantined) {
		t.Fatalf("conflict err = %v", err)
	}
	if err := inbox.Quarantine("s", 1, "boom"); err != nil {
		t.Fatal(err)
	}
	rs, err := inbox.ReplayStatus(0)
	if err != nil {
		t.Fatal(err)
	}
	if rs.Total != 2 || len(rs.Items) != 2 {
		t.Fatalf("replay = %+v", rs)
	}
	if !rs.Items[0].Retryable || rs.Items[0].First != 1 || rs.Items[0].Reason != "boom" {
		t.Fatalf("items[0] = %+v", rs.Items[0])
	}
	if rs.Items[1].Retryable || rs.Items[1].First != 3 {
		t.Fatalf("items[1] = %+v", rs.Items[1])
	}
	results := inbox.RetryAllQuarantined()
	if len(results) != 2 {
		t.Fatalf("results = %+v", results)
	}
	if !results[0].Retried || results[0].Err != "" {
		t.Fatalf("results[0] = %+v", results[0])
	}
	if results[1].Retried || results[1].Err == "" {
		t.Fatalf("results[1] = %+v", results[1])
	}
	// The failure hold re-staged; the terminal conflict is untouched.
	prog := inbox.Progress()
	if prog[0].QuarantineTotal != 1 || len(prog[0].Staged) != 2 {
		t.Fatalf("progress = %+v", prog)
	}
}

func TestImportStatusBacklogAge(t *testing.T) {
	signer, recip, trust := inboxKeys(t, "s")
	dir := t.TempDir()
	inbox, err := OpenInbox(dir, trust, Limits{}.withDefaults())
	if err != nil {
		t.Fatal(err)
	}
	if got := inbox.ImportStatus(StatusOptions{}); got.HasBacklog {
		t.Fatalf("empty inbox = %+v", got)
	}
	a := sealForInbox(t, signer, recip, "s", 1, []Batch{
		putBatch(1, ids.NewRowID(), ColumnValue{Column: "name", Value: codec.Text("ann")}),
	})
	if err := inbox.Receive(a); err != nil {
		t.Fatal(err)
	}
	files, err := filepath.Glob(filepath.Join(dir, "staged", "*.spb"))
	if err != nil || len(files) != 1 {
		t.Fatalf("staged files = %v, %v", files, err)
	}
	old := time.Now().Add(-2 * time.Hour)
	if err := os.Chtimes(files[0], old, old); err != nil {
		t.Fatal(err)
	}
	got := inbox.ImportStatus(StatusOptions{})
	if !got.HasBacklog || got.OldestBacklogAt.IsZero() {
		t.Fatalf("backlog = %+v", got)
	}
	if age := time.Since(got.OldestBacklogAt); age < 2*time.Hour || age > 2*time.Hour+time.Minute {
		t.Fatalf("backlog age = %v", age)
	}
}

func TestStatusCarriesNoPayloads(t *testing.T) {
	signer, recip, trust := inboxKeys(t, "s")
	o, err := OpenOutbox(t.TempDir(), Limits{}.withDefaults())
	if err != nil {
		t.Fatal(err)
	}
	appendBatch(t, o, ids.NewNodeID(), 1)
	inbox, err := OpenInbox(t.TempDir(), trust, Limits{}.withDefaults())
	if err != nil {
		t.Fatal(err)
	}
	a := sealForInbox(t, signer, recip, "s", 1, []Batch{
		putBatch(1, ids.NewRowID(), ColumnValue{Column: "name", Value: codec.Text("s3cr3t-name")}),
	})
	if err := inbox.Receive(a); err != nil {
		t.Fatal(err)
	}
	lowDB := stubDB{ids.NewDBID()}
	exp, err := NewExporter(lowDB, Config{Role: RoleLowExporter, Domain: lowDB.id, Stream: "s"}, &stubPublisher{})
	if err != nil {
		t.Fatal(err)
	}
	st := exp.Describe(o, inbox, trust, StatusOptions{})
	raw, err := json.Marshal(st)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "s3cr3t-name") {
		t.Fatal("record payload leaked into diagnostics")
	}
}

func TestDescribeComposes(t *testing.T) {
	signer, recip, trust := inboxKeys(t, "s")
	o, err := OpenOutbox(t.TempDir(), Limits{}.withDefaults())
	if err != nil {
		t.Fatal(err)
	}
	origin := ids.NewNodeID()
	pub := appendBatch(t, o, origin, 1)
	appendBatch(t, o, origin, 2)
	if err := o.MarkPublished(pub); err != nil {
		t.Fatal(err)
	}
	inbox, err := OpenInbox(t.TempDir(), trust, Limits{}.withDefaults())
	if err != nil {
		t.Fatal(err)
	}
	a := sealForInbox(t, signer, recip, "s", 2, []Batch{
		putBatch(2, ids.NewRowID(), ColumnValue{Column: "name", Value: codec.Text("ann")}),
	})
	if err := inbox.Receive(a); err != nil {
		t.Fatal(err)
	}
	lowDB, highDB := stubDB{ids.NewDBID()}, stubDB{ids.NewDBID()}
	exp, err := NewExporter(lowDB, Config{Role: RoleLowExporter, Domain: lowDB.id, Stream: "s"}, &stubPublisher{})
	if err != nil {
		t.Fatal(err)
	}
	est := exp.Describe(o, nil, nil, StatusOptions{})
	if est.Role != "low-exporter" || est.Export == nil || est.Import != nil || est.Keys != nil {
		t.Fatalf("exporter describe = %+v", est)
	}
	if est.Exported != pub || est.Export.PendingTotal != 1 || !est.Export.HasBacklog {
		t.Fatalf("exporter export = %+v", est.Export)
	}
	if est.GeneratedAt.IsZero() {
		t.Fatal("missing generation timestamp")
	}
	rec, err := NewReceiver(highDB, Config{Role: RoleHighReceiver, Domain: highDB.id}, &stubSource{})
	if err != nil {
		t.Fatal(err)
	}
	rst := rec.Describe(nil, inbox, trust, StatusOptions{})
	if rst.Role != "high-receiver" || rst.Import == nil || rst.Replay == nil || rst.Keys == nil || rst.Export != nil {
		t.Fatalf("receiver describe = %+v", rst)
	}
	if rst.Applied != 0 || rst.Observed != 2 {
		t.Fatalf("applied/observed = %d/%d", rst.Applied, rst.Observed)
	}
	if len(rst.Import.Streams) != 1 || !rst.Import.HasBacklog {
		t.Fatalf("receiver import = %+v", rst.Import)
	}
	if rst.Replay.Total != 0 || len(rst.Keys.Signers) != 1 {
		t.Fatalf("receiver replay/keys = %+v %+v", rst.Replay, rst.Keys)
	}
}
