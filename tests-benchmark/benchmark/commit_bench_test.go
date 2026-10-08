package benchmark

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/marcgauthier/murmur"
	"github.com/marcgauthier/murmur/codec"
	"github.com/marcgauthier/murmur/ids"
	"github.com/marcgauthier/murmur/internal/testidentity"
	"github.com/marcgauthier/murmur/spool"
	"github.com/marcgauthier/murmur/state"
)

// openTemplateStore opens a copy of the n-row template directly at the
// state layer, for storage-component benchmarks.
func openTemplateStore(b *testing.B, n int) (*state.Store, func()) {
	b.Helper()
	tmpl := templateFor(b, n)
	dest := b.TempDir()
	if err := copyDir(tmpl.dir, dest); err != nil {
		b.Fatal(err)
	}
	dataPath := filepath.Join(dest, "data")
	st, err := state.Open(dataPath, tmpl.node, tmpl.dbid,
		state.Options{
			Spool: spool.Options{
				Path:          dataPath,
				MasterKey:     append([]byte(nil), benchKey...),
				WrappingKeyID: "bench",
				Encryption:    spool.EncryptionAES256GCM,
			},
			Limits:        codec.DefaultLimits(),
			OriginSigning: testidentity.Config(tmpl.node),
		})
	if err != nil {
		b.Fatal(err)
	}
	return st, func() { _ = st.Close() }
}

// BenchmarkSpoolCommitLatency commits single-mutation batches directly
// to the store and reports commit latency plus bytes per second.
func BenchmarkSpoolCommitLatency(b *testing.B) {
	for _, n := range datasetSizes(b) {
		b.Run(sizeName(n), func(b *testing.B) {
			st, cleanup := openTemplateStore(b, n)
			defer cleanup()
			ctx := context.Background()
			var lat latency
			var totalBytes int64
			trackPeakAlloc(b)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				batch := &codec.MutationBatch{
					TxID:       ids.NewTxID(),
					OriginNode: st.NodeID(),
					HLC:        st.ClockNow(),
					Mutations: []codec.Mutation{{
						TableID: 1, RowID: ids.NewRowID(), ColumnID: 1,
						Value: codec.Text("commit-latency-probe"),
					}},
				}
				totalBytes += int64(len(codec.EncodeBatch(nil, batch)))
				start := time.Now()
				if _, err := st.CommitLocal(ctx, batch); err != nil {
					b.Fatal(err)
				}
				lat.record(time.Since(start))
			}
			lat.report(b, 1, "commits")
			b.ReportMetric(float64(totalBytes), "wal-bytes")
		})
	}
}

// BenchmarkRemoteApplyRate applies received batches directly to the store
// (no network) and reports remote-apply mutations/sec.
func BenchmarkRemoteApplyRate(b *testing.B) {
	for _, n := range datasetSizes(b) {
		b.Run(sizeName(n), func(b *testing.B) {
			st, cleanup := openTemplateStore(b, n)
			defer cleanup()
			ctx := context.Background()
			remote := ids.NewNodeID()
			// Register the remote origin as trusted (Key adds it to the
			// shared test registry) so origin verification passes.
			_ = testidentity.Key(remote)
			var lat latency
			const mutsPerBatch = 10
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				muts := make([]codec.Mutation, 0, mutsPerBatch)
				for r := 0; r < mutsPerBatch; r++ {
					muts = append(muts, codec.Mutation{
						TableID: 91, RowID: ids.NewRowID(), ColumnID: 2,
						Value: codec.Text("remote-apply-probe"),
					})
				}
				batch := &codec.MutationBatch{
					TxID:       ids.NewTxID(),
					OriginNode: remote,
					Sequence:   uint64(i + 1),
					HLC:        st.ClockNow(),
					Mutations:  muts,
				}
				testidentity.Sign(batch, st.DBID())
				start := time.Now()
				if _, err := st.CommitRemote(ctx, batch); err != nil {
					b.Fatal(err)
				}
				lat.record(time.Since(start))
			}
			lat.report(b, float64(mutsPerBatch), "mutations")
		})
	}
}

var _ = murmur.NewNodeID
