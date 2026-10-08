package rime_test

import (
	"fmt"
	"runtime"
	"testing"

	"github.com/marcgauthier/murmur/rime"
)

func benchDB(b *testing.B, n int) (*rime.DB, *rime.Table[Device]) {
	b.Helper()
	db := rime.New()
	dev, err := rime.Register[Device](db, rime.WithCompound[Device]("site_status", "Site", "Status"))
	if err != nil {
		b.Fatal(err)
	}
	recs := make([]*Device, n)
	for i := 0; i < n; i++ {
		recs[i] = &Device{
			ID:       fmt.Sprintf("d-%07d", i),
			Hostname: fmt.Sprintf("host-%07d", i),
			Site:     []string{"OTT", "MTL", "WPG", "YVR"}[i%4],
			Status:   i % 8,
			Latency:  i % 500,
		}
	}
	if err := dev.UpsertMany(recs); err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	return db, dev
}

func BenchmarkPrimaryLookup(b *testing.B) {
	for _, n := range []int{10000, 100000} {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			db, dev := benchDB(b, n)
			defer db.Close()
			b.RunParallel(func(pb *testing.PB) {
				i := 0
				for pb.Next() {
					_, _ = dev.Get(fmt.Sprintf("d-%07d", i%n))
					i++
				}
			})
		})
	}
}

func BenchmarkIndexedLookup(b *testing.B) {
	db, dev := benchDB(b, 100000)
	defer db.Close()
	site := rime.SF[Device](dev, "Site")
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _ = dev.Where(site.Eq("OTT")).Limit(100).Find()
		}
	})
}

func BenchmarkCompoundLookup(b *testing.B) {
	db, dev := benchDB(b, 100000)
	defer db.Close()
	site := rime.SF[Device](dev, "Site")
	status := rime.OF[Device, int](dev, "Status")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = dev.Where(rime.And(site.Eq("OTT"), status.Eq(3))).Limit(100).Find()
	}
}

func BenchmarkRangeLookup(b *testing.B) {
	db, dev := benchDB(b, 100000)
	defer db.Close()
	lat := rime.OF[Device, int](dev, "Latency")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = dev.Where(lat.Between(100, 200)).Limit(100).Find()
	}
}

func BenchmarkInsert(b *testing.B) {
	db := rime.New()
	defer db.Close()
	dev, err := rime.Register[Device](db)
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = dev.Upsert(&Device{ID: fmt.Sprintf("b-%08d", i), Hostname: fmt.Sprintf("h-%08d", i)})
	}
}

func BenchmarkUpdate(b *testing.B) {
	db, dev := benchDB(b, 50000)
	defer db.Close()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = dev.Update(fmt.Sprintf("d-%07d", i%50000), func(d *Device) error {
			d.Status++
			return nil
		})
	}
}

func BenchmarkDelete(b *testing.B) {
	db := rime.New()
	defer db.Close()
	dev, err := rime.Register[Device](db)
	if err != nil {
		b.Fatal(err)
	}
	const total = 200000
	recs := make([]*Device, total)
	for i := 0; i < total; i++ {
		recs[i] = &Device{ID: fmt.Sprintf("x-%07d", i), Hostname: fmt.Sprintf("xh-%07d", i)}
	}
	if err := dev.UpsertMany(recs); err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = dev.Delete(fmt.Sprintf("x-%07d", i%total))
	}
}

func BenchmarkMixedReadWrite(b *testing.B) {
	db, dev := benchDB(b, 50000)
	defer db.Close()
	site := rime.SF[Device](dev, "Site")
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			switch i % 4 {
			case 0, 1:
				_, _ = dev.Get(fmt.Sprintf("d-%07d", i%50000))
			case 2:
				_, _ = dev.Where(site.Eq("OTT")).Limit(10).Find()
			case 3:
				_ = dev.Update(fmt.Sprintf("d-%07d", i%50000), func(d *Device) error {
					d.Latency++
					return nil
				})
			}
			i++
		}
	})
}

func BenchmarkFullScanCount(b *testing.B) {
	db, dev := benchDB(b, 100000)
	defer db.Close()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = dev.Where().Count()
	}
}

func BenchmarkJoin(b *testing.B) {
	db, dev := benchDB(b, 20000)
	defer db.Close()
	sites, _ := rime.Register[Site](db, rime.WithTableName[Site]("sites"))
	for _, s := range []string{"OTT", "MTL", "WPG", "YVR"} {
		_ = sites.Upsert(&Site{ID: s, Region: "R"})
	}
	dsite := rime.SF[Device](dev, "Site")
	sid := rime.SF[Site](sites, "ID")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = rime.InnerJoinOn(dev.In(nil), dsite, sites.In(nil), sid)
	}
}

func BenchmarkAggregation(b *testing.B) {
	db, dev := benchDB(b, 100000)
	defer db.Close()
	lat := rime.OF[Device, int](dev, "Latency")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = dev.Where().Aggregate(rime.Count[Device](), rime.AvgOf(lat))
	}
}

func BenchmarkTxCommit(b *testing.B) {
	db, dev := benchDB(b, 10000)
	defer db.Close()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = db.WriteTx(func(tx *rime.Tx) error {
			for j := 0; j < 10; j++ {
				if err := dev.In(tx).Upsert(&Device{ID: fmt.Sprintf("t-%d", (i*10+j)%10000), Hostname: "h"}); err != nil {
					return err
				}
			}
			return nil
		})
	}
}

func BenchmarkSnapshotRead(b *testing.B) {
	db, dev := benchDB(b, 50000)
	defer db.Close()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tx := db.ReadTx()
		_, _ = dev.In(tx).Get("d-0000001")
		tx.Close()
	}
}

func BenchmarkGC(b *testing.B) {
	db := rime.New()
	defer db.Close()
	dev, err := rime.Register[Device](db)
	if err != nil {
		b.Fatal(err)
	}
	for i := 0; i < 20000; i++ {
		_ = dev.Upsert(&Device{ID: fmt.Sprintf("g-%06d", i), Hostname: "h"})
	}
	for r := 0; r < 5; r++ {
		for i := 0; i < 20000; i++ {
			_ = dev.Update(fmt.Sprintf("g-%06d", i), func(d *Device) error {
				d.Status++
				return nil
			})
		}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db.GC()
	}
}

// BenchmarkPrimaryLookupSizes scales point reads across table sizes.
func BenchmarkPrimaryLookupSizes(b *testing.B) {
	for _, n := range []int{1000, 10000, 100000, 1000000} {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			db, dev := benchDB(b, n)
			defer db.Close()
			for i := 0; i < b.N; i++ {
				_, _ = dev.Get(fmt.Sprintf("d-%07d", i%n))
			}
		})
	}
}

// BenchmarkIndexedLookupSizes scales an indexed equality query.
func BenchmarkIndexedLookupSizes(b *testing.B) {
	for _, n := range []int{1000, 10000, 100000, 1000000} {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			db, dev := benchDB(b, n)
			defer db.Close()
			site := rime.SF[Device](dev, "Site")
			for i := 0; i < b.N; i++ {
				_, _ = dev.Where(site.Eq("OTT")).Limit(100).Find()
			}
		})
	}
}

func withProcs(n int, fn func()) {
	old := runtime.GOMAXPROCS(n)
	defer runtime.GOMAXPROCS(old)
	fn()
}

// BenchmarkPrimaryLookupThreads sweeps goroutine counts for point reads.
func BenchmarkPrimaryLookupThreads(b *testing.B) {
	for _, procs := range []int{1, 2, 4, 8, 16, 32, 64} {
		b.Run(fmt.Sprintf("p=%d", procs), func(b *testing.B) {
			db, dev := benchDB(b, 100000)
			defer db.Close()
			b.ResetTimer()
			withProcs(procs, func() {
				b.RunParallel(func(pb *testing.PB) {
					i := 0
					for pb.Next() {
						_, _ = dev.Get(fmt.Sprintf("d-%07d", i%100000))
						i++
					}
				})
			})
		})
	}
}

// BenchmarkMixedThreads sweeps goroutine counts for a mixed workload.
func BenchmarkMixedThreads(b *testing.B) {
	for _, procs := range []int{1, 2, 4, 8, 16, 32, 64} {
		b.Run(fmt.Sprintf("p=%d", procs), func(b *testing.B) {
			db, dev := benchDB(b, 50000)
			defer db.Close()
			site := rime.SF[Device](dev, "Site")
			b.ResetTimer()
			withProcs(procs, func() {
				b.RunParallel(func(pb *testing.PB) {
					i := 0
					for pb.Next() {
						switch i % 4 {
						case 0, 1:
							_, _ = dev.Get(fmt.Sprintf("d-%07d", i%50000))
						case 2:
							_, _ = dev.Where(site.Eq("OTT")).Limit(10).Find()
						case 3:
							_ = dev.Update(fmt.Sprintf("d-%07d", i%50000), func(d *Device) error {
								d.Latency++
								return nil
							})
						}
						i++
					}
				})
			})
		})
	}
}
