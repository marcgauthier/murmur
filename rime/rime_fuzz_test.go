package rime_test

import (
	"testing"

	"github.com/marcgauthier/murmur/rime"
)

// FuzzQueryModel applies a deterministic op stream (save/update/delete over
// a tiny key domain) and checks the engine against an in-test model after
// every batch: point reads, indexed counts, and full-scan counts must agree.
func FuzzQueryModel(f *testing.F) {
	f.Add([]byte{1, 2, 3, 4, 5, 6, 7, 8})
	f.Add([]byte{9, 9, 9, 9})
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) == 0 {
			t.Skip("empty input")
		}
		db := rime.New()
		defer db.Close()
		dev, err := rime.Register[Device](db, rime.WithCompound[Device]("site_status", "Site", "Status"))
		if err != nil {
			t.Fatal(err)
		}
		site := rime.SF[Device](dev, "Site")
		status := rime.OF[Device, int](dev, "Status")
		model := map[string]*Device{}
		sites := []string{"OTT", "MTL", "WPG"}
		step := func(i int) (key string, d Device) {
			b := data[i%len(data)]
			key = string([]byte{'k', '0' + byte((int(b)>>4)%4)})
			d = Device{
				ID:       key,
				Hostname: "h-" + key + string([]byte{'a' + byte(i%26)}),
				Site:     sites[int(b)%3],
				Status:   int(b) % 5,
				Latency:  int(b),
			}
			return key, d
		}
		for i := 0; i < 64; i++ {
			key, d := step(i)
			op := data[(i*7)%len(data)] % 4
			switch op {
			case 0, 1:
				if err := dev.Upsert(&d); err != nil {
					t.Fatalf("save %d: %v", i, err)
				}
				cp := d
				model[key] = &cp
			case 2:
				err := dev.Update(key, func(x *Device) error {
					x.Status = (x.Status + 1) % 5
					x.Latency++
					return nil
				})
				if m, ok := model[key]; ok {
					if err != nil {
						t.Fatalf("update %d: %v", i, err)
					}
					m.Status = (m.Status + 1) % 5
					m.Latency++
				} else if err == nil {
					t.Fatalf("update of missing key %d succeeded", i)
				}
			case 3:
				_ = dev.Delete(key)
				delete(model, key)
			}
			if i%8 != 7 {
				continue
			}
			// Point-read agreement.
			for k, want := range model {
				got, err := dev.Get(k)
				if err != nil {
					t.Fatalf("step %d get %s: %v", i, k, err)
				}
				if *got != *want {
					t.Fatalf("step %d divergence for %s: %+v vs %+v", i, k, got, want)
				}
			}
			// Indexed-vs-scan agreement per site.
			for _, s := range sites {
				want := 0
				for _, m := range model {
					if m.Site == s {
						want++
					}
				}
				got, err := dev.Where(site.Eq(s)).Count()
				if err != nil || got != want {
					t.Fatalf("step %d site %s: engine=%d model=%d err=%v", i, s, got, want, err)
				}
			}
			// Compound-vs-scan agreement.
			for _, s := range sites {
				for st := 0; st < 5; st++ {
					want := 0
					for _, m := range model {
						if m.Site == s && m.Status == st {
							want++
						}
					}
					got, err := dev.Where(rime.And(site.Eq(s), status.Eq(st))).Count()
					if err != nil || got != want {
						t.Fatalf("step %d compound %s/%d: engine=%d model=%d", i, s, st, got, want)
					}
				}
			}
			total, _ := dev.Where().Count()
			if total != len(model) {
				t.Fatalf("step %d total: engine=%d model=%d", i, total, len(model))
			}
		}
		// GC must preserve the model.
		db.GC()
		for k, want := range model {
			got, err := dev.Get(k)
			if err != nil || *got != *want {
				t.Fatalf("post-GC divergence for %s: %+v %v", k, got, err)
			}
		}
	})
}
