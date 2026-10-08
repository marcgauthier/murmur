package rime_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/marcgauthier/murmur/rime"
)

func FuzzTransactionModel(f *testing.F) {
	f.Add([]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11})
	f.Add([]byte{255, 0, 255, 3, 17, 80})
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) == 0 {
			return
		}
		if len(data) > 256 {
			data = data[:256]
		}
		db, tab := openDevices(t)
		defer db.Close()
		model := map[string]Device{}
		type pin struct {
			tx    *rime.Tx
			model map[string]Device
		}
		var pins []pin
		defer func() {
			for _, p := range pins {
				p.tx.Close()
			}
		}()
		clone := func(m map[string]Device) map[string]Device {
			out := map[string]Device{}
			for k, v := range m {
				out[k] = v
			}
			return out
		}
		verify := func(tx *rime.Tx, m map[string]Device) {
			rows, err := tab.Where().In(tx).Find()
			if err != nil {
				t.Fatal(err)
			}
			requireRows(t, rows, m)
			for i := 0; i < 8; i++ {
				key := fmt.Sprintf("k%d", i)
				row, err := tab.In(tx).Get(key)
				want, ok := m[key]
				if !ok {
					if !errors.Is(err, rime.ErrNotFound) {
						t.Fatalf("missing %s: %+v %v", key, row, err)
					}
				} else if err != nil || *row != want {
					t.Fatalf("%s: %+v want %+v: %v", key, row, want, err)
				}
			}
			for site := 0; site < 3; site++ {
				value := fmt.Sprint(site)
				expected := map[string]Device{}
				for k, d := range m {
					if d.Site == value {
						expected[k] = d
					}
				}
				rows, err := tab.Where(rime.SF[Device](tab, "Site").Eq(value)).In(tx).Find()
				if err != nil {
					t.Fatal(err)
				}
				requireRows(t, rows, expected)
			}
		}
		abort := errors.New("model rollback")
		for step, b := range data {
			next := clone(model)
			err := db.WriteTx(func(tx *rime.Tx) error {
				for j := 0; j < 1+int(b%4); j++ {
					op := data[(step+j)%len(data)]
					key := fmt.Sprintf("k%d", (op>>3)%8)
					switch op % 3 {
					case 0:
						row := Device{ID: key, Hostname: "h" + key, Site: fmt.Sprint(op % 3), Status: int(op) % 5, Latency: step + j}
						if err := tab.In(tx).Upsert(&row); err != nil {
							return err
						}
						next[key] = row
					case 1:
						err := tab.In(tx).Update(key, func(d *Device) error { d.Status++; d.Latency++; return nil })
						row, ok := next[key]
						if !ok {
							if !errors.Is(err, rime.ErrNotFound) {
								t.Fatalf("missing update %s: %v", key, err)
							}
						} else {
							if err != nil {
								return err
							}
							row.Status++
							row.Latency++
							next[key] = row
						}
					case 2:
						err := tab.In(tx).Delete(key)
						if _, ok := next[key]; ok {
							if err != nil {
								return err
							}
							delete(next, key)
						} else if !errors.Is(err, rime.ErrNotFound) {
							t.Fatalf("missing delete %s: %v", key, err)
						}
					}
				}
				// Queries see staged writes (read-your-writes): the in-txn view
				// equals the staged model, filtered or not.
				rows, err := tab.Where().In(tx).Find()
				if err != nil {
					return err
				}
				requireRows(t, rows, next)
				for site := 0; site < 3; site++ {
					value := fmt.Sprint(site)
					expected := map[string]Device{}
					for k, d := range next {
						if d.Site == value {
							expected[k] = d
						}
					}
					srows, err := tab.Where(rime.SF[Device](tab, "Site").Eq(value)).In(tx).Find()
					if err != nil {
						return err
					}
					requireRows(t, srows, expected)
				}
				if b&0x80 != 0 {
					return abort
				}
				return nil
			})
			if b&0x80 != 0 {
				if !errors.Is(err, abort) {
					t.Fatalf("step %d rollback: %v", step, err)
				}
			} else {
				if err != nil {
					t.Fatalf("step %d: %v", step, err)
				}
				model = next
			}
			if step%4 == 0 {
				pins = append(pins, pin{db.ReadTx(), clone(model)})
				if len(pins) > 3 {
					pins[0].tx.Close()
					pins = pins[1:]
				}
			}
			if step%3 == 0 {
				db.GC()
			}
			verify(nil, model)
			for _, p := range pins {
				verify(p.tx, p.model)
			}
		}
		for _, p := range pins {
			p.tx.Close()
		}
		pins = nil
		db.GC()
		verify(nil, model)
		if db.Stats().ActiveTxns != 0 {
			t.Fatal("model leaked snapshots")
		}
	})
}
