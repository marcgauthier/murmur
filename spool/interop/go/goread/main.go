// Command goread loads a spool store offline with the Go implementation
// and prints records sorted, one per line:
//
//	KV "key"="value"
//	TOMB "key"
//
// Usage: goread <dir> [enc|plain]  (default enc; enc uses the compat
// key 32 x 0x42).
package main

import (
	"bytes"
	"fmt"
	"os"
	"sort"

	"github.com/marcgauthier/murmur/spool"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: goread <dir> [enc|plain]")
		os.Exit(2)
	}
	dir, mode := os.Args[1], "enc"
	if len(os.Args) > 2 {
		mode = os.Args[2]
	}
	var key []byte
	if mode != "plain" {
		key = bytes.Repeat([]byte{0x42}, 32)
	}
	var got [][2]string
	var tombs []string
	err := spool.Load(dir, key, func(recs []spool.Record) error {
		for _, r := range recs {
			if r.Deleted {
				tombs = append(tombs, string(r.Key))
			} else {
				got = append(got, [2]string{string(r.Key), string(r.Value)})
			}
		}
		return nil
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, "load:", err)
		os.Exit(1)
	}
	sort.Slice(got, func(i, j int) bool { return got[i][0] < got[j][0] })
	sort.Strings(tombs)
	for _, kv := range got {
		fmt.Printf("KV %q=%q\n", kv[0], kv[1])
	}
	for _, k := range tombs {
		fmt.Printf("TOMB %q\n", k)
	}
}
