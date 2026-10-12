// Command gowrite writes the fixed interop dataset with the Go spool
// implementation so the Rust crate can prove byte-compatibility.
//
// Usage: gowrite <dir> [enc|plain]  (default enc; enc uses the compat
// key 32 x 0x42).
package main

import (
	"bytes"
	"fmt"
	"os"

	"github.com/marcgauthier/murmur/spool"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: gowrite <dir> [enc|plain]")
		os.Exit(2)
	}
	dir, mode := os.Args[1], "enc"
	if len(os.Args) > 2 {
		mode = os.Args[2]
	}
	o := spool.DefaultOptions(dir)
	o.ReclaimInterval = -1 // deterministic: no background worker
	if mode == "plain" {
		o.Encryption = spool.EncryptionNone
	} else {
		o.MasterKey = bytes.Repeat([]byte{0x42}, 32)
	}
	s, err := spool.Open(o)
	if err != nil {
		fmt.Fprintln(os.Stderr, "open:", err)
		os.Exit(1)
	}
	fail := func(op string, err error) {
		fmt.Fprintln(os.Stderr, op+":", err)
		s.Close()
		os.Exit(1)
	}
	for i := 0; i < 50; i++ {
		if err := s.Put([]byte(fmt.Sprintf("key-%03d", i)), []byte(fmt.Sprintf("value-%d", i))); err != nil {
			fail("put", err)
		}
	}
	if err := s.PutBatch([]spool.KV{{Key: []byte("batch-a"), Value: []byte("1")}, {Key: []byte("batch-b"), Value: []byte("22")}}); err != nil {
		fail("put_batch", err)
	}
	if err := s.Commit([]spool.Mutation{
		{Key: []byte("commit-x"), Value: []byte("xxx")},
		{Key: []byte("key-000"), Deleted: true},
	}, spool.DurabilitySync); err != nil {
		fail("commit", err)
	}
	if err := s.Delete([]byte("key-001")); err != nil {
		fail("delete", err)
	}
	if err := s.Put([]byte("key-010"), []byte("v2")); err != nil {
		fail("put", err)
	}
	if err := s.Put([]byte("key-010"), []byte("v3-final")); err != nil {
		fail("put", err)
	}
	if err := s.Put([]byte("empty-val"), []byte("")); err != nil {
		fail("put", err)
	}
	if err := s.Flush(); err != nil {
		fail("flush", err)
	}
	st := s.Stats()
	fmt.Printf("GO-WRITE keys=%d groups=%d blocks=%d\n", st.Keys, st.GroupsWritten, st.BlocksWritten)
	if err := s.Close(); err != nil {
		fmt.Fprintln(os.Stderr, "close:", err)
		os.Exit(1)
	}
}
