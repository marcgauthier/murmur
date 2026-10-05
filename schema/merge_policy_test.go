package schema

import (
	"bytes"
	"github.com/marcgauthier/murmur/ids"
	"testing"
)

func TestPolicySchemaIdentityAndImmutability(t *testing.T) {
	columns := []ColumnSchema{{ID: 1, Name: "id", Type: ColBlob}, {ID: 2, Name: "value", Type: ColText}}
	tables := []TableSchema{{ID: 1, Name: "items", PK: 1, Columns: columns}}
	base, err := NewGenesis(tables, 1, ids.NewNodeID(), 1)
	if err != nil {
		t.Fatal(err)
	}
	legacy := EncodeManifest(base)
	if string(legacy[:4]) != "SMF1" {
		t.Fatal("legacy manifest changed")
	}
	explicit := *base
	explicit.Tables = cloneTables(base.Tables)
	explicit.Tables[0].Columns[1].MergePolicy = LWW
	if !bytes.Equal(EncodeManifest(&explicit), legacy) {
		t.Fatal("explicit LWW changed historical bytes")
	}
	policies := []MergePolicy{PN_COUNTER, OR_SET, MAX, MIN}
	hashes := map[[32]byte]bool{base.Hash: true}
	for _, p := range policies {
		next := cloneTables(tables)
		next[0].Columns[1].MergePolicy = p
		if p == MAX || p == MIN {
			next[0].Columns[1].Type = ColReal
		}
		manifest, err := NewGenesis(next, 1, base.CreatedOnNode, 1)
		if err != nil {
			t.Fatal(err)
		}
		if hashes[manifest.Hash] {
			t.Fatal("policy absent from hash")
		}
		hashes[manifest.Hash] = true
		raw := EncodeManifest(manifest)
		if string(raw[:4]) != "SMF2" {
			t.Fatal("policy manifest not versioned")
		}
		back, err := DecodeManifest(raw)
		if err != nil || !EqualRevision(manifest, back) {
			t.Fatal("round trip", err)
		}
		if _, err = NewAuthoredRevision(base, next, ids.NewNodeID(), 2); err == nil {
			t.Fatal("changed existing policy")
		}
	}
	bad := cloneTables(tables)
	bad[0].Columns[0].MergePolicy = OR_SET
	if _, err = BuildRegistry(1, bad); err == nil {
		t.Fatal("policy primary key accepted")
	}
}
