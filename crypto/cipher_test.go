package crypto

import (
	"bytes"
	"encoding/hex"
	"testing"
)

func mustHex(t *testing.T, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(s)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// TestPublishedVectors checks every cipher against published test vectors:
// NIST SP 800-38D cases (as embedded in Go's own AES-GCM suite), RFC 8439
// A.5, draft-irtf-cfrg-xchacha-01 A.3.1.
func TestPublishedVectors(t *testing.T) {
	vectors := []struct {
		name      string
		alg       AlgorithmID
		key       string
		nonce     string
		plaintext string
		aad       string
		sealed    string // ciphertext || tag
	}{
		{
			name:      "AES-128-GCM/NIST-case2",
			alg:       AlgorithmAES128GCM,
			key:       "11754cd72aec309bf52f7687212e8957",
			nonce:     "3c819d9a9bed087615030b65",
			plaintext: "",
			aad:       "",
			sealed:    "250327c674aaf477aef2675748cf6971",
		},
		{
			name:      "AES-192-GCM/NIST-case",
			alg:       AlgorithmAES192GCM,
			key:       "e2e001a36c60d2bf40d69ff5b2b1161ea218db263be16a4e",
			nonce:     "3c819d9a9bed087615030b65",
			plaintext: "",
			aad:       "",
			sealed:    "c7b8da1fe2e3dccc4071ba92a0a57ba8",
		},
		{
			name:      "AES-256-GCM/NIST-case",
			alg:       AlgorithmAES256GCM,
			key:       "5394e890d37ba55ec9d5f327f15680f6a63ef5279c79331643ad0af6d2623525",
			nonce:     "3c819d9a9bed087615030b65",
			plaintext: "",
			aad:       "",
			sealed:    "d9b260d4bc4630733ffb642f5ce45726",
		},
		{
			name:      "ChaCha20-Poly1305/RFC8439-A.5-vector1",
			alg:       AlgorithmChaCha20Poly1305,
			key:       "808182838485868788898a8b8c8d8e8f909192939495969798999a9b9c9d9e9f",
			nonce:     "070000004041424344454647",
			plaintext: "",
			aad:       "",
			sealed:    "a0784d7a4716f3feb4f64e7f4b39bf04",
		},
		{
			name:      "XChaCha20-Poly1305/draft-A.3.1",
			alg:       AlgorithmXChaCha20Poly1305,
			key:       "808182838485868788898a8b8c8d8e8f909192939495969798999a9b9c9d9e9f",
			nonce:     "404142434445464748494a4b4c4d4e4f5051525354555657",
			plaintext: "4c616469657320616e642047656e746c656d656e206f662074686520636c617373206f66202739393a204966204920636f756c64206f6666657220796f75206f6e6c79206f6e652074697020666f7220746865206675747572652c2073756e73637265656e20776f756c642062652069742e",
			aad:       "50515253c0c1c2c3c4c5c6c7",
			sealed:    "bd6d179d3e83d43b9576579493c0e939572a1700252bfaccbed2902c21396cbb731c7f1b0b4aa6440bf3a82f4eda7e39ae64c6708c54c216cb96b72e1213b4522f8c9ba40db5d945b11b69b982c1bb9e3f3fac2bc369488f76b2383565d3fff921f9664c97637da9768812f615c68b13b52ec0875924c1c7987947deafd8780acf49",
		},
	}
	for _, v := range vectors {
		t.Run(v.name, func(t *testing.T) {
			aead, err := v.alg.NewAEAD(mustHex(t, v.key))
			if err != nil {
				t.Fatal(err)
			}
			got := aead.Seal(nil, mustHex(t, v.nonce), mustHex(t, v.plaintext), mustHex(t, v.aad))
			if !bytes.Equal(got, mustHex(t, v.sealed)) {
				t.Fatalf("seal mismatch:\n got %x\nwant %s", got, v.sealed)
			}
			open, err := aead.Open(nil, mustHex(t, v.nonce), mustHex(t, v.sealed), mustHex(t, v.aad))
			if err != nil {
				t.Fatalf("open: %v", err)
			}
			if !bytes.Equal(open, mustHex(t, v.plaintext)) {
				t.Fatal("open mismatch")
			}
			// Any tag flip must fail authentication.
			bad := mustHex(t, v.sealed)
			bad[len(bad)-1] ^= 1
			if _, err := aead.Open(nil, mustHex(t, v.nonce), bad, mustHex(t, v.aad)); err == nil {
				t.Fatal("expected authentication failure on flipped tag")
			}
		})
	}
}

func TestAlgorithmRoundTrip(t *testing.T) {
	algs := []AlgorithmID{
		AlgorithmAES128GCM, AlgorithmAES192GCM, AlgorithmAES256GCM,
		AlgorithmChaCha20Poly1305, AlgorithmXChaCha20Poly1305,
	}
	for _, alg := range algs {
		t.Run(alg.String(), func(t *testing.T) {
			ks, err := alg.KeySize()
			if err != nil {
				t.Fatal(err)
			}
			key := bytes.Repeat([]byte{0x42}, ks)
			aead, err := alg.NewAEAD(key)
			if err != nil {
				t.Fatal(err)
			}
			nonce := bytes.Repeat([]byte{0x11}, aead.NonceSize())
			pt := bytes.Repeat([]byte{0xab}, 1000)
			aad := []byte("aad-test")
			sealed := aead.Seal(nil, nonce, pt, aad)
			open, err := aead.Open(nil, nonce, sealed, aad)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(open, pt) {
				t.Fatal("round trip mismatch")
			}
			if _, err := ParseAlgorithm(alg.String()); err != nil {
				t.Fatalf("reparse %q: %v", alg.String(), err)
			}
			if len(sealed) != len(pt)+aead.Overhead() {
				t.Fatalf("sealed len %d, want %d", len(sealed), len(pt)+aead.Overhead())
			}
		})
	}
}

func TestAlgorithmKeySizes(t *testing.T) {
	if _, err := AlgorithmAES256GCM.NewAEAD(make([]byte, 16)); err == nil {
		t.Fatal("expected key-length error")
	}
	if _, err := AlgorithmUnknown.NewAEAD(make([]byte, 32)); err == nil {
		t.Fatal("expected unknown-algorithm error")
	}
	if _, err := ParseAlgorithm("AES-512-GCM"); err == nil {
		t.Fatal("expected parse error")
	}
}

func TestDeriveSubkey(t *testing.T) {
	master := bytes.Repeat([]byte{0x77}, 32)
	k1, err := DeriveSubkey(master, AlgorithmAES256GCM, []byte("salt"), []byte("info"))
	if err != nil {
		t.Fatal(err)
	}
	k2, err := DeriveSubkey(master, AlgorithmAES256GCM, []byte("salt"), []byte("info"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(k1, k2) || len(k1) != 32 {
		t.Fatal("derivation not deterministic or wrong length")
	}
	// Callers bind the algorithm into info; same salt/info yields
	// prefix-equal HKDF output by design, so length is the check here.
	k3, err := DeriveSubkey(master, AlgorithmAES128GCM, []byte("salt"), []byte("info"))
	if err != nil {
		t.Fatal(err)
	}
	if len(k3) != 16 {
		t.Fatal("wrong subkey length")
	}
	k3b, err := DeriveSubkey(master, AlgorithmAES128GCM, []byte("salt"), []byte("AES-128-GCM|info"))
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(k3, k3b) {
		t.Fatal("info separation failure")
	}
	k4, err := DeriveSubkey(master, AlgorithmAES256GCM, []byte("other"), []byte("info"))
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(k1, k4) {
		t.Fatal("salt separation failure")
	}
	if _, err := DeriveSubkey(make([]byte, 16), AlgorithmAES256GCM, nil, nil); err == nil {
		t.Fatal("expected master-length error")
	}
}
