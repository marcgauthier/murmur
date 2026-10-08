package rime

import (
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"strings"
)

// UUID is a 16-byte universally unique identifier. It is usable as a RIME
// primary key. The zero value is treated as "unset" for uuid5 auto-generation.
type UUID [16]byte

// Namespace constants for deterministic UUIDv5 generation.
var (
	NamespaceDNS  = UUID{0x6b, 0xa7, 0xb8, 0x10, 0x9d, 0xad, 0x11, 0xd1, 0x80, 0xb4, 0x00, 0xc0, 0x4f, 0xd4, 0x30, 0xc8}
	NamespaceURL  = UUID{0x6b, 0xa7, 0xb8, 0x11, 0x9d, 0xad, 0x11, 0xd1, 0x80, 0xb4, 0x00, 0xc0, 0x4f, 0xd4, 0x30, 0xc8}
	NamespaceOID  = UUID{0x6b, 0xa7, 0xb8, 0x12, 0x9d, 0xad, 0x11, 0xd1, 0x80, 0xb4, 0x00, 0xc0, 0x4f, 0xd4, 0x30, 0xc8}
	NamespaceX500 = UUID{0x6b, 0xa7, 0xb8, 0x14, 0x9d, 0xad, 0x11, 0xd1, 0x80, 0xb4, 0x00, 0xc0, 0x4f, 0xd4, 0x30, 0xc8}
)

// NewUUIDv5 derives a deterministic UUIDv5 (SHA-1 namespace + name, RFC 4122
// version/variant bits) using only the Go standard library.
func NewUUIDv5(namespace UUID, name string) UUID {
	h := sha1.New()
	h.Write(namespace[:])
	h.Write([]byte(name))
	sum := h.Sum(nil)
	var out UUID
	copy(out[:], sum[:16])
	out[6] = (out[6] & 0x0f) | 0x50 // version 5
	out[8] = (out[8] & 0x3f) | 0x80 // RFC 4122 variant
	return out
}

// TableNamespace derives a stable namespace UUID for a table name.
func TableNamespace(table string) UUID {
	return NewUUIDv5(NamespaceURL, "rime:table:"+table)
}

func (u UUID) String() string {
	var buf [36]byte
	hex.Encode(buf[0:8], u[0:4])
	buf[8] = '-'
	hex.Encode(buf[9:13], u[4:6])
	buf[13] = '-'
	hex.Encode(buf[14:18], u[6:8])
	buf[18] = '-'
	hex.Encode(buf[19:23], u[8:10])
	buf[23] = '-'
	hex.Encode(buf[24:36], u[10:16])
	return string(buf[:])
}

// ParseUUID parses a canonical 8-4-4-4-12 hex UUID string.
func ParseUUID(s string) (UUID, error) {
	var u UUID
	clean := strings.ReplaceAll(s, "-", "")
	if len(clean) != 32 {
		return u, fmt.Errorf("rime: invalid UUID %q", s)
	}
	b, err := hex.DecodeString(clean)
	if err != nil {
		return u, fmt.Errorf("rime: invalid UUID %q: %w", s, err)
	}
	copy(u[:], b)
	return u, nil
}

// IsZero reports whether the UUID is the zero value.
func (u UUID) IsZero() bool { return u == UUID{} }
