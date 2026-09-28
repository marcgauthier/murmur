// Copyright 2026. Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package crypto

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/cockroachdb/pebble/v2/vfs"
)

// RebindOptions configures a coordinated-reseed ciphertext rebind from
// SourceDBID to NewDBID. Callers must keep Pebble closed throughout.
type RebindOptions struct {
	// RegDir holds the KEYREGISTRY file.
	RegDir string
	// Roots holds the data directories whose containers rebind.
	Roots []string
	// Provider supplies the storage key (unchanged by reseed).
	Provider KeyProvider
	// SourceDBID is the backup's cluster identity; NewDBID is the
	// replacement. Equal identities are a no-op (clone).
	SourceDBID [16]byte
	NewDBID    [16]byte
	// Base is the filesystem under the encrypted views. Nil means
	// vfs.Default.
	Base vfs.FS
	// Logger receives warnings. Nil discards.
	Logger Logger
}

// RebindStore rebinds a restored encrypted store from the source DBID to
// the new DBID so normal open under the new identity succeeds: every file
// container is decrypted and re-sealed with new-DBID-bound headers, file
// keys, and record AADs, then the key registry re-derives its KEK and
// re-seals its payload under the new DBID.
//
// Crash-safety comes from idempotent phases with the registry persisted
// last: each file installs via verified-temp plus atomic rename, an
// already-rebound file is detected (readable under the new DBID) and
// skipped, and orphan temps are swept. A crash anywhere retries to
// convergence; success is exactly "registry opens under the new DBID".
// A missing registry (nothing DBID-bound) is a no-op; a registry that
// opens under neither identity fails closed (corrupt store or wrong key).
func RebindStore(ctx context.Context, opt RebindOptions) error {
	if opt.SourceDBID == opt.NewDBID {
		return nil
	}
	if _, err := os.Stat(filepath.Join(opt.RegDir, RegistryFileName)); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("crypto: stat registry: %w", err)
	}
	base := opt.Base
	if base == nil {
		base = vfs.Default
	}
	// Already rebound: the registry persists last, so files completed.
	if reg, err := OpenRegistry(opt.RegDir, opt.Provider, opt.NewDBID); err == nil {
		reg.Close()
		return nil
	}
	srcReg, err := OpenRegistry(opt.RegDir, opt.Provider, opt.SourceDBID)
	if err != nil {
		return fmt.Errorf("crypto: rebind needs source-bound registry: %w", err)
	}
	defer srcReg.Close()
	srcFS, err := NewEncryptedFS(FSOptions{Base: base, Registry: srcReg, DBID: opt.SourceDBID, Logger: opt.Logger})
	if err != nil {
		return fmt.Errorf("crypto: rebind source fs: %w", err)
	}
	dstFS, err := NewEncryptedFS(FSOptions{Base: base, Registry: srcReg, DBID: opt.NewDBID, Logger: opt.Logger})
	if err != nil {
		return fmt.Errorf("crypto: rebind target fs: %w", err)
	}
	files, err := scanRebind(base, opt.Roots)
	if err != nil {
		return err
	}
	for _, path := range files {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err := rebindFile(base, srcFS, dstFS, path); err != nil {
			return fmt.Errorf("crypto: rebind %s: %w", path, err)
		}
	}
	// Registry persists last: success is exactly "opens under new DBID".
	if err := srcReg.RebindDBID(ctx, opt.NewDBID); err != nil {
		return fmt.Errorf("crypto: rebind registry: %w", err)
	}
	return nil
}

// scanRebind lists non-empty regular files for rebind, skipping plaintext
// metadata (LOCK, rotation journal), registry artifacts, and orphan temps.
func scanRebind(base vfs.FS, roots []string) ([]string, error) {
	var out []string
	for _, root := range roots {
		names, err := base.List(root)
		if err != nil {
			continue // a missing dir is fine
		}
		for _, name := range names {
			if isRegistryArtifact(name) || strings.Contains(name, ".compact-") ||
				strings.Contains(name, ".rewrite-") || strings.Contains(name, ".rebind-") ||
				strings.Contains(name, ".tmp-") ||
				name == "LOCK" || name == rewriteJournalName {
				continue
			}
			full := base.PathJoin(root, name)
			fi, err := base.Stat(full)
			if err != nil || fi.IsDir() {
				continue
			}
			if fi.Size() == 0 {
				continue
			}
			out = append(out, full)
		}
	}
	return out, nil
}

// rebindFile re-seals one container from the source DBID to the new DBID:
// orphan temps are swept, an already-rebound image (readable under the new
// DBID) is kept, else the logical content is decrypted, written to a fresh
// verified temp, and atomically installed.
func rebindFile(base vfs.FS, srcFS, dstFS *EncryptedFS, path string) error {
	sweepRebindTemps(base, path)
	if f, err := dstFS.Open(path); err == nil {
		_ = f.Close()
		return nil
	}
	f, err := srcFS.Open(path)
	if err != nil {
		return err
	}
	plain, err := io.ReadAll(fileReader{f: f})
	_ = f.Close()
	if err != nil {
		return err
	}
	var nonce [8]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		Zero(plain)
		return err
	}
	tmp := path + ".rebind-" + hex.EncodeToString(nonce[:])
	w, err := dstFS.Create(tmp, "")
	if err != nil {
		Zero(plain)
		return err
	}
	if _, err := w.Write(plain); err != nil {
		w.Close()
		_ = base.Remove(tmp)
		Zero(plain)
		return err
	}
	if err := w.Sync(); err != nil {
		w.Close()
		_ = base.Remove(tmp)
		Zero(plain)
		return err
	}
	if err := w.Close(); err != nil {
		_ = base.Remove(tmp)
		Zero(plain)
		return err
	}
	Zero(plain)
	if err := dstFS.verifyImage(tmp); err != nil {
		_ = base.Remove(tmp)
		return fmt.Errorf("rebind verify: %w", err)
	}
	if err := base.Rename(tmp, path); err != nil {
		_ = base.Remove(tmp)
		return err
	}
	return dstFS.syncParentDir(path)
}

// sweepRebindTemps removes orphan staging files for one path.
func sweepRebindTemps(base vfs.FS, path string) {
	dir := filepath.Dir(path)
	baseName := filepath.Base(path)
	names, err := base.List(dir)
	if err != nil {
		return
	}
	for _, name := range names {
		if strings.HasPrefix(name, baseName+".rebind-") {
			_ = base.Remove(base.PathJoin(dir, name))
		}
	}
}
