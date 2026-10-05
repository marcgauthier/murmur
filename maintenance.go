package murmur

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"sort"

	"github.com/marcgauthier/murmur/crypto"
	"github.com/marcgauthier/murmur/replication"
)

// --- key rotation ---

// RotateStorageKey replaces the at-rest wrapping key by re-encrypting the
// key registry. Data keys and encrypted files are unchanged; subsequent
// Open calls use the new application material. material requires a new
// application-key ID (different from the current one), an explicit
// supported algorithm identifying the input key bytes, and matching key
// length. The registry wrapper stays domain-separated AES-256-GCM.
func (db *DB) RotateStorageKey(ctx context.Context, material KeyMaterial) error {
	db.mu.Lock()
	if db.dbState != StateReady {
		st := db.dbState
		db.mu.Unlock()
		return fmt.Errorf("murmur: rotate in state %s", st)
	}
	reg := db.keyReg
	if reg == nil {
		db.mu.Unlock()
		return fmt.Errorf("%w: rotation requires an encrypted store", ErrEncryptionKey)
	}
	currentID := reg.StorageKeyID()
	db.dbState = StateRotatingKey
	db.encPhase = crypto.PhaseRotating
	db.mu.Unlock()
	defer func() {
		db.mu.Lock()
		db.encPhase = crypto.PhaseIdle
		db.mu.Unlock()
		db.setState(StateReady)
	}()

	if err := validateKeyMaterial(currentID, material); err != nil {
		return err
	}
	algID, _ := crypto.ParseAlgorithm(material.Algorithm) // validated above
	if _, err := reg.RewrapWith(ctx, material); err != nil {
		return fmt.Errorf("murmur: rewrap key registry: %w", err)
	}
	// Later persists must re-wrap under the new key, not the provider that
	// served the old one. The next Open uses the application's own
	// configured provider/material.
	reg.ReplaceProvider(&crypto.MapProvider{
		Keys:      map[string][]byte{material.ID: bytes.Clone(material.Key)},
		CurrentID: material.ID,
		Algorithm: algID,
	})
	return nil
}

// RotateDataKey starts a new data-key generation. Files written afterwards
// use the fresh key; existing files keep theirs until re-encrypted (lazily
// by the expiry worker past the rotation lifetime, or eagerly by
// RewriteEncryptedFiles). Online: writers are not blocked.
func (db *DB) RotateDataKey(ctx context.Context) error {
	db.mu.Lock()
	if db.dbState != StateReady {
		st := db.dbState
		db.mu.Unlock()
		return fmt.Errorf("murmur: rotate data key in state %s", st)
	}
	mgr := db.encMgr
	if mgr == nil {
		db.mu.Unlock()
		return fmt.Errorf("%w: rotation requires an encrypted store", ErrEncryptionKey)
	}
	db.encPhase = crypto.PhaseRotating
	db.mu.Unlock()
	defer func() {
		db.mu.Lock()
		db.encPhase = crypto.PhaseIdle
		db.mu.Unlock()
	}()

	if _, err := mgr.RotateDataKey(ctx); err != nil {
		return fmt.Errorf("murmur: rotate data key: %w", err)
	}
	return nil
}

// SetEncryptionAlgorithm persists a new active write algorithm plus a fresh
// data key; files created afterwards use both. Application wrapping
// material and existing files are unchanged. A reopen must configure the
// same algorithm (Open refuses to silently revert it).
func (db *DB) SetEncryptionAlgorithm(ctx context.Context, algorithm EncryptionAlgorithm) error {
	algID, err := parseAlgorithm(algorithm)
	if err != nil {
		return fmt.Errorf("murmur: SetEncryptionAlgorithm: %w", err)
	}
	db.mu.Lock()
	if db.dbState != StateReady {
		st := db.dbState
		db.mu.Unlock()
		return fmt.Errorf("murmur: set algorithm in state %s", st)
	}
	reg, mgr := db.keyReg, db.encMgr
	if reg == nil || mgr == nil {
		db.mu.Unlock()
		return fmt.Errorf("%w: algorithm change requires an encrypted store", ErrEncryptionKey)
	}
	db.dbState = StateRotatingKey
	db.encPhase = crypto.PhaseRotating
	db.mu.Unlock()
	defer func() {
		db.mu.Lock()
		db.encPhase = crypto.PhaseIdle
		db.mu.Unlock()
		db.setState(StateReady)
	}()

	if err := reg.SetDefaultAlgorithm(ctx, algID); err != nil {
		return fmt.Errorf("murmur: set write algorithm: %w", err)
	}
	// The algorithm is persisted; mint its first data key now. If this
	// fails the caller retries RotateDataKey: until then new files keep
	// using the previous generation (retained, never orphaned).
	if _, err := mgr.RotateDataKey(ctx); err != nil {
		return fmt.Errorf("murmur: mint data key for %s: %w", algorithm, err)
	}
	db.mu.Lock()
	db.cfg.Encryption.Algorithm = algorithm
	db.mu.Unlock()
	return nil
}

// --- maintenance rewrite ---

// stopReplication stops the replication manager for the maintenance window
// and returns its peer set for restart. db.repl stays nil until
// startReplicationAfterMaintenance. Callers must serialize maintenance
// (the Ready->Maintenance transition under db.mu does that).
func (db *DB) stopReplication() (peers []replication.PeerInfo, hadManager bool) {
	db.mu.Lock()
	old := db.repl
	done := db.replDone
	db.repl, db.replDone = nil, nil
	db.mu.Unlock()
	if old == nil {
		return nil, false
	}
	peers = old.ConfiguredPeers()
	_ = old.Close()
	if done != nil {
		<-done
	}
	return peers, true
}

// startReplicationAfterMaintenance rebuilds the manager over the reopened
// store. A rebuild failure degrades to replication-off (logged); the store
// itself is usable.
func (db *DB) startReplicationAfterMaintenance(peers []replication.PeerInfo, hadManager bool) {
	if !hadManager {
		return
	}
	mgr, err := db.newReplicationManager(peers)
	if err != nil {
		db.log.Error("restart replication after maintenance failed", "err", err.Error())
		return
	}
	db.mu.Lock()
	db.repl = mgr
	db.mu.Unlock()
	db.startReplication(mgr)
}

// RewriteEncryptedFiles re-encrypts every content file with the current
// data key and write algorithm. It enters maintenance: local durable writes
// fail with ErrMaintenance, remote apply blocks, active state operations
// drain, then Pebble flushes and closes for the rewrite. Afterwards Pebble
// reopens, identity/generation verify, and writes and replication resume.
//
// SQL reads keep serving the in-memory materialization throughout (Status
// reports StateMaintenance). On ctx cancel the rewrite stops at a file
// boundary: completed files are durable, the journal resumes the rest on
// the next Open, and the store still reopens and resumes normally.
func (db *DB) RewriteEncryptedFiles(ctx context.Context) error {
	db.mu.Lock()
	if db.dbState != StateReady {
		st := db.dbState
		db.mu.Unlock()
		return fmt.Errorf("murmur: rewrite in state %s", st)
	}
	mgr := db.encMgr
	if mgr == nil || db.keyReg == nil {
		db.mu.Unlock()
		return fmt.Errorf("%w: rewrite requires an encrypted store", ErrEncryptionKey)
	}
	db.mu.Unlock()

	// Serialize with scheduled backups, then drain local transactions and
	// remote applies. Lock order (outer to inner): scheduler ticket,
	// backupMu, writeMu, applyMu, store gate; every other path nests the
	// same way. Rewrites are maintenance-class writers admitted before
	// the state flips, so a refused admission leaves no residue.
	ticket, err := db.sched.Admit(ctx, WriterMaintenance)
	if err != nil {
		return fmt.Errorf("murmur: writer admission: %w", err)
	}
	defer ticket.Release()

	db.mu.Lock()
	db.dbState = StateMaintenance
	db.encPhase = crypto.PhaseRewriting
	mgr = db.encMgr
	db.mu.Unlock()
	db.backupMu.Lock()
	defer db.backupMu.Unlock()
	db.writeMu.Lock()
	defer db.writeMu.Unlock()
	db.applyMu.Lock()
	defer db.applyMu.Unlock()

	// Snapshot status for the window and mark the store unusable so Status
	// and DBID serve the snapshot instead of a closed handle.
	snap := db.statusLive()
	preGen := snap.StateGeneration
	db.mu.Lock()
	db.lastStatus, db.lastStatusOK = snap, true
	db.storeUsable = false
	db.mu.Unlock()
	markUsable := func() {
		db.mu.Lock()
		db.storeUsable = true
		db.encPhase = crypto.PhaseIdle
		db.mu.Unlock()
		db.setState(StateReady)
	}

	peers, hadManager := db.stopReplication()
	if err := db.store.CloseForMaintenance(); err != nil {
		if db.store.MaintenanceClosed() {
			// Past flush: the handle is dead; reopen before resuming.
			if rerr := db.store.ReopenAfterMaintenance(); rerr != nil {
				db.setState(StateFailed)
				return fmt.Errorf("murmur: reopen after failed maintenance close: %w (close: %v)",
					rerr, err)
			}
		}
		// Flush failed: the store never closed and stays usable.
		db.startReplicationAfterMaintenance(peers, hadManager)
		markUsable()
		return fmt.Errorf("murmur: close for maintenance: %w", err)
	}
	// The store is closed; rewrite (journaled, resumable). A rewrite error
	// or cancel still reopens and resumes below.
	rewriteErr := func() error {
		_, _, err := mgr.RewriteEncryptedFiles(ctx)
		return err
	}()
	if err := db.store.ReopenAfterMaintenance(); err != nil {
		db.setState(StateFailed)
		return fmt.Errorf("murmur: reopen after maintenance: %w", err)
	}
	// Nobody could write across the window; a generation change means the
	// rewrite did not preserve logical state. Fail closed (restart
	// recovers: Open re-verifies and resumes any journaled work).
	if gen, err := db.store.StateGeneration(); err != nil || gen != preGen {
		db.setState(StateFailed)
		if err != nil {
			return fmt.Errorf("murmur: verify after maintenance: %w", err)
		}
		return fmt.Errorf("murmur: generation changed across maintenance (%d != %d)", gen, preGen)
	}
	db.startReplicationAfterMaintenance(peers, hadManager)
	markUsable()
	return rewriteErr
}

// --- status ---

// EncryptionStatus describes at-rest encryption state. It never contains
// key bytes. Key reference counts come from a live inventory scan.
func (db *DB) EncryptionStatus() EncryptionStatus {
	db.mu.Lock()
	phase := db.encPhase
	cfgAlg := db.cfg.Encryption.Algorithm
	reg, mgr := db.keyReg, db.encMgr
	db.mu.Unlock()
	if phase == "" {
		phase = crypto.PhaseIdle
	}
	st := EncryptionStatus{Algorithm: cfgAlg, Phase: phase}
	if reg == nil {
		return st
	}
	st.Algorithm = algorithmString(reg.DefaultAlgorithm())
	st.ApplicationKeyID = reg.StorageKeyID()
	st.RegistryGeneration = reg.Generation()
	if id, err := reg.ActiveKeyID(); err == nil {
		st.ActiveDataKeyID = hex.EncodeToString(id[:])
	}
	var inv map[string]*crypto.KeyRefs
	if mgr != nil {
		inv, _ = mgr.BuildInventory()
		ms := mgr.Status()
		st.FilesDone, st.FilesTotal = ms.FilesDone, ms.FilesTotal
		st.BytesDone, st.BytesTotal = ms.BytesDone, ms.BytesTotal
	}
	for _, k := range reg.Keys() {
		ks := KeyReferenceStatus{
			ID:        hex.EncodeToString(k.ID[:]),
			Algorithm: algorithmString(k.Alg),
			CreatedAt: k.CreatedAt,
		}
		if r := inv[string(k.ID[:])]; r != nil {
			ks.LiveFiles, ks.OpenHandles = r.LiveFiles, r.OpenHandles
			ks.Checkpoints, ks.Backups = r.Checkpoints, r.Backups
		}
		st.Keys = append(st.Keys, ks)
	}
	sort.Slice(st.Keys, func(i, j int) bool { return st.Keys[i].ID < st.Keys[j].ID })
	return st
}
