package murmur

import (
	"context"
	"fmt"
	"sort"
)

// --- key rotation ---

// RotateStorageKey replaces the at-rest wrapping key by re-protecting the
// key envelope under new wrapping material. Data keys and encrypted segments
// are unchanged; subsequent Open calls use the new application material.
func (db *DB) RotateStorageKey(ctx context.Context, material KeyMaterial) error {
	db.mu.Lock()
	if db.dbState != StateReady {
		st := db.dbState
		db.mu.Unlock()
		return fmt.Errorf("murmur: rotate in state %s", st)
	}
	if db.store == nil {
		db.mu.Unlock()
		return fmt.Errorf("%w: rotation requires an open store", ErrEncryptionKey)
	}
	inv := db.store.KeyInventory()
	currentID := inv.WrappingKeyID
	db.dbState = StateRotatingKey
	db.encPhase = "rotating"
	db.mu.Unlock()
	defer func() {
		db.mu.Lock()
		db.encPhase = "idle"
		db.mu.Unlock()
		db.setState(StateReady)
	}()

	if err := validateKeyMaterial(currentID, material); err != nil {
		return err
	}
	if err := db.store.RotateWrappingKey(material.Key, material.ID); err != nil {
		return fmt.Errorf("murmur: rotate wrapping key: %w", err)
	}
	return nil
}

// RotateDataKey starts a new data-key generation. Blocks written afterwards
// use the fresh key; existing blocks keep theirs until compaction or
// RewriteEncryptedFiles re-encrypts them. Online: writers are not blocked.
func (db *DB) RotateDataKey(ctx context.Context) error {
	db.mu.Lock()
	if db.dbState != StateReady {
		st := db.dbState
		db.mu.Unlock()
		return fmt.Errorf("murmur: rotate data key in state %s", st)
	}
	if db.store == nil {
		db.mu.Unlock()
		return fmt.Errorf("%w: rotation requires an open store", ErrEncryptionKey)
	}
	db.encPhase = "rotating"
	db.mu.Unlock()
	defer func() {
		db.mu.Lock()
		db.encPhase = "idle"
		db.mu.Unlock()
	}()

	if err := db.store.RotateDataKey(); err != nil {
		return fmt.Errorf("murmur: rotate data key: %w", err)
	}
	return nil
}

// SetEncryptionAlgorithm sets the write algorithm for storage.
// Murmur on Spool exclusively uses AES-256-GCM; setting AES-256-GCM rotates
// the active data key, other algorithms are rejected.
func (db *DB) SetEncryptionAlgorithm(ctx context.Context, algorithm EncryptionAlgorithm) error {
	if algorithm != AES256GCM && algorithm != "AES-GCM-256" {
		return fmt.Errorf("murmur: SetEncryptionAlgorithm: unsupported algorithm %s (only %s supported)", algorithm, AES256GCM)
	}
	return db.RotateDataKey(ctx)
}

// --- maintenance rewrite ---

// RewriteEncryptedFiles re-encrypts every segment under the current
// data key. It runs online through Spool's maintenance gate and resumes
// after a crash from its intent file.
func (db *DB) RewriteEncryptedFiles(ctx context.Context) error {
	db.mu.Lock()
	if db.dbState != StateReady {
		st := db.dbState
		db.mu.Unlock()
		return fmt.Errorf("murmur: rewrite in state %s", st)
	}
	if db.store == nil {
		db.mu.Unlock()
		return fmt.Errorf("%w: rewrite requires an open store", ErrEncryptionKey)
	}
	db.mu.Unlock()

	ticket, err := db.sched.Admit(ctx, WriterMaintenance)
	if err != nil {
		return fmt.Errorf("murmur: writer admission: %w", err)
	}
	defer ticket.Release()

	db.mu.Lock()
	db.dbState = StateMaintenance
	db.encPhase = "rewriting"
	db.mu.Unlock()
	db.backupMu.Lock()
	defer db.backupMu.Unlock()
	db.writeMu.Lock()
	defer db.writeMu.Unlock()
	db.applyMu.Lock()
	defer db.applyMu.Unlock()

	snap := db.statusLive()
	preGen := snap.StateGeneration

	markUsable := func() {
		db.mu.Lock()
		db.encPhase = "idle"
		db.mu.Unlock()
		db.setState(StateReady)
	}

	rewriteErr := db.store.RewriteDataKeys()
	if rewriteErr != nil {
		markUsable()
		return fmt.Errorf("murmur: rewrite data keys: %w", rewriteErr)
	}

	if gen, err := db.store.StateGeneration(); err != nil || gen != preGen {
		db.setState(StateFailed)
		if err != nil {
			return fmt.Errorf("murmur: verify after maintenance: %w", err)
		}
		return fmt.Errorf("murmur: generation changed across maintenance (%d != %d)", gen, preGen)
	}
	markUsable()
	return nil
}

// --- status ---

// EncryptionStatus describes at-rest encryption state. It never contains
// key bytes. Key reference counts come from a live inventory scan.
func (db *DB) EncryptionStatus() EncryptionStatus {
	db.mu.Lock()
	phase := db.encPhase
	store := db.store
	db.mu.Unlock()
	if phase == "" {
		phase = "idle"
	}
	st := EncryptionStatus{Algorithm: AES256GCM, Phase: phase}
	if store == nil {
		return st
	}
	inv := store.KeyInventory()
	if inv.WrappingKeyID != "" {
		st.ApplicationKeyID = inv.WrappingKeyID
	}
	st.ActiveDataKeyID = fmt.Sprintf("%08x", inv.ActiveDataKeyID)
	st.RegistryGeneration = inv.ManifestGeneration
	if inv.MaintenancePhase != "" && inv.MaintenancePhase != "idle" {
		st.Phase = inv.MaintenancePhase
	}
	st.FilesDone = inv.MaintenanceDone
	st.FilesTotal = inv.MaintenanceTotal

	refs, _ := store.KeyReferences()
	refMap := make(map[uint32]uint64, len(refs))
	ckptMap := make(map[uint32]uint64, len(refs))
	for _, r := range refs {
		refMap[r.KeyID] = r.SegmentFiles
		ckptMap[r.KeyID] = r.Checkpoints
	}

	for _, k := range inv.DataKeys {
		ks := KeyReferenceStatus{
			ID:          fmt.Sprintf("%08x", k.ID),
			Algorithm:   AES256GCM,
			CreatedAt:   k.CreatedAt,
			LiveFiles:   refMap[k.ID],
			Checkpoints: ckptMap[k.ID],
		}
		st.Keys = append(st.Keys, ks)
	}
	sort.Slice(st.Keys, func(i, j int) bool { return st.Keys[i].ID < st.Keys[j].ID })
	return st
}
