package bridge

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// ErrBackpressure signals that a durable bridge journal cannot accept
// another entry without exceeding its configured capacity. Callers treat
// it as explicit, retryable-after-drain backpressure: nothing was
// accepted and nothing already queued was dropped. Future commit-admission
// and bulk-transfer budgets map this error into their own overload
// responses.
var ErrBackpressure = errors.New("bridge: staging capacity exhausted")

// BackpressureError details a rejected journal admission: which journal,
// what the entry needed, and the usage it would exceed.
type BackpressureError struct {
	Journal     string
	NeedBytes   int64
	UsedBytes   int64
	MaxBytes    int64
	UsedEntries int
	MaxEntries  int
}

func (e *BackpressureError) Error() string {
	if e.UsedEntries >= e.MaxEntries {
		return fmt.Sprintf("bridge: %s holds %d entries (limit %d): %v",
			e.Journal, e.UsedEntries, e.MaxEntries, ErrBackpressure)
	}
	return fmt.Sprintf("bridge: %s needs %d bytes at %d/%d: %v",
		e.Journal, e.NeedBytes, e.UsedBytes, e.MaxBytes, ErrBackpressure)
}

// Unwrap exposes ErrBackpressure for errors.Is.
func (e *BackpressureError) Unwrap() error { return ErrBackpressure }

// CapacityUsage reports durable journal usage against its bounds.
type CapacityUsage struct {
	BytesUsed   int64 `json:"bytes_used"`
	BytesMax    int64 `json:"bytes_max"`
	EntriesUsed int   `json:"entries_used"`
	EntriesMax  int   `json:"entries_max"`
}

// admit checks one journal admission against byte and entry bounds.
func admit(journal string, need, used, maxBytes int64, entries, maxEntries int) error {
	if entries >= maxEntries {
		return &BackpressureError{Journal: journal, NeedBytes: need,
			UsedBytes: used, MaxBytes: maxBytes, UsedEntries: entries, MaxEntries: maxEntries}
	}
	if used+need > maxBytes {
		return &BackpressureError{Journal: journal, NeedBytes: need,
			UsedBytes: used, MaxBytes: maxBytes, UsedEntries: entries, MaxEntries: maxEntries}
	}
	return nil
}

// journalBytes sums the live payload files under dir. Crash-orphaned temp
// files are removed (they are never referenced) and excluded.
func journalBytes(dir string) (int64, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return 0, err
	}
	var total int64
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if strings.Contains(e.Name(), ".tmp-") {
			_ = os.Remove(filepath.Join(dir, e.Name()))
			continue
		}
		fi, err := e.Info()
		if err != nil {
			continue
		}
		total += fi.Size()
	}
	return total, nil
}
