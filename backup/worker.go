package backup

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// ScheduleConfig configures the automated background backup worker.
type ScheduleConfig struct {
	// Enabled activates the periodic worker.
	Enabled bool
	// Interval is the duration between automatic backups (e.g. 24 * time.Hour).
	Interval time.Duration
	// Destination is the target storage backend.
	Destination Destination
	// Compression: "gzip" (default) or "none".
	Compression string
	// RetentionDays prunes backups older than this number of days (0 disables).
	RetentionDays int
	// MaxBackups retains at most this many latest backups (0 disables).
	MaxBackups int
	// IncludeFiles packs file objects into scheduled backups.
	IncludeFiles bool
	// Logger for progress and error logs.
	Logger Logger
}

// Worker manages background scheduled backups and retention pruning.
type Worker struct {
	cfg ScheduleConfig
	src SourceDB
	log Logger

	mu      sync.Mutex
	running bool
	cancel  context.CancelFunc
	wg      sync.WaitGroup

	lastBackup time.Time
	lastErr    error
}

// NewWorker creates an automated backup worker.
func NewWorker(cfg ScheduleConfig, src SourceDB) *Worker {
	if cfg.Logger == nil {
		cfg.Logger = discardLogger{}
	}
	if cfg.Interval == 0 {
		cfg.Interval = 24 * time.Hour
	}
	return &Worker{
		cfg: cfg,
		src: src,
		log: cfg.Logger,
	}
}

// Start begins the background backup loop in a goroutine.
func (w *Worker) Start(ctx context.Context) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if !w.cfg.Enabled {
		return nil
	}
	if w.running {
		return nil
	}
	if w.cfg.Destination == nil {
		return fmt.Errorf("backup worker: destination is nil")
	}

	w.running = true
	runCtx, cancel := context.WithCancel(ctx)
	w.cancel = cancel

	w.wg.Add(1)
	go w.loop(runCtx)
	return nil
}

// Stop gracefully terminates the background worker and waits for any active backup to finish.
func (w *Worker) Stop() {
	w.mu.Lock()
	if !w.running {
		w.mu.Unlock()
		return
	}
	w.running = false
	if w.cancel != nil {
		w.cancel()
	}
	w.mu.Unlock()
	w.wg.Wait()
}

// TriggerOnDemand runs an immediate backup cycle synchronously.
func (w *Worker) TriggerOnDemand(ctx context.Context) (*Metadata, error) {
	w.log.Info("backup worker: on-demand backup triggered")
	meta, err := CreateBackup(ctx, w.src, Config{
		Destination:  w.cfg.Destination,
		Compression:  w.cfg.Compression,
		IncludeFiles: w.cfg.IncludeFiles,
		Logger:       w.log,
	})
	w.mu.Lock()
	w.lastBackup = time.Now()
	w.lastErr = err
	w.mu.Unlock()

	if err != nil {
		return nil, err
	}
	w.pruneRetention(ctx)
	return meta, nil
}

func (w *Worker) loop(ctx context.Context) {
	defer w.wg.Done()
	ticker := time.NewTicker(w.cfg.Interval)
	defer ticker.Stop()

	w.log.Info("backup worker: started background schedule",
		"interval", w.cfg.Interval,
		"retention_days", w.cfg.RetentionDays,
		"max_backups", w.cfg.MaxBackups,
	)

	for {
		select {
		case <-ctx.Done():
			w.log.Info("backup worker: shutting down")
			return
		case <-ticker.C:
			w.log.Info("backup worker: interval triggered")
			_, err := CreateBackup(ctx, w.src, Config{
				Destination:  w.cfg.Destination,
				Compression:  w.cfg.Compression,
				IncludeFiles: w.cfg.IncludeFiles,
				Logger:       w.log,
			})
			w.mu.Lock()
			w.lastBackup = time.Now()
			w.lastErr = err
			w.mu.Unlock()

			if err != nil {
				w.log.Error("backup worker: scheduled backup failed", "err", err.Error())
				continue
			}

			w.pruneRetention(ctx)
		}
	}
}

// pruneRetention deletes expired backups based on RetentionDays and MaxBackups.
func (w *Worker) pruneRetention(ctx context.Context) {
	if w.cfg.RetentionDays <= 0 && w.cfg.MaxBackups <= 0 {
		return
	}

	backups, err := w.cfg.Destination.ListBackups(ctx, w.src.ClusterID())
	if err != nil {
		w.log.Warn("backup worker: prune failed to list backups", "err", err.Error())
		return
	}

	// List is sorted newest first
	cutoff := time.Now().AddDate(0, 0, -w.cfg.RetentionDays)

	for i, b := range backups {
		toDelete := false
		if w.cfg.MaxBackups > 0 && i >= w.cfg.MaxBackups {
			toDelete = true
		} else if w.cfg.RetentionDays > 0 && b.CreatedAt.Before(cutoff) {
			toDelete = true
		}

		if toDelete {
			w.log.Info("backup worker: pruning expired backup", "name", b.Name, "created_at", b.CreatedAt)
			if err := w.cfg.Destination.DeleteBackup(ctx, b.Name); err != nil {
				w.log.Warn("backup worker: failed to delete expired backup", "name", b.Name, "err", err.Error())
			}
		}
	}
}
