// Package ingest accepts call-completion webhooks and processes them.
package ingest

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/convin/webhook-ingest/internal/stats"
	"github.com/convin/webhook-ingest/internal/store"
)

// recordingWork stands in for downloading and transcoding a recording.
const recordingWork = 50 * time.Millisecond

// recordingTimeout bounds background recording processing so a stuck
// download can't hang around forever.
const recordingTimeout = 10 * time.Second

// Service ingests webhook deliveries.
type Service struct {
	store *store.Store
	cache *stats.Cache
	rdb   *redis.Client
	log   *slog.Logger

	// wg tracks recording-processing goroutines spawned by Ingest so that
	// Shutdown can wait for in-flight work instead of dropping it on the
	// floor when the process exits.
	wg sync.WaitGroup
}

// New builds a Service.
func New(s *store.Store, c *stats.Cache, rdb *redis.Client, log *slog.Logger) *Service {
	return &Service{store: s, cache: c, rdb: rdb, log: log}
}

// Shutdown waits for in-flight background work (recording processing) to
// finish, or for ctx to be done, whichever comes first.
func (s *Service) Shutdown(ctx context.Context) error {
	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Stats returns the cached totals for an account.
func (s *Service) Stats(accountID string) stats.AccountStats {
	return s.cache.Get(accountID)
}

// Ingest stores a delivery and kicks off processing. Processing runs
// asynchronously so the provider gets a fast acknowledgement.
//
// The provider redelivers at least once, including occasional redelivery
// after a 200, so this must be safe to call more than once with the same
// event_id. store.IngestEvent is the source of truth for that: it applies
// the event exactly once inside a transaction guarded by a unique
// constraint, and reports back whether this call actually inserted
// anything. Everything else in this method -- the in-memory cache update
// and kicking off recording processing -- only happens for a genuinely new
// event, so a duplicate delivery can never double-count.
func (s *Service) Ingest(ctx context.Context, evt Event) error {
	payload, err := json.Marshal(evt)
	if err != nil {
		return err
	}

	rec := store.Event{
		EventID:      evt.EventID,
		CallID:       evt.CallID,
		AccountID:    evt.AccountID,
		Status:       evt.Status,
		DurationSec:  evt.DurationSec,
		RecordingURL: evt.RecordingURL,
		OccurredAt:   evt.OccurredAt,
		Payload:      payload,
	}

	inserted, err := s.store.IngestEvent(ctx, rec)
	if err != nil {
		return err
	}
	if !inserted {
		s.log.Info("duplicate delivery ignored", "event_id", evt.EventID)
		return nil
	}
	s.cache.Record(rec.AccountID, rec.DurationSec)

	// Recordings are slow to fetch, so that part does not block the
	// provider. It must NOT run against the request's context: net/http
	// cancels r.Context() the moment the handler returns, which is right
	// after this goroutine is started, so the recording work would almost
	// always be killed mid-flight with no visible error (the caller never
	// sees it, and the old code silently discarded it too). Use a detached
	// context with its own timeout instead, and track the goroutine so
	// Shutdown can wait for it instead of the process just exiting under it
	// on deploy.
	if rec.RecordingURL != "" {
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			bgCtx, cancel := context.WithTimeout(context.Background(), recordingTimeout)
			defer cancel()
			if err := s.processRecording(bgCtx, rec); err != nil {
				s.log.Error("process recording failed",
					"event_id", rec.EventID, "call_id", rec.CallID, "err", err)
			}
		}()
	}

	return nil
}

// processRecording downloads and transcodes the call recording, then marks
// the call as done.
func (s *Service) processRecording(ctx context.Context, rec store.Event) error {
	time.Sleep(recordingWork)
	return s.store.MarkRecordingProcessed(ctx, rec.CallID)
}
