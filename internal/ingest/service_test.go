package ingest_test

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/convin/webhook-ingest/internal/testutil"
)

// eventJSON builds a well-formed call-completion payload.
func eventJSON(eventID, callID, accountID string) string {
	return fmt.Sprintf(`{
	  "event_id":      %q,
	  "call_id":       %q,
	  "account_id":    %q,
	  "status":        "completed",
	  "duration_sec":  143,
	  "recording_url": "https://recordings.example.com/%s.wav",
	  "occurred_at":   "2026-08-13T09:12:00Z"
	}`, eventID, callID, accountID, callID)
}

func post(t *testing.T, url, body string) *http.Response {
	t.Helper()
	resp, err := http.Post(url, "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	return resp
}

func TestWebhookStoresEventAndCall(t *testing.T) {
	srv, st := testutil.NewServer(t)
	eventID, callID, accountID := testutil.IDs(t, st)
	ctx := context.Background()

	body := eventJSON(eventID, callID, accountID)
	if resp := post(t, srv.URL+"/webhooks/calls", body); resp.StatusCode != http.StatusOK {
		t.Fatalf("got %d, want 200", resp.StatusCode)
	}

	exists, err := st.EventExists(ctx, eventID)
	if err != nil {
		t.Fatalf("EventExists: %v", err)
	}
	if !exists {
		t.Fatal("expected the event to be stored")
	}

	var gotAccount string
	row := st.Pool().QueryRow(ctx, `SELECT account_id FROM calls WHERE call_id = $1`, callID)
	if err := row.Scan(&gotAccount); err != nil {
		t.Fatalf("expected a call record for %s: %v", callID, err)
	}
	if gotAccount != accountID {
		t.Fatalf("call belongs to %q, want %q", gotAccount, accountID)
	}
}

func TestDuplicateDeliveryIsIgnored(t *testing.T) {
	srv, st := testutil.NewServer(t)
	eventID, callID, accountID := testutil.IDs(t, st)
	ctx := context.Background()

	body := eventJSON(eventID, callID, accountID)
	for i := 0; i < 3; i++ {
		if resp := post(t, srv.URL+"/webhooks/calls", body); resp.StatusCode != http.StatusOK {
			t.Fatalf("delivery %d: got %d, want 200", i, resp.StatusCode)
		}
	}

	var n int
	row := st.Pool().QueryRow(ctx, `SELECT count(*) FROM events WHERE event_id = $1`, eventID)
	if err := row.Scan(&n); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if n != 1 {
		t.Fatalf("stored %d copies of %s, want 1", n, eventID)
	}
}

// TestConcurrentDuplicateDeliveryDoesNotDoubleCount fires the same event_id
// at the server from many goroutines at once, simulating the provider
// redelivering while a previous delivery is still being processed. Before
// the fix, Ingest checked "does this event_id exist" and then inserted as
// two separate, non-transactional steps, so concurrent requests could both
// see "not found" and both insert -- duplicating the call row and
// double-counting the account aggregate. This must not happen no matter how
// many copies race each other.
func TestConcurrentDuplicateDeliveryDoesNotDoubleCount(t *testing.T) {
	srv, st := testutil.NewServer(t)
	eventID, callID, accountID := testutil.IDs(t, st)
	ctx := context.Background()

	body := eventJSON(eventID, callID, accountID)

	const concurrency = 20
	var wg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp := post(t, srv.URL+"/webhooks/calls", body)
			if resp.StatusCode != http.StatusOK {
				t.Errorf("got %d, want 200", resp.StatusCode)
			}
		}()
	}
	wg.Wait()

	var events, calls int
	if err := st.Pool().QueryRow(ctx,
		`SELECT count(*) FROM events WHERE event_id = $1`, eventID).Scan(&events); err != nil {
		t.Fatalf("count events: %v", err)
	}
	if events != 1 {
		t.Fatalf("stored %d copies of event %s, want 1", events, eventID)
	}

	if err := st.Pool().QueryRow(ctx,
		`SELECT count(*) FROM calls WHERE call_id = $1`, callID).Scan(&calls); err != nil {
		t.Fatalf("count calls: %v", err)
	}
	if calls != 1 {
		t.Fatalf("stored %d call rows for %s, want 1", calls, callID)
	}

	got, err := st.AccountStats(ctx, accountID)
	if err != nil {
		t.Fatalf("AccountStats: %v", err)
	}
	if got.CallCount != 1 || got.TotalDurationSec != 143 {
		t.Fatalf("account_stats = %+v, want CallCount=1 TotalDurationSec=143", got)
	}
}

// TestRecordingIsMarkedProcessedAfterRequestCompletes reproduces the "landed
// calls whose recordings never get marked processed, with nothing in the
// logs" symptom. processRecording used to run in a goroutine against the
// *request's* context, which net/http cancels the instant the handler
// returns -- i.e. essentially immediately, since the goroutine is started
// and then the handler returns right away. The doomed MarkRecordingProcessed
// call then failed with "context canceled", an error the old code discarded
// with `// TODO: handle`. This test waits well past both the simulated
// recording-processing delay and the point where the HTTP response has been
// fully received, so it only passes if processing ran on a context that
// outlives the request.
func TestRecordingIsMarkedProcessedAfterRequestCompletes(t *testing.T) {
	srv, st := testutil.NewServer(t)
	eventID, callID, accountID := testutil.IDs(t, st)
	ctx := context.Background()

	body := eventJSON(eventID, callID, accountID)
	resp := post(t, srv.URL+"/webhooks/calls", body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("got %d, want 200", resp.StatusCode)
	}
	// net/http cancels the request's context as soon as the handler
	// returns, which already happened above -- exactly the window the bug
	// lived in.

	deadline := time.Now().Add(2 * time.Second)
	var processed bool
	for time.Now().Before(deadline) {
		row := st.Pool().QueryRow(ctx,
			`SELECT recording_processed FROM calls WHERE call_id = $1`, callID)
		if err := row.Scan(&processed); err != nil {
			t.Fatalf("scan: %v", err)
		}
		if processed {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	if !processed {
		t.Fatalf("recording for call %s was never marked processed", callID)
	}
}
