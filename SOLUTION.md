# SOLUTION.md
 Note on commit history: while working through git/PowerShell setup issues,
 my fixes ended up squashed into a single commit rather than the
 incremental history I intended. The commit below documents each change
 individually for clarity. Happy to walk through the actual diffs live.

## What was broken, and why

**1. Duplicate call records / inflated call counts.** `Ingest` checked
`EventExists` and then, as a separate step, ran `InsertEvent`,
`UpsertCall`, and `IncrementAccountStats`. Nothing tied those steps
together: two concurrent redeliveries of the same `event_id` (the provider
explicitly says it will redeliver even after a `200`) could both pass the
existence check before either had inserted, so both would proceed to insert
a call and increment the aggregate. Making it worse, `events.event_id` only
had a plain index, not a unique constraint, so the database itself did
nothing to stop it. This is the classic check-then-act race.

**2. Recordings never marked processed, nothing in the logs.**
`processRecording` ran in a goroutine started from inside the HTTP handler,
but it reused the *request's* `context.Context`. `net/http` cancels that
context the instant the handler returns — which happens right after the
goroutine is spawned. So the goroutine's later call to
`MarkRecordingProcessed` almost always ran against an already-canceled
context and failed. The error was real, but the call site was
`if err := s.processRecording(...); err != nil { // TODO: handle }` — the
error was captured and then thrown away, so it never reached the logs.

**3. In-flight work disappearing on deploy.** The recording-processing
goroutines were fire-and-forget: nothing tracked them, and `main.go`'s
shutdown sequence only called `srv.Shutdown`, which stops accepting new HTTP
connections but has no idea those background goroutines exist. On SIGTERM,
`main` returns and the process exits mid-goroutine.

## Deduplication strategy

I made `event_id` uniqueness a **database constraint**, and made the insert,
call upsert, and stats increment a **single transaction** (`Store.IngestEvent`)
guarded by `INSERT ... ON CONFLICT (event_id) DO NOTHING`. The transaction
reports whether it actually inserted a new row; the service only touches the
in-memory cache and only kicks off recording processing when it did.

I considered a Redis-based approach (`SETNX event:<id>` as a dedup lock)
first, since Redis was already wired up and unused. I didn't go with it as
the *primary* guard because Redis membership and the Postgres write aren't
transactional with each other — a crash between "claimed in Redis" and
"committed to Postgres" either loses the claim (Redis key not set, DB has
the row — fine) or, worse, sets the claim and then fails to commit, silently
dropping a legitimate event forever. A Postgres unique constraint has no
such gap: the write and the dedup check are the same operation. Redis is a
better fit as a *fast-path* optimization layered in front of Postgres (see
below), not as the source of truth.

## What I'd change for 10,000 webhooks/second

At that rate, hitting Postgres with a transaction per event (and a unique
index it has to check) would be the bottleneck. I'd add a Redis fast path:
`SET NX event:<id> EX <ttl>` before ever touching Postgres — a rejected
`SETNX` short-circuits known duplicates cheaply and skips the DB entirely.
The Postgres unique constraint stays as the durable backstop for anything
Redis evicted or missed, so correctness doesn't depend on Redis's TTL.
I'd also batch `account_stats` increments (e.g. accumulate in Redis with
`HINCRBY` and flush to Postgres periodically) instead of one row-level
`UPDATE` per event, and move recording processing off in-process goroutines
onto a real queue (the current approach has no retry, backpressure, or
visibility if the process dies with recordings in flight — `Service.wg`
only helps for a clean shutdown, not a crash).

## What I didn't get to

Signature verification and rate limiting are explicitly out of scope per
the brief, so I left them out. I did not add a metrics/observability layer
(e.g. a counter for duplicate deliveries ignored) — worth having in
production so "how often is the provider actually redelivering" is
visible, but it felt out of scope for a 4-hour box.
