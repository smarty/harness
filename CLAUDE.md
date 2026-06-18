# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

- `make test` — `go mod tidy`, `go fmt ./...`, then short unit tests twice: once for coverage and once with `-race`. Default working command while iterating.
- `make build` — `make test` + `go build ./...`. CI runs this (`.github/workflows/build.yml`).
- `make test.db` — runs the `storage/mysql` package tests against a real MySQL (30s timeout, with `-race`). These hit a live DB and are excluded by `-short`.
- `make test.db.local` — `docker compose -f doc/docker-compose.yml up --wait`, then `make test.db`, then `docker compose down`. Use when no MySQL is already running.
- Single test: `go test -run '^TestName$' ./internal/pipeline` (add `-race -count=1` to match `make test`).
- Single subtest in this codebase's `gunit` style: `go test -run '^TestFixture$/^MethodName$' ./path`.

## Architecture

The module is a staged, store-and-forward message-handling pipeline. The only exported entry point is `harness.New(ctx, options...)` in `config.go`; everything inside `internal/` is unexported.

### Pipeline stages

`internal/pipeline/Build(...)` wires seven `Listen()`-driven goroutine stages connected by buffered channels — one goroutine per stage, so work flows in execution order and persisted IDs are monotonic:

```
entrypoint → execution → serialization → persistence → completion → broadcast → terminal
                                                                       ↑
                                                           recovery (startup-only)
```

- **entrypoint** (`00_entrypoint.go`) exposes two `Handler`s on the returned `contracts.Pipeline`: `SheddingEntrypoint` (admission-controlled via `SheddingHTTPWrapper`, replies 503 over `ShedThreshold`) and `BlockingEntrypoint` (returns only after a durable write).
- **execution** (`01_execution.go`) coalesces up to `ExecutionUnitSize` batches into one `unitOfWork`, runs the reflective `router` over each batch's instructions, and emits resulting `*contracts.Message`s.
- **serialization → persistence → completion** encode payloads, retry the storage insert with backoff (via the internal `adapters.Writer` over the `Storage` seam), then fire each batch's `complete(true)` callback (this is what unblocks `BlockingEntrypoint.Handle`).
- **broadcast** (`05_broadcast.go`) first drains `recovery`'s output (startup backlog) and only then begins forwarding live units; `terminal` returns pooled `unitOfWork`/`Message` values to their `sync.Pool`s.

### Critical invariants (don't violate)

- **Never acknowledge unstored work.** If persistence abandons a unit (ctx cancelled before the `Writer` ever succeeded), or the entrypoint is closed while a caller is still blocked enqueuing, the blocked caller **panics with `monitoring.ErrBatchAbandoned`** rather than returning. Returning normally would let an MQ ack work that was never written. The panic intentionally ends an already-shutting-down process; the broker redelivers on restart.
- **Serializer failures panic.** Producing values that cannot serialize is the caller's contract violation. `02_serialization.go` tracks a `SerializationError` and then panics so the process crash-loops deterministically.
- **persistence, broadcast, and recovery retry forever** (backoff caps at 30s) until the pipeline `ctx` is cancelled. The `Storage` implementation (`storage/mysql.Mapper`) and any caller-supplied inner `Dispatcher` must honor the context they receive.
- **Recovery stalls the whole pipeline by design.** Until recovery returns an empty page, broadcast won't serve live traffic, and that backpressure propagates upstream. Recovery reads the same store persistence writes to; if recovery can't progress, neither can durable writes, so there's no live work worth admitting.
- **Pooled `*contracts.Message`s.** The caller-supplied `Dispatcher.Dispatch` (and any `Storage` that retains message slices) must fully consume their argument slices before returning and must not retain references to messages or their `Content` buffers.
- **One-goroutine stage rule.** The internal `adapters.Writer` and `adapters.Dispatcher` reuse instance-level buffers and are driven from a single goroutine each. The `storage/mysql.Mapper` underneath them **is** safe for concurrent use (it pools its statement buffers) — it has to be, since persistence, broadcast, and recovery all call it at once. Do not add concurrent callers to the adapters themselves.
- **Driving the pipeline.** `contracts.Pipeline.Listeners` must each be run on its own goroutine by the caller (e.g. via something like `github.com/smarty/dominoes`). The entrypoint is one of those listeners, and its `Close()` is what triggers ordered shutdown.

### Domain-type routing

`Options.DomainTypes(...)` accepts any value implementing either `executor` (`Execute(any, func(...any))`) or `applicator` (`Apply(any)`). `scanner.go` reflectively discovers typed methods named `Execute<Foo>(Foo, func(...any))` / `Apply<Foo>(Foo)` and routes by the parameter's `reflect.Type`. `validateDomainTypes` rejects two foot-guns at `New(...)` time:
1. A typed `Execute<Foo>`/`Apply<Foo>` exists but the generic `Execute(any, ...)`/`Apply(any)` interface isn't implemented (routes nothing).
2. A typed handler routes an interface type (its map key can never match the concrete runtime type).
What it **cannot** detect: a generic `Execute`/`Apply` switch that omits a case its typed methods advertise — that message routes then silently vanishes. Keep the generic switch and typed methods in lockstep.

### Storage seam (`contracts → internal/storage → storage/mysql` + `internal/adapters`)

All database work flows through a single seam: `Options.Storage(...)` takes a `storage.Storage` (`Exec(ctx, operation any) error`). The operation types (`InsertMessages`, `MarkMessagesDispatched`, `LoadUndispatchedBounds`/`Page`, `SaveSnapshot`, `LoadSnapshot`, `LoadLatestSnapshot`, `LoadEventsSince`) live in `internal/storage`, and the interface lives there too — so the seam is **module-private**: `storage/mysql.Mapper` is the only possible implementation, and external callers can't supply their own (intentional; targeting another DB means forking). The interface used to be `contracts.Storage` but was moved into `internal/storage` to make this explicit.

- `internal/adapters` holds the thin `Writer`, `Dispatcher`, and `Recovery` types the pipeline wires from `config.Storage`. Each builds a table-agnostic `storage.*` operation and hands it to `Storage.Exec`. They are single-goroutine (one reusable op buffer each). `Dispatcher` rejects `ID == 0` before publishing (an unassigned id could never be marked and would republish forever). `Recovery` is the stateful keyset cursor (snapshot `MIN/MAX(id)` of undispatched rows on the first call, page within that frozen window, advance only after a clean page).
- `storage/mysql.Mapper` implements `Storage` against the `Snapshots` and `Messages` tables in `doc/mysql/schema.sql`. It is **safe for concurrent use** (pooled statement buffers). Insert emits one multi-row INSERT per batch and derives IDs from `LAST_INSERT_ID() + i*stride` (relies on `innodb_autoinc_lock_mode = 2` "simple insert" semantics and `stride` matching the server's `auto_increment_increment`). `quoteTableName` validates table names against `^[A-Za-z0-9_]+$` and back-tick quotes them. `WithLegacyWrite(...)` is a deprecated transitional hook run inside the INSERT transaction.

### Snapshots & replay (`snapshots/`, `internal/domainscan/`)

`snapshots.Save` gzip+JSON-encodes a snapshot row; `snapshots.Load` loads a snapshot (latest or by id), applies it to the domain, and — only when `RegisteredEvents(...)` is supplied — replays events since the snapshot's high watermark. `internal/domainscan` centralizes the reflective `Execute<Foo>`/`Apply<Foo>` method-shape detection shared by the pipeline router and the replay machinery. Note: `LoadResult.NewHighWatermark` is zero when no events were replayed (`EventsAppliedCount == 0`).

### Buffer reclaim discipline (`internal/generic/`)

`Reclaim` / `ReclaimBuffer` are used at the boundaries of pooled values (units of work, message buffers, statement buffers). A single oversized batch is allowed to grow the buffer, but on the next reuse the oversized backing array is discarded rather than pinned in a `sync.Pool` for the life of the process. Pass the **steady-state working capacity** as the cap argument when calling these.

## Conventions specific to this repo

- Tests use `github.com/smarty/gunit/v2` fixtures (method-per-test on a struct). New tests should match that style.
- Pipeline stage files are numbered (`00_entrypoint.go` ... `06_terminal.go`) to make the stage order obvious in directory listings. Preserve the numbering when adding stages.
- `Options.*` is a singleton-method pattern (`var Options singleton; func (singleton) Foo(...)`); new options follow that shape and must also be added to `Options.defaults(...)` if they have a meaningful zero-defeating default.
