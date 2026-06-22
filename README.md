# github.com/smarty/harness/v2

`harness` is a Go library for building **store-and-forward message-handling services** — the kind of process that accepts a command (over HTTP, a queue, or anywhere else), runs domain logic against it, durably persists the resulting events, acknowledges the caller, and then publishes those events to downstream consumers.

It exists to make one guarantee easy to keep: **a caller is told its work succeeded only after the work is durably stored**, and a stored event is dispatched **at least once** even if the process crashes between persistence and publish.


## When to use it

Reach for `harness` when all of the following are true:

- Your service receives messages that should produce one or more outbound events.
- "I acked the request" must mean "the events are durably stored" — never an unstored optimistic ack.
- You publish to one or more downstream consumers that demand at-least-once delivery in storage order.
- You want a single shared pipeline (with buffering, backpressure, load-shedding, retry, and restart-time recovery already wired up) instead of building those concerns into every handler.

If you only need a request → response handler with no outbound side effects, this library is heavier than you need.


## Architecture at a glance

A pipeline is seven goroutine stages connected by buffered channels. Each stage is a single goroutine, so the order callers observed becomes the order events are written, acked, and published:

```
                                                                    ┌──────────────────┐
                                                                    │  recovery (once  │
                                                                    │  at startup)     │
                                                                    └────────┬─────────┘
                                                                             │ backlog (stored but undispatched)
                                                                             ▼
entrypoint ─► execution ─► serialization ─► persistence ─► completion ─► broadcast ─► terminal
   ▲              │              │               │              │             │           │
   │              │              │               │              │             │           │
caller        domain          encode          Writer        ack caller    Dispatcher    return
           (Execute/Apply)                                                            pooled objs
```

| Stage         | Responsibility                                                                       |
|---------------|--------------------------------------------------------------------------------------|
| entrypoint    | Accept callers; admit or shed; block until the work below it completes.              |
| execution     | Coalesce batches into a unit of work; run registered domain `Execute*`/`Apply*`.     |
| serialization | Encode each outbound event into its `Content` buffer.                                |
| persistence   | Persist the unit via `Storage`; retry forever with backoff until success or shutdown.|
| completion    | Fire each batch's `complete(true)` so blocked entrypoint callers return.             |
| broadcast     | Call `Dispatcher.Dispatch(...)`; retry forever with backoff. Drains recovery first.  |
| terminal      | Return pooled `*Message` / `unitOfWork` values to their `sync.Pool`s.                |

Recovery is the same kind of source as a live unit, just one that fires once at process start: it pages the `stored-but-undispatched` backlog out of the same datastore the `Writer` writes to, snapshots its upper bound so live writes can't pollute the cursor, and feeds those rows into broadcast **before any live unit is allowed past broadcast**.


## Guarantees worth knowing about

| Guarantee                       | What it means in practice                                                                                  |
|---------------------------------|------------------------------------------------------------------------------------------------------------|
| Never ack unstored work         | If shutdown abandons a unit before `Writer.Write` ever succeeded, the blocked caller **panics** with       |
|                                 | `monitoring.ErrBatchAbandoned` instead of returning, so an MQ won't ack and will redeliver after restart.  |
| At-least-once outbound          | The broadcast stage retries forever; on crash between persist and dispatch, recovery republishes at start. |
| In-order persist + dispatch     | One goroutine per stage means batch N is persisted before N+1 and dispatched before N+1.                   |
| Deterministic poison handling   | A serializer error is treated as a caller-contract violation: the process panics and crash-loops until the |
|                                 | offending domain type is fixed. Bad data never wedges silently.                                            |
| Backpressure all the way up     | A wedged downstream (failing `Writer` or `Recoverer`) propagates as fill of the channels, which the HTTP   |
|                                 | wrapper turns into 503s via `ShedThreshold`. There is no unbounded buffer in front of a stuck stage.       |
| Bounded memory under bursts     | Pooled `*Message` and `unitOfWork` values reset between uses, and oversized backing arrays are discarded   |
|                                 | (not pinned) once they exceed the configured working capacity.                                             |

Things the library does **not** do:

- It does not implement exactly-once delivery. Recovery republishes anything not yet marked dispatched.
- It does not transport messages anywhere. You supply a `Dispatcher` (Kafka, RabbitMQ, NATS, HTTP, whatever).
- It does not own the database. You hand it a storage implementation via `Options.Storage(...)`; the bundled `storage/mysql.Mapper` (targeting the schema in `doc/mysql/schema.sql`) is the supported one. The storage seam is module-private for now, so targeting a different store means forking that package.


## A minimal example

```go
package main

import (
    "context"
    "database/sql"
    "encoding/json"
    "io"
    "net/http"
    "reflect"
    "sync"

    _ "github.com/go-sql-driver/mysql"
    harness "github.com/smarty/harness/v2"
    "github.com/smarty/harness/v2/contracts"
    "github.com/smarty/harness/v2/storage/mysql"
)

// Domain message types.
type RenewSubscription struct{ AccountID string }
type SubscriptionRenewed struct{ AccountID string }

// Domain handler: one struct, two methods per command type.
type Handlers struct{}

func (Handlers) ExecuteRenewSubscription(cmd RenewSubscription, broadcast func(...any)) {
    broadcast(SubscriptionRenewed{AccountID: cmd.AccountID})
}
func (Handlers) Execute(message any, broadcast func(...any)) {
    // Required generic switch: the pipeline routes via this method.
    switch m := message.(type) {
    case RenewSubscription:
        Handlers{}.ExecuteRenewSubscription(m, broadcast)
    }
}

type JSONSerializer struct{}

func (JSONSerializer) Serialize(out io.Writer, in any) error { return json.NewEncoder(out).Encode(in) }
func (JSONSerializer) ContentType() string                   { return "application/json; charset=utf-8" }

type kafkaPublisher struct{ /* ... */ }

func (kafkaPublisher) Dispatch(ctx context.Context, messages ...*contracts.Message) error {
    // Push to your broker. Must fully consume `messages` before returning; do not retain references.
    return nil
}

func main() {
    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()

    db, _ := sql.Open("mysql", "user:pass@tcp(localhost:3306)/harness?parseTime=true")

    // One mapper is the whole storage seam: the pipeline builds its persistence,
    // dispatched-marking, and startup recovery on top of it. The mapper reads the
    // server's auto_increment_increment itself on its first write. Table names
    // default to "Messages" and "Snapshots"; override them with functional options.
    mapper := mysql.NewMapper(db)

    pipeline, err := harness.New(ctx,
        harness.Options.DomainTypes(Handlers{}),
        harness.Options.MessageTypes(map[reflect.Type]string{
            reflect.TypeOf(SubscriptionRenewed{}): "subscription:renewed-v1",
        }),
        harness.Options.Serializer(JSONSerializer{}),
        harness.Options.Storage(mapper),
        harness.Options.Dispatcher(kafkaPublisher{}), // your broker; the pipeline marks rows dispatched via Storage
    )
    if err != nil {
        panic(err)
    }

    // Each listener runs on its own goroutine; in production, use something like
    // github.com/smarty/dominoes that supervises them and triggers ordered shutdown.
    var wg sync.WaitGroup
    for _, listener := range pipeline.Listeners {
        wg.Go(listener.Listen)
    }

    // HTTP front door: the wrapper returns 503 above ShedThreshold; the inner
    // handler hands the request payload to SheddingEntrypoint, which returns
    // only after the resulting events have been durably stored — unless the
    // request's context is cancelled (client disconnect, server timeout), in
    // which case it bails out and tracks a CallerDeparted observation.
    // (Use BlockingEntrypoint for non-HTTP sources like message brokers, where
    // an in-flight context never cancels and dropping on disconnect would be
    // an unrecoverable false-ack.)
    handler := pipeline.SheddingHTTPWrapper(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        pipeline.SheddingEntrypoint.Handle(r.Context(), RenewSubscription{AccountID: r.URL.Query().Get("id")})
        w.WriteHeader(http.StatusNoContent)
    }))
    _ = http.ListenAndServe(":8080", handler)

    wg.Wait()
}
```

A few things this example skips that a real wiring would include: a `Monitor` implementation that exports metrics, graceful shutdown that cancels `ctx` and closes the entrypoint, and the recovery of the `ErrBatchAbandoned` panic from the HTTP handler into a 5xx response.


## Configuration

Defaults are tuned so `harness.New(ctx)` produces a runnable (but inert) pipeline with no-op collaborators — useful in tests, never in production.

| Option                    | Default | What it controls                                                                |
|---------------------------|---------|---------------------------------------------------------------------------------|
| `BurstCapacity`           | 1024    | Buffer between entrypoint and execution. Absorbs short request spikes.          |
| `PipelineBufferCapacity`  | 4       | Buffer between every downstream stage. Keep small; deep buffers hide stalls.    |
| `ExecutionUnitSize`       | 64      | Max batches coalesced per unit of work. Trade latency for throughput.           |
| `ShedThreshold`           | 0.80    | Entrypoint fill ratio at which `SheddingHTTPWrapper` starts returning 503.      |
| `DomainTypes(...)`        | —       | Handlers whose `Execute<T>`/`Apply<T>` methods drive the pipeline.              |
| `MessageTypes(...)`       | —       | `reflect.Type` → type-name map stamped into each persisted message.             |
| `Serializer`              | no-op   | Encodes outbound event values into their `Content` buffer.                      |
| `Storage`                 | no-op   | Persists messages, marks them dispatched, and pages the recovery backlog.       |
| `Dispatcher`              | no-op   | Publishes messages downstream (your broker). Retried forever on error.          |
| `Monitor`                 | no-op   | Receives `monitoring.*` observations from every stage.                          |

Invalid configuration (e.g. `BurstCapacity <= 0`, an interface-typed `Execute<T>`, a handler missing its generic `Execute`/`Apply`) is rejected by `New`, which returns a non-nil error wrapping `contracts.ErrInvalidConfiguration`. The returned `Pipeline` is the zero value in that case — don't run it.


## Observability

Every stage emits typed observations to your `Monitor` (`contracts/monitoring`). The ones worth alerting on:

| Observation             | Meaning                                                                                       |
|-------------------------|-----------------------------------------------------------------------------------------------|
| `BatchInFlight`         | Caller admitted, work enqueued.                                                               |
| `BatchComplete`         | Caller's work durably stored; entrypoint about to return.                                     |
| `BatchAbandoned`        | Shutdown before durable write. The caller is panicking; the broker will redeliver.            |
| `LoadShed`              | HTTP admission rejected (returned 503). Sustained occurrence == under-provisioned.            |
| `CallerDeparted`        | HTTP caller's `ctx` cancelled while blocked at the entrypoint (client disconnect, timeout).   |
| `SerializationError`    | A registered domain type produced a value the `Serializer` can't encode. **The process will   |
|                         | panic immediately after this observation** and crash-loop until fixed.                        |
| `PersistenceError`      | `Writer.Write` failed; retry pending.                                                         |
| `PersistenceAbandoned`  | Shutdown abandoned the retry loop. The unit was not stored; the broker redelivers on restart. |
| `BroadcastError`        | `Dispatcher.Dispatch` failed; retry pending. Already-stored events; recovery covers restarts. |
| `BroadcastAbandoned`    | Shutdown abandoned the retry loop. Recovery will redispatch on restart.                       |
| `RecoveryError`         | `Recoverer.Recover` failed. **The whole pipeline is stalled behind this** until success.      |
| `RecoveryComplete`      | Backlog drained; live traffic now flows. Includes total count for visibility.                 |


## Domain-type routing rules

Each value passed to `Options.DomainTypes(...)` must satisfy two parallel contracts:

1. A typed method per command/event it cares about: `ExecuteRenewal(Renewal, func(...any))` or `ApplyRenewal(Renewal)`. The method-name prefix (`Execute`/`Apply`) plus the parameter type is how routing is wired at startup.
2. The generic interface that does the actual dispatch: `Execute(any, func(...any))` and/or `Apply(any)`. The pipeline only invokes the generic method; your switch inside it must forward to the typed methods.

`New` rejects two foot-guns at startup: a typed `Execute<T>` without the generic interface (routes nothing), and a typed method routing an interface type (its `reflect.Type` key can never match a concrete runtime message). What it **cannot** catch: a generic-method switch missing a case its typed methods advertise. That message routes successfully, falls into no `case`, and silently vanishes. Keep the typed methods and the generic switch in lockstep — they are two halves of one contract.


## The `storage/mysql` package

`storage/mysql.Mapper` is the bundled storage implementation. A single `Mapper` backs the whole module: `harness` builds its persistence, dispatched-marking, and startup recovery on top of it, and the `snapshots` package uses the same `Mapper` for snapshot load/save. Construct it with the `*sql.DB` and optional functional options; the table names default to `Messages` and `Snapshots`. It discovers the server's `auto_increment_increment` itself on its first write, so construction does no I/O and never blocks startup when the database is unavailable:

```go
// Defaults: the Messages and Snapshots tables.
mapper := mysql.NewMapper(db)
```

Override either table name with a functional option:

```go
mapper := mysql.NewMapper(db,
    mysql.Options.MessagesTableName("Messages"),
    mysql.Options.SnapshotsTableName("Snapshots"),
)
```

It targets the schema in `doc/mysql/schema.sql`:

```sql
CREATE TABLE Snapshots (
  id               bigint unsigned NOT NULL AUTO_INCREMENT,
  created          datetime(3)     NOT NULL,
  high_watermark   bigint unsigned NOT NULL,
  payload          longblob        NOT NULL,
  content_type     varchar(127)    NOT NULL DEFAULT '',
  content_encoding varchar(31)     NOT NULL DEFAULT '',
  PRIMARY KEY (id)
);

CREATE TABLE Messages (
    id         bigint unsigned AUTO_INCREMENT NOT NULL,
    dispatched datetime(3)                        NULL,
    type       varchar(256)                   NOT NULL,
    payload    mediumblob                     NOT NULL,
    PRIMARY KEY (id)
);
CREATE UNIQUE INDEX ix_messages_dispatched ON Messages (dispatched, id);
```

Unlike the per-role types it replaces, the `Mapper` is **safe for concurrent use** (it pools its statement buffers) — and it must be, since the pipeline drives it from three stages at once: persistence inserting, broadcast marking dispatched, and startup recovery paging the backlog. Notable behaviors:

- **Insert.** One multi-row INSERT per batch; each message's `ID` is derived from `LAST_INSERT_ID() + i*stride`. That derivation is safe only if no other writer issues "bulk inserts" against the table concurrently and `stride` matches the server's `auto_increment_increment`. The batch is not size-capped, so keep per-unit payloads within `max_allowed_packet`.
- **Mark dispatched.** `UPDATE Messages SET dispatched = NOW(3) WHERE dispatched IS NULL AND id IN (...)`. The `IS NULL` guard makes redelivery during recovery a no-op rather than a double-mark. Messages with `ID == 0` are rejected up front — they could never be marked and would republish on every restart.
- **Recovery.** A keyset cursor: it snapshots `MIN(id)/MAX(id)` of undispatched rows on the first call and pages within that frozen window, advancing only after a clean page. Rows written by live traffic during the recovery window fall outside the snapshotted boundary and are handled by the live path.
- **Table names** are validated against `^[A-Za-z0-9_]+$` (they can't be bound as `?` placeholders) and back-tick quoted before interpolation.

The storage seam is module-private, so `Mapper` is the supported implementation; targeting a different database currently means forking this package. A deprecated `mapper.WithLegacyWrite(...)` hook runs a transitional callback inside the same transaction as the message INSERT — retained only for migration and slated for removal.


## Snapshots and replay (the `snapshots` package)

For a domain that folds a long event stream into in-memory state, the `snapshots` package persists and rebuilds that state without replaying from the beginning of time. It uses the same `Storage` (the `Mapper`) as the pipeline.

- `snapshots.Save(ctx, ...)` gzip-compresses a JSON snapshot and writes it, tagged with the high-watermark id of the last event it reflects.
- `snapshots.Load(ctx, ...)` loads a snapshot (the latest, or a specific id via `SnapshotID`), applies it to your domain, and — only if you pass `RegisteredEvents(...)` — replays every event since that high watermark, in id order, by reflectively discovering the domain's `Apply<T>(T)` methods. It returns a `LoadResult` carrying the previous/new high watermarks and the count of events applied.

```go
domain := &AccountProjection{}   // applies the snapshot, then any newer events
snapshot := &AccountSnapshot{}   // the DTO the stored JSON unmarshals into

result, err := snapshots.Load(ctx,
    snapshots.LoadOptions.Storage(mapper),
    snapshots.LoadOptions.Domain(domain),
    snapshots.LoadOptions.LoadedSnapshot(snapshot),
    snapshots.LoadOptions.RegisteredEvents(typesByName, namesByType), // omit to stop at the snapshot
)
// ... later, after the domain has advanced ...
err = snapshots.Save(ctx,
    snapshots.SaveOptions.Storage(mapper),
    snapshots.SaveOptions.HighWatermark(result.NewHighWatermark),
    snapshots.SaveOptions.Snapshot(snapshot),
)
```

`LoadResult.NewHighWatermark` is the id through which the domain now reflects events; when no events are replayed it equals `PreviousHighWatermark` (the snapshot's watermark) rather than zero, so it is always safe to hand straight back to `Save`.


## Building and testing

```
make test          # go mod tidy, go fmt, then short tests with coverage and -race
make build         # make test + go build ./...
make test.db.local # docker compose up MySQL, run storage/mysql integration tests, then down
```

CI (`.github/workflows/build.yml`) runs `make build` on every push.


## Module status

This is the `v2` line of the module. The `WithLegacyWrite(...)` hook on `storage/mysql.Mapper` is the only deprecated surface, retained for migration from older callers and slated for removal; new callers should omit it.


## SMARTY DISCLAIMER

Subject to the terms of the associated license agreement, this software is freely available for your use. This software is FREE, AS IN PUPPIES, and is a gift. Enjoy your new responsibility. This means that while we may consider enhancement requests, we may or may not choose to entertain requests at our sole and absolute discretion.
