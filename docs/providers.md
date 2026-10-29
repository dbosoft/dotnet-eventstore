# Providers & capabilities

## The providers

| Package | Store | Use in `AddEventStore` |
| --- | --- | --- |
| `Dbosoft.EventStore.InMemory` | In-memory (tests, samples, single process) | `.UseInMemory()` |
| `Dbosoft.EventStore.EfCore.Sqlite` | SQLite | `.UseSqlite("Data Source=…")` |
| `Dbosoft.EventStore.EfCore.SqlServer` | SQL Server | `.UseSqlServer("Server=…")` |
| `Dbosoft.EventStore.EfCore.PostgreSql` | PostgreSQL | `.UseNpgsql("Host=…")` |
| `Dbosoft.EventStore.AzureTables` | Azure Table Storage (+ blobs) | `.UseAzureTables("connection-string")` |

One provider (chosen once) backs all domains, each in its own storage unit.

### EF Core

The three EF Core packages are convenience wrappers over one provider. Each `UseXxx(connectionString)` picks
the database. For full control over the `DbContext` — a different connection setup, provider options — use
`UseEfCore` directly, with an optional model hook for provider-specific tweaks:

```csharp
services.AddEventStore(es => es
    .UseEfCore(
        db => db.UseSqlServer(connectionString, o => o.EnableRetryOnFailure()),
        model => model.UseVersionConcurrencyToken())   // optional, provider-specific model tweaks
    .AddDomain("banking", d => d.AddEventTypesImplementing<IBankingEvent>()));
```

The store holds no `DbContext`; it opens one per operation from a factory, so it is safe to share as a
singleton. Appends are race-free via the unique index on `(domain, stream, version)`: a lost race is a unique
violation, re-read and classified as an idempotent replay or a concurrency conflict.

#### Schema & migrations

Each EF Core provider **ships** migrations for the store's schema (in the `Sqlite` / `SqlServer` / `PostgreSql`
package). The library **never applies** them — you decide **if and how**. The registered
`IDbContextFactory<EventStoreDbContext>` hands out a context that already carries the provider, the migrations
assembly, and the history table, so you drive whatever fits your deployment:

```csharp
await using var ctx = await factory.CreateDbContextAsync();
await ctx.Database.MigrateAsync();                       // apply at startup, or…
var sql = ctx.Database.GenerateCreateScript();          // …hand a script to a DBA / your migration pipeline
```

`EnsureCreated()` still works for a throwaway or local schema, but it and migrations are mutually exclusive.

The store records its migrations in **its own** history table — `es_migrations_history` by default, deliberately
distinct from EF's `__EFMigrationsHistory`. This matters when the store shares a database with your application's
own migrations: a single shared history table would make each `Migrate()` treat the other's migrations as
foreign. Override the name (and, on SQL Server / PostgreSQL, the schema) at registration:

```csharp
services.AddEventStore(es => es
    .UseSqlServer(connectionString, migrationsHistoryTable: "es_migrations_history", migrationsHistorySchema: "eventstore")
    .AddDomain("banking", d => d.AddEventTypesImplementing<IBankingEvent>()));
```

### Azure Table Storage

`UseAzureTables(connectionString, tablePrefix?)` backs each domain with its own tables and a blob container
(large event payloads spill to blobs). Call `EnsureCreatedAsync` on the provider once at startup to create the
resources. Several stores can share one account via `tablePrefix`.

## The capability model

Providers are **not** reduced to a lowest common denominator. Each uses the best mechanism its backend offers,
and the model names the differences along two axes.

### Axis 1 — availability is a type

Whether an *operation* exists is a structural fact carried by the type system. A provider supports an optional
operation **iff** it implements the matching interface:

| Interface | Operation |
| --- | --- |
| `IAllStreamReader` | Read the global `$all` sequence (`ReadAllAsync`). |
| `ITimeNavigable` | Resolve a `GlobalPosition` for a wall-clock time (`FindPositionAtAsync`). |
| `ITruncatable` | Truncate a stream before a version. |
| `IStreamDeletable` | Delete a stream (soft or hard). |
| `IRedactable` | Overwrite an event's payload in place. |
| `IScavengeable` | Reclaim tombstones and apply retention. |
| `IStreamMaintenance` | Composite of the four maintenance interfaces above. |
| `IMultiStreamAppend` | Append to several streams atomically (`AppendToStreamsAsync`). |
| `IStreamMetadataStore` | Per-stream metadata / retention policy. |
| `IEnlistableStore` | Enlist writes onto a caller-owned transaction (see [Transactional enlistment](#transactional-enlistment)). |

You branch with an `is`-cast:

```csharp
if (store is IAllStreamReader allReader)
    await foreach (var e in allReader.ReadAllAsync(GlobalPosition.Start)) { … }
```

Calling an operation on a provider that does not support it throws `NotSupportedByProviderException` — a
missing capability is a loud, clear failure, never a silent degrade. (Some helpers do the cast for you and
throw the same exception; for example constructing an `EventStoreSubscription` over a store without `$all`.)

### Axis 2 — behaviour is a flag

How an *available* operation behaves — the guarantees a type cannot express — is carried by
`StoreCapabilities`. Only two flags exist, and each is one a consumer actually branches on:

```csharp
[Flags]
public enum StoreCapabilities
{
    None = 0,

    /// A catch-up read of the global sequence is exact: reading forward from a position never silently
    /// skips an event that commits below a point already returned.
    ExactCatchup = 1 << 0,

    /// More than the latest snapshot per stream is retained and readable.
    SnapshotHistory = 1 << 1,
}
```

`ExactCatchup` is consumed by `EventStoreSubscription` (see [Reading & subscriptions](reading-and-subscriptions.md));
`SnapshotHistory` by the aggregate repository's fall-back-through-older-snapshots loop.

## Capability matrix

| | InMemory | EF Core | Azure Tables |
| --- | :---: | :---: | :---: |
| `IAllStreamReader` (`$all`) | ✅ | ✅ | ✅ |
| `ITimeNavigable` | ✅ | ✅ | ✅ |
| `IStreamMaintenance` (truncate/delete/redact/scavenge) | ✅ | ✅ | ✅ |
| `IStreamMetadataStore` (retention) | ✅ | ✅ | ✅ |
| `IMultiStreamAppend` (atomic multi-stream) | ✅ | ✅ | — |
| `IEnlistableStore` (transactional enlistment) | — | ✅ | — |
| `StoreCapabilities.ExactCatchup` | ✅ | ✅ | — |
| `StoreCapabilities.SnapshotHistory` | ✅ | ✅ | ✅ |

The in-memory store implements every capability and is the reference the [conformance suite](bus-and-modules.md#testing)
runs against. EF Core orders `$all` by a commit sequence taken just before each commit, so it declares
`ExactCatchup`; Azure Tables assigns `$all` positions before commit, so a catch-up read there is complete only up
to a bounded staleness window — the subscription handles that for you (see
[completeness](reading-and-subscriptions.md#completeness-and-exactcatchup)).
Azure Tables cannot do a cross-partition atomic append, so it does not offer `IMultiStreamAppend`.

Every provider passes the same conformance suite; a provider only runs the tests for what it declares.

## Transactional enlistment

By default a write owns its own transaction: the EF Core store opens a context per operation and commits it
immediately. When a consumer needs an append to commit **atomically with its own database writes** — its own
state changes and the events, all or nothing — the EF Core provider lets the append **enlist onto a
transaction the caller owns**. This is the `IEnlistableStore` capability; only the EF Core provider offers it
(in-memory has no transaction, Azure Tables no cross-partition one), and its `IEventStoreUnitOfWorkFactory` is
registered in DI whenever an `UseXxx`/`UseEfCore` registration can rebind its dialect onto a borrowed
connection.

The model is a **borrowed frame**, ambient like Rebus's transaction context. The caller owns the connection
and transaction; the store joins them:

```csharp
await using var connection = new SqlConnection(connectionString);
await connection.OpenAsync();
await using var tx = await connection.BeginTransactionAsync();

// Both writers share this one connection + transaction.
myStateContext.Database.UseTransaction(tx.GetDbTransaction());     // the consumer's own DbContext

await using (var unitOfWork = unitOfWorkFactory.Enlist(connection, tx.GetDbTransaction()))
{
    await store.AppendToStreamAsync("order-42", expectedVersion, events); // enlists — stages, does not commit
    await myStateContext.SaveChangesAsync();                              // the consumer's own writes
    await unitOfWork.CommitAsync();  // takes the events' commit sequence, commits both, then fires the change signals
}
```

Both writers must be on the **same** connection (`UseTransaction` requires it) and the same physical database.
While a frame is ambient (`AmbientEventStoreUnitOfWork.Current`) the enlisted append behaves differently from
the autonomous one, by necessity:

- It **does not retry** a version conflict against the new head and **does not reclassify** a failed save in
  place — a failed save dooms the caller's transaction. A conflict (`WrongExpectedVersionException`, or the raw
  store error) propagates, and the caller rolls the whole frame back and retries. The retry boundary moves up
  to the caller (or, under Rebus, to message retry).
- Its **change signal is deferred** to `CommitAsync`, so a reader never wakes on events a rollback discards.
- Its **`$all` position is taken at commit**: the append returns `AppendResult.Position == null`, and an event
  read back inside the frame stands at `GlobalPosition.Start`. When the caller commits the transaction itself
  (rather than through `CommitAsync`), it calls `unitOfWork.PrepareAsync()` as the very last step before its
  commit and `CompleteAsync()` after it; a commit without `PrepareAsync` leaves the events out of `$all`, and
  `CompleteAsync` throws to say so. Nothing slow belongs between `PrepareAsync` and the commit: from there to the
  commit, other committing writers wait.
- Snapshot and checkpoint writes and the reads run on the frame's connection, so a read inside the frame sees
  the frame's own uncommitted writes — except in `$all`, which holds only events with a commit sequence.

A `NonRetryingExecutionStrategy` is forced on the enlisted context even when the app enabled
`EnableRetryOnFailure`, because a retrying strategy refuses a user-initiated transaction.

Under a message bus, the [`Dbosoft.EventStore.Rebus`](bus-and-modules.md#transactional-enlistment-under-rebus)
package bridges the frame onto Rebus's per-message transaction so a handler's append commits with the message.
