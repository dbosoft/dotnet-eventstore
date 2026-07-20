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
| `StoreCapabilities.ExactCatchup` | ✅ | — | — |
| `StoreCapabilities.SnapshotHistory` | ✅ | ✅ | ✅ |

The in-memory store implements every capability and is the reference the [conformance suite](bus-and-modules.md#testing)
runs against. EF Core and Azure Tables do not declare `ExactCatchup` because their `$all` positions are
assigned before commit, so a catch-up read is complete only up to a bounded staleness window — the
subscription handles that for you (see [completeness](reading-and-subscriptions.md#completeness-and-exactcatchup)).
Azure Tables cannot do a cross-partition atomic append, so it does not offer `IMultiStreamAppend`.

Every provider passes the same conformance suite; a provider only runs the tests for what it declares.
