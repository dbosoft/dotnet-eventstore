# Concepts

## Streams and versions

An event store holds **streams**. A stream is an ordered sequence of events identified by a string
`streamId` (for example `lot-42`). Each event in a stream has a **stream version** — a 1-based counter that
increases by one per event. The version is how optimistic concurrency works.

You append with an **expected version**, and the append succeeds only if the stream is at that version:

```csharp
await store.AppendToStreamAsync(
    "lot-42",
    ExpectedVersion.NoStream,                       // the stream must not exist yet
    [new EventData(Guid.NewGuid(), "LotListed", payloadBytes)]);
```

`ExpectedVersion` has sentinels plus any concrete version:

| Value | Meaning |
| --- | --- |
| `ExpectedVersion.Any` | Append regardless of the current version. |
| `ExpectedVersion.NoStream` | The stream must not exist yet. |
| `ExpectedVersion.StreamExists` | The stream must already exist, at any version. |
| a number `n` | The stream must be exactly at version `n`. |

If the stream is not at the expected version, the append throws `WrongExpectedVersionException`, which
carries `StreamId`, `ExpectedVersion` and `ActualVersion` — the last one is what retry logic needs.

### Idempotency

Every event carries an `EventId`. An append is **batch-idempotent** keyed on the set of `EventId`s: replaying
a batch whose ids are all already stored is a no-op that returns the original outcome, a batch that only
partially overlaps stored events is rejected, and a duplicate id within one batch is rejected. This makes a
retried append (for example after a transient failure) safe. De-duplication holds only while the events are
retained — truncating a stream drops the records, so a later re-append of a truncated batch is treated as new.

## Domains

Events that belong together form a **domain** (a bounded context), mapped one-to-one to its own storage
unit — its own tables in EF Core, its own tables / partition-key prefix in Azure Tables, its own area
in-memory. A domain gives storage isolation and its own independent ordered log.

The consequence: **there is no ordering across domains.** Each domain has its own versions and its own global
`$all` sequence. A single cross-domain total order would need one shared physical store, which defeats the
partitioning. Order within a context is what applications need; order across unrelated contexts is not.

You register domains on the builder, and each is exposed as a keyed service:

```csharp
services.AddEventStore(es => es
    .UseSqlite(connectionString)
    .AddDomain("banking", d => d.AddEventTypesImplementing<IBankingEvent>())
    .AddDomain("catalog", d => d.AddEventTypesImplementing<ICatalogEvent>()));

// resolve a domain's store, keyed by domain name
var banking = serviceProvider.GetRequiredKeyedService<IEventStore>("banking");
```

Event types are registered per domain, in bulk via a marker/contract interface
(`AddEventTypesImplementing<IBankingEvent>()`) or one at a time (`AddEventType<MoneyDeposited>()`).

## The global sequence: `GlobalPosition`

Across a domain, every stored event also has a **`GlobalPosition`** — its place in the domain's global `$all`
order. It is an opaque, comparable, serializable token; the provider decides what is inside it (EF Core uses a
dense identity sequence, Azure Tables a forward tick). You never construct one except `GlobalPosition.Start`
and `GlobalPosition.End`; you get positions from stored events and hand them back to resume a read.

```csharp
public readonly record struct GlobalPosition(string Token) : IComparable<GlobalPosition>;
```

## The value types

The core knows only bytes plus a content type; typing lives in the [serialization layer](serialization.md).

```csharp
// What you append.
public sealed record EventData(
    Guid EventId,
    string EventType,
    ReadOnlyMemory<byte> Data,
    ReadOnlyMemory<byte>? Metadata = null,
    string ContentType = "application/json");

// What you read back.
public sealed record StoredEvent(
    string StreamId,
    long StreamVersion,
    GlobalPosition Position,
    Guid EventId,
    string EventType,
    ReadOnlyMemory<byte> Data,
    ReadOnlyMemory<byte>? Metadata,
    string ContentType,
    DateTimeOffset CreatedAt);

// What an append returns.
public sealed record AppendResult(long NextExpectedVersion, GlobalPosition? Position);
```

`GetStreamInfoAsync` returns a `StreamInfo` (current version, truncation boundary, deletion flag, metadata),
or `null` for a missing stream.

## The core interface

Every provider implements `IEventStore`, which extends the read-only `IEventReader`:

```csharp
public interface IEventReader
{
    IAsyncEnumerable<StoredEvent> ReadStreamAsync(
        string streamId, ReadDirection direction = ReadDirection.Forwards,
        long fromVersion = 0, long maxCount = long.MaxValue, CancellationToken ct = default);

    Task<StreamInfo?> GetStreamInfoAsync(string streamId, CancellationToken ct = default);
}

public interface IEventStore : IEventReader
{
    StoreCapabilities Capabilities { get; }

    Task<AppendResult> AppendToStreamAsync(
        string streamId, long expectedVersion,
        IReadOnlyList<EventData> events, CancellationToken ct = default);
}
```

Everything beyond appending and reading a stream — global reads, time navigation, truncation, deletion,
multi-stream atomic append, stream metadata — is an **optional interface** a provider implements only when its
backend supports it. See [Providers & capabilities](providers.md).

`TimeProvider` is used everywhere a timestamp is needed, so time-dependent behaviour (retention, safety
windows) is testable.
