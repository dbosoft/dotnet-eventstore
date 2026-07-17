# Dbosoft.EventStore — Design & Plan

Status: draft, 2026-07-17. Based on a critical review of the event store in `SAPId`
(`src/core/src/SAPId.EventStore*`, ~1,300 LOC).

## 1. Goal and scope

A publicly usable NuGet library for event sourcing that is deliberately **not a product**: the
concept is theoretically simple and conceptually clear, and that clarity is the whole point
compared to the existing .NET stores. No foreign frameworks in the core.

**In scope:** streams, optimistic concurrency, snapshots, `$all`, catch-up subscriptions,
truncation/retention/deletion, serialization + upcasting, an aggregate repository, and multiple
storage providers (EF Core, Azure Table Storage).

**Out of scope:** projection host, command dispatch, read model runner, CQRS framework. We ship
the store and the building blocks, not the application around them.

**Target framework:** `net10.0`, single target.

## 2. What we keep from SAPId — and what we don't

Kept: the stream/version/position shape, payload and metadata separated, a snapshot table
alongside the event table, a unique index on `(StreamId, StreamVersion)`.

Rejected, with reasons:

| Existing | Problem |
|---|---|
| `IEventStore<TAggregate>` with marker classes | The type parameter carries no information — it is a table selector (`Set<EventEntry>("ResourceEvents")`). Consequence: no global ordering across the four tables. Table-per-aggregate is storage configuration, not a type system concern. |
| `MaxAsync` → check → `Add` | A TOCTOU race. Two transactions both read `max=5` and both write `6`. Only the unique index saves it, and that throws `DbUpdateException`, not `ConcurrencyException` — so the documented contract does not hold under load. |
| `expectedVersion` as a bare `int` | No sentinels for `Any` / `NoStream` / `StreamExists`. |
| `DeferCommit` as a public bool on the DbContext | A mutable flag on a shared object that switches another class's commit semantics is a trap under DI. The real need — caller-owned commits — belongs in an explicit transaction context (see D6), which is deferred past v1. |
| No `$all` read | `Position` exists on the record but cannot be read by. Read model rebuilds and catch-up cannot be built generically. |
| `GetSnapshotsByTypeAsync`, `ResourceSnapshotEntry.ResourceType`, `SapRoleSnapshotEntry.SystemId/Client` | The snapshot table is being used as a read model. Plus `ResourceTypeFromStreamId` with `IndexOf('-')` — an undocumented string contract. Domain knowledge that does not belong in a general library. |
| `ReadStreamAsync` without paging | Always materializes the entire stream. |
| Payload as `string` | Forces JSON and collides with ATS limits (64 KB per string property, 1 MB per entity). |
| No EventId | Rebus retries append duplicates. |
| `DateTime.UtcNow` inline | Not testable; `TimeProvider` is the standard. |
| One snapshot per stream (upsert) | No history, no "snapshot at version N". |

## 3. Key decisions

### D1 — Capabilities instead of a lowest common denominator

One core interface every provider implements, plus optional interfaces a provider declares. The
conformance suite only tests what is declared. Rationale: SQL and ATS are not equally capable.
Reducing the API to the ATS denominator would kill projections on SQL, and pretending both are
identical would be a lie.

Symmetry is explicitly a non-goal. Each provider uses the best mechanism its storage offers, and
the capability flags name the difference instead of hiding it.

### D2 — `GlobalPosition` is an opaque token, defined by the provider

```csharp
public readonly record struct GlobalPosition(string Token) : IComparable<GlobalPosition>
{
    public static GlobalPosition Start { get; }
    public static GlobalPosition End   { get; }
}
```

Comparable and serializable, so checkpoints round-trip. The provider decides what is inside:

- **EF Core:** the dense identity sequence.
- **ATS:** a forward-tick value (see §6).

A uniform time-based token for both providers was considered and **rejected**. On SQL it would be
a voluntary downgrade:

- The identity column already exists, already is the PK and already serves `$all` ordering.
  A time-ordered token needs a *second* composite index on `(CreatedAt, Position)`, maintained on
  every append — a permanent write tax on the hot path.
- It buys almost nothing: `CreatedAt` and `Position` are assigned in the same instant and are
  therefore near-perfectly correlated. The orderings are practically always identical.
- Where they *do* diverge — a clock step — the sequence is right and the clock is wrong. A
  sequence is monotonic by construction; a clock is not.
- It destroys gap detection (see D3), which is the mechanism that makes SQL exact in the first
  place.

What a uniform token would have bought: checkpoints portable across providers — theoretical,
since nobody migrates an event store from EF Core to ATS while keeping subscription checkpoints
instead of re-projecting. And "replay from timestamp T", which is real but is a rare admin
operation, not the hot path.

**Time navigation therefore becomes an opt-in capability instead**, so nobody pays for a feature
they do not use:

```csharp
Task<GlobalPosition> FindPositionAtAsync(DateTimeOffset timestamp, CancellationToken ct = default);
```

ATS answers it for free — the ticks *are* the key. EF Core answers it via an index on `CreatedAt`
that is only created when the feature is switched on.

### D3 — Subscriptions are eventually complete; how eventually differs by provider

This is not an ATS-only problem. On SQL an identity value is assigned at `INSERT` but only becomes
visible at `COMMIT`, so a subscriber polling `WHERE Position > checkpoint` skips events that were
assigned an earlier position and committed later. Both providers need a **watermark** — never read
up to a point somebody could still write below — plus **dedupe by EventId**. The mechanism differs:

**EF Core: gap detection over the dense sequence.** The subscriber sees 1, 2, 3, 5 and *knows*
that 4 is either still open or aborted. It waits, and after `GapTimeout` it moves on. The
watermark is the highest position below which no holes remain.

This works with plain table reads and **no elevated permissions** — which is why we do *not* use
the "oldest open transaction" approach (`sys.dm_tran_active_transactions`,
`pg_snapshot_xmin(pg_current_snapshot())`). That would be exact too, but it requires
`VIEW SERVER STATE` or its equivalent, and "only works with elevated server rights" disqualifies a
public library. Prior art for gap detection: SqlStreamStore, Marten.

A non-dense sequence cannot support this at all: you can never distinguish "something is missing"
from "nothing was ever there". This is the decisive argument for the dense sequence on SQL.

**ATS: a safety window plus dedupe.** No dense sequence is available (§6), so the subscriber only
reads up to `now - safetyWindow`.

The window is **not** sized for clock drift. NTP-synced servers sit in the low single-digit
milliseconds, and Azure VMs with hypervisor time sync below that — drift is a secondary concern.
The window is sized for **Δ: the span between a writer stamping its tick and the index entry
becoming visible.** That is latency, retries and above all ATS throttling — a 429 plus exponential
backoff turns Δ into a second while the clock is perfect.

Δ is kept small by construction: **stamp the tick as late as possible, and re-stamp on retry**, so
Δ collapses to the in-flight time of the one successful attempt rather than the whole retry
cascade. The tick then means "when the index entry landed", not "when the event was appended" —
which is correct, because `$all` order is defined by the index, not by the stream.

⇒ EF Core declares `ExactSubscriptions`; ATS does not.

### D4 — The core only knows bytes

`ReadOnlyMemory<byte>` plus a `ContentType`. Serialization, type registry, upcasting and
aggregates live in optional packages above it. If you only want a log, you pay nothing for the
rest.

The serializer is a pluggable extension slot: `IEventSerializer` (used for both events and
snapshots) defaults to System.Text.Json but is swapped via the builder — `UseSerializer(...)` for a
custom implementation, or `UseSystemTextJson(o => …)` to adjust the JSON rules (naming policy,
converters, unmapped-member handling) so fields can be mapped differently without writing one.

### D5 — Snapshots are their own interface, and disposable

`ISnapshotStore`, not part of `IEventStore`. Keyed by `(StreamId, StreamVersion)`, so history is
possible. No domain-specific query methods — that is what projections are for.

A snapshot is a memoized fold of the stream, never the source of truth: it can always be discarded
and rebuilt by replaying. Schema evolution of the snapshot state is handled **not** by an upcaster
chain (as events are) but by two properties:

- **Schemaless/tolerant deserialization** — the serializer ignores unknown members and defaults
  missing ones (System.Text.Json does this by default; `UseSystemTextJson` tunes it), so additive
  changes to the snapshot state just work.
- **Best-effort load** — if a snapshot still fails to deserialize (a breaking change, or
  corruption), the repository discards it and rebuilds from the event stream instead of failing the
  load. A snapshot can never break reading.

This is also why truncation (§7) requires a covering snapshot: the snapshot is what lets old events
be dropped while the state stays reconstructable.

### D6 — Caller-owned transactions are a future unit-of-work context, not a commit-mode flag

The underlying need in SAPId is real: when the caller wants to batch the store's writes with the
rest of its work and own the commit. But the answer is **not** a `Deferred` commit mode — a flag
that silently moves conflict detection to the caller's commit and leaves `AppendResult.Position`
unassigned is exactly the kind of hidden-semantics trap the SAPId `DeferCommit` bool already is.

The right shape is an explicit **transaction / unit-of-work context** the caller opens and commits,
with the store enlisting in it — so the participation is visible in the call, not toggled on a
shared object. That is deliberately **out of scope for v1**: the store commits eagerly, and the
UoW context is designed and added later.

### D7 — Deletion means actual deletion

Truncate-before and delete-stream physically remove data. No crypto-shredding: if history before
point X is no longer needed, there is no reason to keep it. Details in §7.

## 4. API sketch (Abstractions)

Values and identity:

```csharp
public static class ExpectedVersion
{
    /// <summary>Append regardless of the stream's current version.</summary>
    public const long Any = -2;

    /// <summary>The stream must not exist yet.</summary>
    public const long NoStream = -1;

    /// <summary>The stream must already exist, at any version.</summary>
    public const long StreamExists = -4;
}

public readonly record struct GlobalPosition(string Token) : IComparable<GlobalPosition>;

public sealed record EventData(
    Guid EventId,
    string EventType,
    ReadOnlyMemory<byte> Data,
    ReadOnlyMemory<byte>? Metadata = null,
    string ContentType = "application/json");

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
```

Capabilities. Each flag states a guarantee a caller can rely on, which is why they carry doc
comments — the name alone does not say what you get:

```csharp
[Flags]
public enum StoreCapabilities
{
    None = 0,

    /// <summary>Events across all streams can be read in a single ordered sequence.</summary>
    AllStreamReads = 1 << 0,

    /// <summary>Subscriptions can follow that sequence from a checkpoint.</summary>
    Subscriptions = 1 << 1,

    /// <summary>
    /// A subscription never silently skips an event: once it reports a position, every event
    /// ordered below that position has been delivered. Without this flag a subscription is
    /// complete only eventually, and handlers must tolerate redelivery and reordering within
    /// the store's safety window.
    /// </summary>
    ExactSubscriptions = 1 << 2,

    /// <summary>Events for several streams can be appended in one atomic operation.</summary>
    MultiStreamAtomicAppend = 1 << 3,

    /// <summary>A position can be located from a wall-clock timestamp.</summary>
    TimeNavigation = 1 << 4,

    /// <summary>Streams carry readable and writable metadata.</summary>
    StreamMetadata = 1 << 5,

    /// <summary>Events below a version can be permanently removed from a stream.</summary>
    Truncation = 1 << 6,

    /// <summary>Whole streams can be deleted.</summary>
    StreamDeletion = 1 << 7,

    /// <summary>More than the latest snapshot per stream is retained and readable.</summary>
    SnapshotHistory = 1 << 8,
}
```

The core interface, implemented by every provider:

```csharp
public interface IEventStore
{
    StoreCapabilities Capabilities { get; }

    Task<AppendResult> AppendToStreamAsync(
        string streamId, long expectedVersion,
        IReadOnlyList<EventData> events, CancellationToken ct = default);

    IAsyncEnumerable<StoredEvent> ReadStreamAsync(
        string streamId, ReadDirection direction = ReadDirection.Forwards,
        long fromVersion = 0, long maxCount = long.MaxValue,
        CancellationToken ct = default);

    Task<StreamInfo?> GetStreamInfoAsync(string streamId, CancellationToken ct = default);
}
```

The optional interfaces, each gated by its capability flag:

```csharp
public interface IAllStreamReader
{
    IAsyncEnumerable<StoredEvent> ReadAllAsync(
        GlobalPosition from, ReadDirection direction = ReadDirection.Forwards,
        long maxCount = long.MaxValue, CancellationToken ct = default);
}

public interface ITimeNavigable
{
    Task<GlobalPosition> FindPositionAtAsync(DateTimeOffset timestamp, CancellationToken ct = default);
}

public interface ISubscriptionSource
{
    IAsyncDisposable SubscribeToAll(GlobalPosition checkpoint, IEventHandler handler,
        SubscriptionOptions? options = null);
}

public interface ISnapshotStore
{
    Task SaveAsync(string streamId, long streamVersion, ReadOnlyMemory<byte> payload, ...);
    Task<StoredSnapshot?> LoadLatestAsync(string streamId, ...);
    Task<StoredSnapshot?> LoadAtOrBeforeAsync(string streamId, long version, ...);
    Task PruneAsync(string streamId, int keep, ...);
}

public interface IStreamMaintenance
{
    Task TruncateStreamAsync(string streamId, long beforeVersion, bool force = false, ...);
    Task DeleteStreamAsync(string streamId, DeleteMode mode, ...);
    Task<ScavengeReport> ScavengeAsync(ScavengeOptions? options = null, ...);
}
```

`SubscriptionOptions` carries whatever knob the provider needs to bound its watermark; the
provider documents which one it honours.

`WrongExpectedVersionException` carries `StreamId`, `ExpectedVersion` and **`ActualVersion`** — the
last one is missing in SAPId and is what retry logic needs.

## 5. Provider: EF Core

**Schema:**

- `Streams` — `StreamId` (PK), `Version`, `TruncatedBefore`, `IsDeleted`, `MetadataJson`.
- `Events` — `Position` (identity, PK), `StreamId`, `StreamVersion`, `EventId`, `EventType`,
  `Data` (varbinary), `Metadata`, `ContentType`, `CreatedAt`.
  Unique `(StreamId, StreamVersion)`, unique `(StreamId, EventId)` for idempotency.
  **No composite time index** — `$all` rides the PK (D2). An index on `CreatedAt` is created *only*
  when `TimeNavigation` is enabled.
- `Snapshots` — PK `(StreamId, StreamVersion)`.

**Append without the race (fixes the #2 critique):** no `MaxAsync` pre-read. Instead:

```sql
UPDATE Streams SET Version = @new WHERE StreamId = @id AND Version = @expected
```

Zero rows affected means a conflict — deterministic, no exception sniffing, works on any relational
database. The event rows follow in the same transaction. The `Streams` head deliberately mirrors
the ATS `HEAD` entity (§6): one concept, two providers.

**Commits** are eager in v1: each append opens and commits its own transaction. Enlisting in a
caller-owned transaction is the future unit-of-work context (D6), not part of the first cut.

**`$all`:** `WHERE Position > @p ORDER BY Position` on the PK index, with gap detection per D3.
Declares `ExactSubscriptions`.

Tested against SQLite, SQL Server and PostgreSQL (Testcontainers).

## 6. Provider: Azure Table Storage

**Core (clean and atomic):**
- PartitionKey = `StreamId`, RowKey = `"E" + version.ToString("D19")`.
- A `HEAD` entity in the same partition holds the current version.
- Append = **a single Entity Group Transaction**: check-and-bump `HEAD` via ETag plus insert the
  event rows. All in one partition, therefore atomic. Limit: 100 entities per EGT, so at most 99
  events per append.
- Reading a stream = a range query on RowKey prefix `"E"`. One partition scan, fast and cheap.

**Snapshots:** separate table, PK = `StreamId`, RowKey = `(long.MaxValue - version).ToString("D19")`,
so the newest snapshot is the first row.

**`$all` via the log tail pattern:**

- **Forward** ticks in the RowKey, not reverse: ATS only sorts ascending, and a catch-up subscriber
  wants oldest-first from a checkpoint. Reverse ticks (`MaxValue.Ticks - now`) stay for "the N most
  recent" / tailing the head.
- **Stamp the tick late and re-stamp on retry**, to keep Δ small (D3).
- **Sharded buckets, not a single `$all` key.** PartitionKey combines a time bucket with a shard
  (e.g. `"2026-07-17T14/03"`); the subscriber merges N tick-ordered buckets on read. The MS docs
  warn explicitly about the log tail pattern: *"be careful not to create hot spot partitions"*.
  Cost is a merge loop in the reader.
- **Atomicity:** the index entry necessarily lives in a different partition than the event, so no
  EGT spans both. The answer is Microsoft's own *eventually consistent transactions pattern*: the
  stream partition is the source of truth, the `$all` entry is written idempotently afterwards
  (insert-or-replace keyed by EventId), and a repair sweep closes the crash window. The docs are
  explicit: *"This solution does not provide for transaction isolation… The data will be consistent
  eventually."*

**Alternatives considered and rejected:**

| Alternative | Why not |
|---|---|
| The ATS `Timestamp` system property | Tempting — assigned by the *service*, so one clock for everyone, no client clocks involved. But it is not part of the key: ordering or seeking by it means a full table scan, and MS says not to rely on it for ordering. Usable as a cross-check, not as an index. |
| Sequencer entity (a real dense sequence) | Every append in the whole store funnels through one counter entity in one partition; ETag conflicts under concurrency produce retry loops that collapse throughput well below the partition ceiling. Burned numbers would *not* be the problem — they are the exact analogue of an aborted transaction on SQL, which gap detection clears via timeout. The contention is. |
| Hi/Lo number blocks | Fixes the contention, destroys the property it exists for: writer A holds 1–100, B holds 101–200, B writes 101 before A writes 1 — dense but no longer commit-ordered, and gap detection stalls on numbers nobody has written yet. |
| Queue / Event Hubs as the ordering channel | Pulls in an external service dependency we explicitly do not want, and only solves the *live* case — a historical `ReadAllAsync(from: T)` still needs the table index, so you end up with both. |
| No `$all` on ATS at all | The honest fallback: per-stream subscriptions only. Costs cross-stream read models on ATS entirely. Kept in reserve if the index proves unworkable. |

⇒ ATS declares `AllStreamReads | Subscriptions | TimeNavigation`, but **not**
`MultiStreamAtomicAppend` and **not** `ExactSubscriptions`.

**Payloads over 64 KB:** chunked across several properties (`Data0..DataN`) up to the 1 MB entity
limit. Beyond that, v1 throws a clear exception; blob overflow is an optional later addition.

Tested against Azurite.

## 7. Deletion, truncation, retention

The core stays append-only; everything destructive is a separately registered capability.

**Truncate-before** (`IStreamMaintenance.TruncateStreamAsync(streamId, beforeVersion)`) sets
`TruncatedBefore` in the stream metadata and removes the events below it **physically**
(immediately or at scavenge time, depending on the provider). Version numbers stay monotonic —
nothing is renumbered, the versions below the boundary simply no longer exist.

**The invariant the library enforces:** truncation is **refused** unless a snapshot with
`Version >= beforeVersion - 1` exists, because otherwise the stream state can no longer be
reconstructed. This is exactly the "cut off behind snapshots" case. `force: true` overrides it
deliberately and loudly.

**Delete-stream:** `DeleteMode.Soft` (tombstone, the stream can be recreated) or `DeleteMode.Hard`
(data gone, the StreamId permanently burned). This covers "forget a history": one stream per
subject, hard delete, done.

**Retention policies** in the stream metadata: `MaxCount`, `MaxAge`. The scavenger applies them.

**Not in v1:** redacting individual events in the middle of a stream. Truncation (a prefix) and
deletion (the whole thing) both preserve the shape; a hole in the middle does not. Can be added
later if a real need shows up.

## 8. Testing — the heart of the agnosticism

`Dbosoft.EventStore.Testing` as a **public** package: an abstract `EventStoreConformanceTests`
suite that each provider inherits via a fixture. Capability-gated — a provider that does not
declare `Truncation` skips those tests instead of faking them. Public so that third-party providers
can validate their implementation too.

Stack: **xunit**, **AwesomeAssertions**, **NSubstitute**.

Beyond conformance: concurrency stress tests with parallel appends (exactly what the SAPId store
fails at), subscription tests with artificial commit reordering, and `TimeProvider`-driven
retention tests.

The conformance suite is built **together with the abstractions**, not after them: it *is* the
contract.

## 9. Package layout

The contract stack is layered `Primitives → Abstractions → Core`, all sharing the base namespace
`Dbosoft.EventStore` (assembly name ≠ namespace, the standard MS library pattern). The dividing
line: no interface in Primitives, no bare data type in Abstractions.

A provider's store implementation needs only Abstractions, but each provider package also ships a
`UseXxx(this EventStoreBuilder)` DI extension so registration is a single fluent chain
(`AddEventStore(b => b.UseInMemory().AddEventType<…>()…)`, the EF-Core-style pattern). Since
`EventStoreBuilder` lives in Core, the provider packages reference Core. A third-party provider that
ships no DI sugar could reference Abstractions alone.

| Package | Contents | Dependencies |
|---|---|---|
| `Dbosoft.EventStore.Primitives` | value types, records, enums, exceptions, capabilities | none |
| `Dbosoft.EventStore.Abstractions` | interfaces | Primitives |
| `Dbosoft.EventStore` | serializer, event type registry, upcasting, aggregate repository, snapshot policy, DI | Abstractions |
| `Dbosoft.EventStore.EfCore` | EF Core provider | Core, EFCore.Relational |
| `Dbosoft.EventStore.AzureTables` | ATS provider | Core, Azure.Data.Tables |
| `Dbosoft.EventStore.InMemory` | reference / test implementation | Core |
| `Dbosoft.EventStore.Testing` | conformance suite | Abstractions, xunit, AwesomeAssertions |

## 10. Phased plan

1. **Abstractions + InMemory + conformance suite.** The contract is defined by the tests. InMemory
   is the reference implementation the suite proves itself against.
2. **EF Core provider** — `Streams` head table, race-free append, paging. Against SQLite +
   PostgreSQL + SQL Server.
3. **Core layer** — serializer, type registry, upcasting, aggregate repository, snapshot policy.
4. **Truncation / retention / scavenge / delete**, including the snapshot invariant from §7.
5. **`$all` + catch-up subscriptions** — gap detection on SQL, the exact-vs-bounded split from D3.
6. **Azure Table Storage provider** — EGT append, sharded log-tail `$all`, chunking. Against
   Azurite. Deliberately after phase 5: only once the subscription contract stands does it show
   whether the opaque token holds up.
7. **Docs, sample, NuGet, CI.**
8. **Migrate SAPId.** The actual proof: if the four SAPId aggregates run on the library without
   marker classes and without `DeferCommit`, the design is right.

## 11. Open points

- **Multi-stream atomic append:** trivial on EF Core, impossible on ATS. Is the capability worth it
  if only one provider satisfies it? Leaning yes, because process managers need it.
- **Blob overflow for ATS payloads over 1 MB:** later, if needed.
- **`SAPId` targets net8.0** — the migration in phase 8 presupposes a move to net10.0.

Decided and closed: no LanguageExt integration; the core stays idiomatic .NET.
