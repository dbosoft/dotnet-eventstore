# Dbosoft.EventStore — Design & Plan

Status: draft, 2026-07-17. Based on a critical review of the event store in `SAPId`
(`src/core/src/SAPId.EventStore*`, ~1,300 LOC).

## 1. Goal and scope

A publicly usable NuGet library for event sourcing that is deliberately **not a product**: the
concept is theoretically simple and conceptually clear, and that clarity is the whole point
compared to the existing .NET stores. No foreign frameworks in the core.

**In scope:** streams, optimistic concurrency, snapshots, `$all` catch-up reads,
truncation/retention/deletion, serialization + upcasting, an aggregate repository, a per-domain
event materializer, a checkpoint store, and multiple storage providers (EF Core, Azure Table
Storage).

**Out of scope — and one line matters most: we do not build a message bus, we presuppose one.**
Live delivery, fan-out, retries, dead-lettering and subscriptions are the bus's job (Rebus in our
world), never the store's. The store is the durable write side plus a *catch-up read* (`$all`) to
feed and replay onto the bus. Also out: projection host, command dispatch, read-model runner, CQRS
framework. We ship the store and the building blocks, not the application around them, and not the
transport.

**Target framework:** `net10.0`, single target.

## 2. What we keep from SAPId — and what we don't

Kept: the stream/version/position shape, payload and metadata separated, a snapshot table
alongside the event table, a unique index on `(StreamId, StreamVersion)`.

Rejected, with reasons:

| Existing | Problem |
|---|---|
| `IEventStore<TAggregate>` with marker classes | The type parameter carries no information — it is a table selector (`Set<EventEntry>("ResourceEvents")`). The grouping instinct was right (it becomes **domains**, D0), but encoding it in the type system is not: table-per-context is storage configuration, and the marker types gave no ordering, no `$all`, no contract. |
| `MaxAsync` → check → `Add` | A TOCTOU race. Two transactions both read `max=5` and both write `6`. Only the unique index saves it, and that throws `DbUpdateException`, not `ConcurrencyException` — so the documented contract does not hold under load. |
| `expectedVersion` as a bare `int` | No sentinels for `Any` / `NoStream` / `StreamExists`. |
| `DeferCommit` as a public bool on the DbContext | A mutable flag on a shared object that switches another class's commit semantics is a trap under DI. The real need — caller-owned commits — belongs in an explicit transaction context (see D6), which is deferred past v1. |
| No `$all` read | `Position` exists on the record but cannot be read by. Read model rebuilds and catch-up cannot be built generically. |
| `GetSnapshotsByTypeAsync`, `ResourceSnapshotEntry.ResourceType`, `SapRoleSnapshotEntry.SystemId/Client` | The snapshot table is being used as a read model. Plus `ResourceTypeFromStreamId` with `IndexOf('-')` — an undocumented string contract. Domain knowledge that does not belong in a general library. |
| `ReadStreamAsync` without paging | Always materializes the entire stream. |
| Payload as `string` | Forces JSON and collides with ATS limits (64 KB per string property, 1 MB per entity). |
| No EventId | Rebus retries append duplicates. Fixed with an `EventId` assigned **at `Raise`** (stable across save retries, so a retried save is an idempotent no-op, not a duplicate), and **batch-scoped** idempotency keyed on the `EventId` set (not payload contents): a batch whose ids are all already stored succeeds returning the original outcome, a partial overlap is an error, in-batch duplicates are an error. Dedup holds only while the events are retained (truncation drops the record). |
| `DateTime.UtcNow` inline | Not testable; `TimeProvider` is the standard. |
| One snapshot per stream (upsert) | No history, no "snapshot at version N". |

## 3. Key decisions

### D0 — The store is partitioned into domains, not flat

Events that belong together form a **domain** (a bounded context), mapped 1:1 to its own storage
unit: its own table(s) in EF Core, its own table or partition-key prefix in ATS, its own area in
InMemory. This is the storage hint SAPId expressed as table-per-aggregate — the *concept* was right;
only the mechanism (marker types as table selectors) was wrong. Domains give storage isolation and
per-context performance.

**The consequence that shapes everything else:** with separate storage per domain there is **no
ordering across domains**. Each domain is an independent ordered log — its own version and
`GlobalPosition` sequence, its own `$all` catch-up read. A single cross-domain total order
would need one shared physical store (defeating the partitioning) or a merge layer with the very
sequencing problem D2 describes. Order within a context is what applications actually need; order
across unrelated contexts is not.

Access is per domain through `IEventStoreProvider.GetStore(domain)`, and each domain is also exposed
as a keyed `IEventStore`. One provider (chosen once) backs all domains with separate units. Event
types are registered per domain, in bulk via a marker/contract interface
(`AddEventTypesImplementing<IBankingEvent>()`) or individually. A default domain keeps single-context
apps simple.

### D1 — Capabilities instead of a lowest common denominator

One core interface every provider implements, plus optional interfaces a provider declares. The
conformance suite only tests what is declared. Rationale: SQL and ATS are not equally capable.
Reducing the API to the ATS denominator would kill projections on SQL, and pretending both are
identical would be a lie.

Symmetry is explicitly a non-goal. Each provider uses the best mechanism its storage offers, and the
capability model names the difference instead of hiding it — along **two axes**:

- **Whether an operation is *available*** is a structural fact carried by the **type system**: a
  provider supports global reads, time navigation, truncation, deletion, redaction, multi-stream atomic
  append or stream metadata iff it implements `IAllStreamReader`, `ITimeNavigable`, `ITruncatable`,
  `IStreamDeletable`, `IRedactable`, `IMultiStreamAppend`, `IStreamMetadataStore`. A caller branches with
  an `is`-cast; a partially-supported operation throws `NotSupportedByProviderException`. No flag restates
  what the type already says.
- **How an available operation *behaves*** is what the `StoreCapabilities` flags carry — the guarantees
  a type cannot express (is a catch-up read exact? is more than the latest snapshot kept?). A flag
  earns its place only where a consumer branches on it: a faster path, a costlier-but-correct path, or
  fail-fast.

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

Both providers implement it unconditionally, today via a scan filtered on `CreatedAt` — ATS enumerates
the events table (there the tick is *not* the row key, so it is not "free"), EF Core runs an unindexed
`WHERE CreatedAt < t`. A `CreatedAt` index (ATS: an index-backed lookup; EF Core: an opt-in column
index) is a planned optimization, not yet built — so time navigation is correct but not cheap.

### D3 — Catch-up reads, not subscriptions; delivery is the bus's job

The store does **not** deliver events. It offers a *catch-up read* of `$all`
(`IAllStreamReader.ReadAllAsync` from a position), and that is what a read-model rebuild or a
bus relay uses. There is no `ISubscriptionSource`, no in-process handler contract, no live push —
those belong to the presupposed bus. Consumers own their position via `ICheckpointStore` and
de-duplicate by `EventId`; delivery to consumers is therefore at-least-once, and exactly-once
effects come from persisting the checkpoint transactionally with the read model.

What remains a real, provider-dependent property is whether a *catch-up read* is **complete** — the
capability `ExactCatchup`. It is not an ATS-only problem. On SQL an identity value is assigned at
`INSERT` but only becomes visible at `COMMIT`, so a reader taking `WHERE Position > checkpoint` and
advancing to the max position can skip an event that was assigned an earlier position and commits
later. The reader needs a **watermark** — never advance past a point somebody could still write
below.

> **Implementation status (v1): a time-based safety window is built; the gap detection below is not.**
> The realized mechanism is a **safety window** — deliver/index only events older than a window Δ, so an
> event whose position was allocated but whose commit lands late is not passed before its lower siblings
> commit. It lives in the `AzureTablesIndexProjector` (holds fresh events out of the `$all` index) and in
> the core `EventStoreSubscription` (holds fresh events on any non-`ExactCatchup` store, e.g. EF Core).
> This closes the miss **provided no append stays uncommitted longer than Δ** — the one honesty caveat on
> the flag (below). Not yet built: the lower-latency **gap detection** below, which waits only on an
> actual hole instead of paying Δ on every read, and which needs a dense per-domain sequence EF Core does
> not yet have. So the miss is fixed at the cost of Δ staleness; removing that cost is the future work.

The mechanism differs by provider:

**EF Core: gap detection over the dense sequence.** The reader sees 1, 2, 3, 5 and *knows*
that 4 is either still open or aborted. It waits, and after a gap timeout it moves on. The
watermark is the highest position below which no *transient* holes remain — historical holes from
aborted transactions or scavenged (§7) positions are below a settled horizon and are skipped
without waiting, so a rebuild over a long, scavenged log does not stall.

This works with plain table reads and **no elevated permissions** — which is why we do *not* use
the "oldest open transaction" approach (`sys.dm_tran_active_transactions`,
`pg_snapshot_xmin(pg_current_snapshot())`). That would be exact too, but it requires
`VIEW SERVER STATE` or its equivalent, and "only works with elevated server rights" disqualifies a
public library. Prior art for gap detection: SqlStreamStore, Marten.

A non-dense sequence cannot support this at all: you can never distinguish "something is missing"
from "nothing was ever there". This is the decisive argument for the dense sequence on SQL.

**ATS: a safety window plus dedupe.** No dense sequence is available (§6), so the reader only
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

One honesty caveat on the flag: gap detection abandons a hole after a bounded timeout, so
`ExactCatchup` holds *provided no append transaction outlives that timeout*. The provider either
bounds append duration below it or documents the condition — a silently conditional guarantee is
exactly what D1's capability honesty exists to prevent.

⇒ Gap detection needs a **dense per-domain sequence**. The in-memory reference provider has one (a
separate store instance per domain, so its sequence is gap-free) and declares `ExactCatchup`. The
shipped EF Core layout does **not**: domains are an indexed discriminator over one `es_events` table
with one shared identity column (§5), so a single domain's positions are sparse — the gaps are other
domains' events, indistinguishable from missing ones. EF Core therefore does **not** declare
`ExactCatchup` in v1; its raw catch-up reads are ordered but not complete under concurrent out-of-order
commits, so a consumer must take the loss into account — which the core subscription does with the
safety window (status note above), delivering only events older than Δ so none is skipped. That buys
completeness under bounded commit latency without the flag; earning the flag itself (no Δ staleness)
needs density (per-domain sequences, or table-per-domain) plus gap detection, left as a later option.
ATS never had a dense sequence and does not declare it either.

### D4 — The core only knows bytes

`ReadOnlyMemory<byte>` plus a `ContentType`. Serialization, type registry, upcasting and
aggregates live in optional packages above it. If you only want a log, you pay nothing for the
rest.

The serializer is a pluggable extension slot: `IEventSerializer` (used for both events and
snapshots) defaults to System.Text.Json but is swapped via the builder — `UseSerializer(...)` for a
custom implementation, or `UseSystemTextJson(o => …)` to adjust the JSON rules (naming policy,
converters, unmapped-member handling) so fields can be mapped differently without writing one.

**Why JSON is the default (and stays so):** the event log is the source of truth and lives for
years, so readability is a feature — inspecting a row, auditing, ops tooling, forensics — and a
text format keeps the durable log portable across languages and runtime versions. Binary formats
(MemoryPack especially) trade that away; EventStoreDB, Marten and Axon all default to JSON for the
same reason.

Speed is addressed two ways without changing the default:

- **Source-generated JSON** — `UseSystemTextJson(MyJsonContext.Default)` runs System.Text.Json
  reflection-free via a `JsonSerializerContext`: faster and AOT/trim-safe, still readable JSON. This
  is the primary answer to "reflection is slow", since it keeps the format.
- **A different format** — `UseSerializer(...)` swaps in MessagePack/Protobuf/etc. (envisioned as
  separate optional adapter packages, kept out of the core). Because serialization is per the
  builder, a hot domain could use a binary format while the rest stay JSON.

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
  load. A snapshot can never break reading — **except past a truncation boundary** (below).

**The one place best-effort is wrong: after truncation.** Truncation (§7) drops the events below a
boundary, so for that prefix the covering snapshot stops being disposable derived data and *becomes
the source of truth*. If it is then unusable (a breaking schema change, corruption), replaying the
surviving suffix from an empty state would fold a silently wrong aggregate. So `LoadAsync` consults
`StreamInfo.TruncatedBefore` and **fails loudly** with `SnapshotRequiredException` when it cannot
obtain a usable snapshot at or after the boundary, rather than returning corrupt state. Truncation
requiring a covering snapshot (§7) protects the *write* side of this; the load-time check protects
the *read* side.

**Known follow-up — a snapshot epoch.** A new or changed upcaster changes fold semantics, so a
pre-existing snapshot embeds a stale fold; tolerant deserialization does not catch *semantic*
staleness, so it would be served silently. The conventional fix is a snapshot epoch/version stamp
that, when bumped, invalidates all snapshots (they rebuild from events). Harmless while the events
below the snapshot still exist; combined with truncation it is permanent. Not yet implemented —
tracked as the next snapshot-layer decision.

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

Capabilities are **behavioural guarantees only** — availability lives in the interfaces (D1), so the
flag set is small. Each flag states a guarantee a consumer branches on, which is why they carry doc
comments — the name alone does not say what you get:

```csharp
[Flags]
public enum StoreCapabilities
{
    None = 0,

    /// <summary>
    /// A catch-up read of the global sequence is exact: reading forward from a position never silently
    /// skips an event that commits below the point already returned. Without this flag a catch-up is
    /// complete only up to a bounded staleness window, so a subscription must re-read and de-dupe by
    /// EventId. (There is no subscription capability — delivery is the bus's job, D3.)
    /// </summary>
    ExactCatchup = 1 << 0,

    /// <summary>More than the latest snapshot per stream is retained and readable.</summary>
    SnapshotHistory = 1 << 1,
}
```

Only two flags remain — the behavioural guarantees no interface can express. The consumer of
`ExactCatchup` is the core `EventStoreSubscription`: with the flag it forwards each event and advances its
checkpoint; without it, it de-duplicates by `EventId` so re-reads are idempotent — the "faster path vs.
costlier-but-correct path" the flags exist for. `SnapshotHistory` is consumed by the aggregate
repository's fall-back-through-older-snapshots loop.

Everything else is an **availability interface**, not a flag — a provider implements it iff its backend
supports it:

- **`IMultiStreamAppend`** — one `AppendToStreamsAsync` that commits several streams all-or-nothing. EF
  Core does it in one transaction and the in-memory store under its single lock; partition-per-stream ATS
  cannot, so it does not implement it. Its consumer is `IAggregateRepository.SaveAllAsync`, which persists
  several aggregates atomically and throws `NotSupportedByProviderException` on a store that lacks it
  rather than silently degrading to a non-atomic loop.
- **`IStreamMetadataStore`** — per-stream `StreamMetadata` (a retention policy — max event count, max age —
  plus custom key/values), stored on the stream head. Its consumer is the retention pass of `ScavengeAsync`:
  with retention enabled it force-truncates each stream to its policy (emitting the truncation marker), so
  every provider that has metadata also enforces it. All three provider families implement it.

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

// Catch-up reads feed a read-model rebuild or a bus relay; a checkpoint remembers how far.
public interface ICheckpointStore
{
    Task<GlobalPosition?> GetAsync(string name, CancellationToken ct = default);
    Task SetAsync(string name, GlobalPosition position, CancellationToken ct = default);
}

// The read-side / bus-bridge primitive: map a domain's events between bytes and typed objects,
// both directions. Resolved from DI keyed by domain. The store hands this out; it never delivers.
public interface IEventMaterializer
{
    object Materialize(StoredEvent stored);                                   // deserialize + upcast
    bool TryMaterialize(StoredEvent stored, out object? @event);              // false for unknown types
    EventData Dematerialize(object @event, Guid? eventId = null, ReadOnlyMemory<byte>? metadata = null);
}

public interface ISnapshotStore
{
    Task SaveAsync(string streamId, long streamVersion, ReadOnlyMemory<byte> payload, ...);
    Task<StoredSnapshot?> LoadLatestAsync(string streamId, ...);
    Task<StoredSnapshot?> LoadAtOrBeforeAsync(string streamId, long version, ...);
    Task PruneAsync(string streamId, int keep, ...);
}

// Destructive operations are split into role interfaces, so a provider advertises exactly what it
// supports by which it implements; IStreamMaintenance is the composite of all four.
public interface ITruncatable    { Task TruncateStreamAsync(string streamId, long beforeVersion, bool force = false, ...); }
public interface IStreamDeletable { Task DeleteStreamAsync(string streamId, DeleteMode mode, ...); }
public interface IRedactable      { Task RedactEventAsync(string streamId, long version, ReadOnlyMemory<byte> newData, ...); }
public interface IScavengeable    { Task<ScavengeReport> ScavengeAsync(ScavengeOptions? options = null, ...); }

public interface IStreamMaintenance : ITruncatable, IStreamDeletable, IRedactable, IScavengeable;
```

A catch-up reader supplies whatever knob the provider needs to bound its watermark (gap timeout on
SQL, safety window on ATS); the provider documents which one it honours.

`WrongExpectedVersionException` carries `StreamId`, `ExpectedVersion` and **`ActualVersion`** — the
last one is missing in SAPId and is what retry logic needs.

## 5. Provider: EF Core

**Schema:**

- `Streams` — `StreamId` (PK), `Version`, `TruncatedBefore`, `IsDeleted`, `MetadataJson`.
- `Events` — `Position` (identity, PK), `StreamId`, `StreamVersion`, `EventId`, `EventType`,
  `Data` (varbinary), `Metadata`, `ContentType`, `CreatedAt`.
  Unique `(StreamId, StreamVersion)`, unique `(StreamId, EventId)` for idempotency.
  **No composite time index** — `$all` rides the PK (D2). There is no `CreatedAt` index yet, so time
  navigation (`FindPositionAtAsync`) is an unindexed scan; a `CreatedAt` index is a planned optimization.
- `Snapshots` — PK `(StreamId, StreamVersion)`.

**Append without a pre-read race:** no `MaxAsync`. Two race-free schemes, composable:

- **Default (any relational database):** the unique index `(Domain, StreamId, StreamVersion)` makes a
  lost race a write-time constraint violation. The provider does not sniff provider-specific error
  codes — on a failed `SaveChanges` it re-reads and classifies by the resulting state: the same batch
  now fully present is an idempotent replay, an advanced head is a version conflict, and an unchanged
  head is a genuine failure surfaced unmasked. With `ExpectedVersion.Any` (or a satisfied
  `StreamExists`) a version conflict is retried against the new head rather than reported.
- **Optional refinement — `UseVersionConcurrencyToken`:** map the head `Version` as a concurrency
  token, so the head update carries `WHERE Version = @expected` and a lost race surfaces as a clean
  `DbUpdateConcurrencyException`. Tidier on databases that support value concurrency tokens; the
  unique index already makes append race-free without it.

The event rows and the head update commit in one transaction either way. The `Streams` head
deliberately mirrors the ATS `HEAD` entity (§6): one concept, two providers.

**Commits** are eager in v1: each append opens and commits its own transaction. Enlisting in a
caller-owned transaction is the future unit-of-work context (D6), not part of the first cut.

**`$all`:** `WHERE Domain = @d AND Position > @p ORDER BY Position`. Because domains are an indexed
discriminator over one `es_events` table with one shared identity column, a single domain's positions
are **not dense** — the gaps are other domains' events. Gap-detection-based `ExactCatchup` therefore
does not apply to this layout: EF Core implements the global-read, time-navigation and maintenance
interfaces and declares `SnapshotHistory`, but **not** `ExactCatchup`. Catch-up reads are still
ordered and complete; a consumer needing exactness de-duplicates by `EventId` (a future per-domain
sequence or table-per-domain layout would restore density).

Tested against SQLite, SQL Server and PostgreSQL (Testcontainers).

## 6. Provider: Azure Table Storage

**Domain = its own tables.** Unlike a relational schema, ATS tables are cheap to create and drop, so
each domain gets its **own** events and snapshots tables (plus a blob container, below) rather than a
shared discriminator — a domain is a self-contained unit that can be dropped whole. Table names are
`{prefix}{domain}Events` / `…Snapshots`; a domain that is not a legal table-name fragment is mapped
to a stable hash. The checkpoint table is shared (the checkpoint store is one domain-agnostic service).

**Core (clean and atomic):**
- PartitionKey = `StreamId`, RowKey = `"E" + version.ToString("D19")`.
- A `HEAD` entity in the same partition holds the current version and lifecycle flags.
- Append = **a single Entity Group Transaction**: check-and-bump `HEAD` via ETag plus insert the
  event rows. All in one partition, therefore atomic. Limit: 100 entities per EGT, so at most 99
  events per append (over that: a clear exception). A lost race is an ETag / entity-exists conflict,
  re-read and classified exactly as the relational provider does.
- Reading a stream = a range query on RowKey prefix `"E"`. One partition scan, fast and cheap.

**Snapshots:** separate table, PK = `StreamId`, RowKey = `(long.MaxValue - version).ToString("D19")`,
so the newest snapshot is the first row. The same reverse-key trick gives **descending reads
server-side**: a per-stream index table (RowKey = `MaxValue - version`) turns a backward stream read
into an ascending range scan instead of buffering the whole stream.

**`$all` and the descending index — bus-driven projection.** Positions come from a **per-process
monotonic tick** allocated at append, so a single process produces strictly increasing, totally-ordered
positions (across processes they interleave — hence no `ExactCatchup`). Two secondary indexes are
maintained by `AzureTablesIndexProjector`, which tails the source-of-truth event scan from a durable
checkpoint (the same shape as a bus relay) and is idempotent:

- **Per-stream descending index** (RowKey = reverse version) → backward `ReadStreamAsync` = index range
  scan for the body + an events-table range read for the not-yet-indexed newest tail. Truncate/delete
  markers prune the index; redaction is reflected by dereferencing the live row.
- **`$all` time-bucket index** (PartitionKey = a coarse time bucket of the position, RowKey = position)
  → `ReadAllAsync` from a position is a bounded partition-range scan of the index, not a full events
  scan. **Sharded buckets, not a single `$all` key** — MS warns about the log-tail hot-spot.

Because the index is built asynchronously, the `$all` read is **eventually consistent**: an event
appears once indexed. This is not a separate capability — `$all` is *uniformly* a position-resumable
catch-up read (resume from the last position, re-read to pick up more), so a consumer treats every
provider the same; ATS merely has more latency before an event shows up. A relay tails and re-reads;
it does not need the newest event synchronously. **Atomicity:** the index entries live in different
partitions than the event, so no EGT spans
both — Microsoft's *eventually consistent transactions pattern*, the projector being the "written
idempotently after" step. If the projector never runs, backward reads fall back to the events-table
scan and `$all` is empty until it does — never blocking a write.
**Completeness:** the projector holds events newer than a safety window Δ before indexing them (D3,
status note), so an append whose tick was allocated before a later-committing sibling is not skipped —
provided no append stays uncommitted longer than Δ. `DrainAsync` ignores the window for a controlled
drain (tests, or shutdown after writers stop). Should an append ever outlive Δ, the events table still
has the event, so a full index rebuild (reset the projector checkpoint) recovers it.

**Alternatives considered and rejected (for a dense global sequence):**

| Alternative | Why not |
|---|---|
| The ATS `Timestamp` system property | Tempting — assigned by the *service*, so one clock for everyone, no client clocks involved. But it is not part of the key: ordering or seeking by it means a full table scan, and MS says not to rely on it for ordering. Usable as a cross-check, not as an index. |
| Sequencer entity (a real dense sequence) | Every append in the whole store funnels through one counter entity in one partition; ETag conflicts under concurrency produce retry loops that collapse throughput well below the partition ceiling. Burned numbers would *not* be the problem — they are the exact analogue of an aborted transaction on SQL, which gap detection clears via timeout. The contention is. |
| Hi/Lo number blocks | Fixes the contention, destroys the property it exists for: writer A holds 1–100, B holds 101–200, B writes 101 before A writes 1 — dense but no longer commit-ordered, and gap detection stalls on numbers nobody has written yet. |
| Queue / Event Hubs as the ordering channel | Pulls in an external service dependency we explicitly do not want, and only solves the *live* case — a historical `ReadAllAsync(from: T)` still needs the table index, so you end up with both. |
| No `$all` on ATS at all | The honest fallback: per-stream reads only. Costs cross-stream read models on ATS entirely. Kept in reserve if the index proves unworkable. |

⇒ ATS implements the global-read, time-navigation and maintenance interfaces (truncation, deletion,
redaction) and declares `SnapshotHistory`, but does **not** support multi-stream atomic append and does
**not** declare `ExactCatchup` (the monotonic position is per-process, so a catch-up subscription
de-duplicates by `EventId`).

**Large payloads → blobs.** Each domain has a blob container. A payload field small enough stays inline
in the entity; a field over the inline limit is written to a blob and the entity keeps a reference, so
there is no entity-size ceiling. Reads resolve the reference transparently; truncate/delete remove the
blobs, redaction overwrites them.

**Markers are cross-partition, so at-least-once.** A destructive op commits atomically in the stream
partition, but its `$system` marker is a separate append to a different partition — no EGT spans both.
The op records a marker watermark on the HEAD only *after* the marker lands, so a crash or failure in
between re-emits the marker on retry rather than losing it; markers apply idempotently, so a duplicate
is harmless. A read likewise tolerates a payload blob a concurrent delete already removed by skipping
that event.

Tested against Azurite: the full conformance suite plus large-payload and fault-injection tests
(a lost marker recovered on retry, a vanished blob skipped).

## 7. Deletion, truncation, retention, redaction — and their markers

The core stays append-only; everything destructive is a separately registered capability. **Every
destructive operation emits a system marker event into `$all`** (`SystemEventTypes`, carried in a
reserved `$system` stream), so read models — which, over the bus, hold copies of the data — learn to
forget or reproject. Destruction that did not propagate would leave the personal data alive in every
projection; the log is rarely where the exposure is. The markers are the propagation.

**Truncate-before** (`TruncateStreamAsync(streamId, beforeVersion)`) removes the events below the
boundary **physically**; versions stay monotonic (nothing is renumbered). Refused unless a snapshot
at `>= beforeVersion - 1` exists (the state must stay reconstructable — the load-time check in D5 is
the read-side counterpart); `force: true` overrides loudly. Emits `$stream-truncated`.

**Delete-stream:** `DeleteMode.Soft` (events go, StreamId reusable) or `DeleteMode.Hard` (data gone,
StreamId burned) — "id reusable vs id burned", not the soft/hard-tombstone of some other stores;
both physically remove the events. Emits `$stream-deleted`.

**Retention — by time *and* by attribute, searching over event metadata (later, but the model is
fixed now).** The target is: "delete all entries older than 30 days", and "delete only entries older
than 30 days of category A". Two consequences that must be baked in before the stores:

- **The searchable home is `EventData.Metadata`, not a new typed field.** Category, tenant, and also
  correlation/causation ids are metadata (structured, e.g. JSON). Attribute-filtered retention
  predicates would match on metadata plus `CreatedAt` (time is free — every event has it). We
  deliberately do **not** add an `Attributes` property to the event. *Not yet built:* event metadata is
  stored opaquely today (EF Core `varbinary`, ATS payload, InMemory bytes), so making it queryable (a
  JSON column with indexed paths / queryable entity properties / parse-and-match) is future work. What
  ships now is *stream-level* retention (`StreamMetadata` max-count / max-age via `IStreamMetadataStore`),
  not per-event attribute search.
- **Attribute-filtered retention deletes selectively — it punches holes.** Removing "category A older
  than 30 days" can drop event #5 while keeping #3 and #7. That is *not* prefix truncation, so it is
  **only valid for non-folded (log) domains** — a hole breaks an aggregate's fold. This is the same
  domain seam as everywhere: logs tolerate holes, aggregates do not. Each removal propagates as a
  marker so read models prune in step.

The retention *engine* (evaluate policy, delete matching, emit markers) is built with the stores,
where a metadata predicate can be translated to SQL/ATS efficiently. What is fixed now: metadata is
the queryable attribute home, and selective retention is a non-folded-domain capability.

Simple prefix/count retention (`MaxCount`, keep-last-N) remains available for the shape-preserving
case; the attribute+time search is the general form above.

**Redaction — the GDPR-erasure mechanism, and it *is* in scope** (`IRedactable.RedactEventAsync`,
emits `$event-redacted`). Erasing a subject's PII is **not** deleting a stream; it is
**overwriting the personal fields of the affected events in place**. Contrary to an earlier read,
this *preserves* the shape: the event's version, position and id are unchanged — only its
`Data`/`Metadata` bytes change, so there is no hole. That is exactly why it is the right erasure tool
and why it belongs in v1: "delete a stream" is not a GDPR answer, overwriting the PII is. The marker
tells read models to reproject the redacted event.

Markers materialize to typed records (`StreamTruncatedMarker`, `StreamDeletedMarker`,
`EventRedactedMarker`) via `IEventMaterializer`, which recognizes the reserved `$`-prefixed types and
reads them as JSON regardless of the domain's serializer — so a bus consumer handles them uniformly.

**Where the marker lands is a per-domain choice** (`DomainOptions`, the vocabulary being *domain* =
storage unit, *stream* = one event sequence within it):

- **`$system` (default).** Truncate/redact/delete markers go to the reserved `$system` stream, which
  shows up in the domain's `$all`. This keeps aggregate streams pure — a marker must never land in a
  folded stream, where it would take a version slot and break the aggregate's optimistic concurrency.
- **In-band (`EmitMarkersInBand()`), opt-in for non-folded domains.** For log-like domains whose
  streams are read, not folded, a survivable marker (truncate, redact) is written *on the affected
  stream itself*, so a per-stream reader sees the deletion in the stream's own flow. **Whole-stream
  deletion always uses `$system`** — the stream is gone, there is nowhere else to put it. The
  aggregate repository additionally skips `$`-typed events when folding, as a safety net.

**Tombstone lifecycle.** The `$system` stream is itself an append-only log — under frequent retention
(the access-log case) it would grow without bound. So markers are **transient tombstones**: the
scavenger removes them after `DomainOptions.TombstoneRetention` (default 7 days, `TimeProvider`-driven
and testable). The contract, stated plainly: a consumer must catch up within that window or it misses
the deletion notification. A checkpoint-aware variant (never scavenge before the slowest consumer)
is a possible later refinement; the time-based window is the first cut.

## 8. Testing — the heart of the agnosticism

`Dbosoft.EventStore.Testing` as a **public** package: an abstract `EventStoreConformanceTests`
suite that each provider inherits via a fixture. Capability-gated — a provider that does not implement
`ITruncatable` (or does not declare a behavioural flag) skips those tests instead of faking them.
Public so that third-party providers can validate their implementation too.

Stack: **xunit**, **AwesomeAssertions**, **NSubstitute**.

Beyond conformance: concurrency stress tests with parallel appends (exactly what the SAPId store
fails at), batch-idempotency tests (exact replay / partial overlap / in-batch duplicates), catch-up
completeness tests with artificial commit reordering, and `TimeProvider`-driven retention tests.

The conformance suite is built **together with the abstractions**, not after them: it *is* the
contract.

## 8a. Bus integration — designed for, not built

We presuppose a message bus (Rebus). The store provides the primitives a bus bridge needs and
nothing more:

- **Catch-up read** (`IAllStreamReader.ReadAllAsync` from a position) — replay/rebuild, and the
  source a relay tails.
- **`IEventMaterializer`** (per domain, keyed) — maps events between stored bytes and typed objects
  **both directions**, so the bridge can turn a stored event into a bus message and an incoming bus
  message into an append. This is the one pipeline read models and the bridge share; it is no longer
  locked inside the aggregate repository.
- **`ICheckpointStore`** — a relay or a read-model rebuild remembers how far it read.

Two positions the design names and leaves to the application:

- **Relay-only (replay).** Live events are published to the bus directly by the producing handler;
  the store→bus relay exists only to replay history for rebuilds. Simplest, but the direct publish
  is a dual-write (append + publish as two steps) that a crash can split.
- **Outbox (single publisher).** The relay is the *only* publisher: append to the store, the relay
  forwards every new event to the bus. No dual-write; live and replay use one path.

The **inbound** side is not a special primitive — a bus handler uses the normal append API, with the
materializer mapping the message to an event. The bus adapter itself is a **separate optional
package**; a **demo in-memory bus** ships as the reference adapter (as InMemory is the reference
provider — proves the seam, no external dependency), and `Dbosoft.EventStore.Rebus` is the real one.

`RebusEventRelay` is the outbox: it tails `$all` from a checkpoint and publishes each materialized event
onto Rebus with the stream id, version, position and event id in the message headers. The order caveat is
explicit and unavoidable: **Rebus delivers unordered and at-least-once**, so the relay publishes in order
but consumers cannot rely on receiving in order. The resolution is *not* to make the bus ordered — it is
to treat each message as a **signal** and read the ordered content from the store: `OrderedEventDispatcher`
applies each stream in ascending version order, exactly once, filling a gap (a later event arriving first)
by reading the store, whatever order the signals arrive in. The bus is a low-latency notification; the
store stays the authority for order and never yields it. This needs the consumer to have store access; a
consumer with only the bus must use an ordered Rebus transport or accept best-effort (the lower-level
`StreamOrderingTracker` classifies each message so it can). `StoreToBusRelay` (in the InMemory package) is
the same shape without the Rebus dependency, so the seam is exercised end to end with no external broker.

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
| `Dbosoft.EventStore` | serializer, type registry, upcasting, materializer, aggregate repository, snapshot policy, DI | Abstractions |
| `Dbosoft.EventStore.EfCore` | EF Core provider | Core, EFCore.Relational |
| `Dbosoft.EventStore.AzureTables` | ATS provider | Core, Azure.Data.Tables |
| `Dbosoft.EventStore.InMemory` | reference / test implementation | Core |
| `Dbosoft.EventStore.Testing` | conformance suite | Abstractions, xunit, AwesomeAssertions |
| `Dbosoft.EventStore.Rebus` | Rebus adapter (relay, headers, ordering tracker) | Core, Rebus |

## 10. Phased plan

1. **Abstractions + InMemory + conformance suite.** The contract is defined by the tests. InMemory
   is the reference implementation the suite proves itself against.
2. **EF Core provider** — `Streams` head table, race-free append, paging. Against SQLite +
   PostgreSQL + SQL Server.
3. **Core layer** — serializer, type registry, upcasting, aggregate repository, snapshot policy.
4. **Truncation / retention / scavenge / delete**, including the snapshot invariant from §7.
5. **`$all` catch-up reads + materializer/checkpoint + demo bus bridge** — gap detection on SQL,
   the exact-vs-bounded split from D3, and the reference in-memory bus proving the relay/inbound seam.
6. **Azure Table Storage provider** — EGT append, sharded log-tail `$all`, chunking. Against
   Azurite. Deliberately after phase 5: only once the catch-up contract stands does it show whether
   the opaque token holds up.
7. **Docs, sample, NuGet, CI.**
8. **Migrate SAPId.** The actual proof: if the four SAPId aggregates run on the library without
   marker classes and without `DeferCommit`, the design is right.

## 11. Open points

- **Multi-stream atomic append:** trivial on EF Core, impossible on ATS. Is the capability worth it
  if only one provider satisfies it? Leaning yes, because process managers need it.
- **Blob overflow for ATS payloads over 1 MB:** later, if needed.
- **`SAPId` targets net8.0** — the migration in phase 8 presupposes a move to net10.0.

Decided and closed: no LanguageExt integration; the core stays idiomatic .NET.
